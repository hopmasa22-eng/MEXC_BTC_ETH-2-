import os
import json
import time
import logging
from collections import OrderedDict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

# Railway Variables: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, MEXC_SYMBOLS=BTCUSDT,ETHUSDT
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_IDS = [x.strip() for x in os.getenv("TELEGRAM_CHAT_ID", "").split(",") if x.strip()]

TIMEFRAME_SECONDS = 5 * 60
TIMEFRAME_LABEL = "5m"
DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT"

def normalize_symbol(value):
    """Accept BTCUSDT, BTC_USDT or BTC__USDT and normalize to BTC_USDT."""
    s = value.strip().upper().replace("_", "")
    if not s:
        return ""
    if s.endswith("USDT"):
        s = s[:-4]
    return f"{s}_USDT"

SYMBOLS = list(dict.fromkeys(
    normalize_symbol(s)
    for s in os.getenv("MEXC_SYMBOLS", DEFAULT_SYMBOLS).split(",")
    if s.strip()
))
POLL_SECONDS = max(5, int(os.getenv("POLL_SECONDS", "15")))
TEST_MODE = os.getenv("TEST_MODE", "false").strip().lower() in {"1", "true", "yes", "on"}
API_URL = "https://contract.mexc.com/api/v1/contract/kline/{symbol}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("mexc-signal-bot")
SESSION = requests.Session()
SESSION.headers.update({"Cache-Control": "no-cache", "Pragma": "no-cache", "User-Agent": "BTC-ETH-5m-Signal-Bot/5.0"})


def utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def kyiv(ts):
    return datetime.fromtimestamp(ts, tz=ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d %H:%M")


def symbol_label(symbol):
    return symbol.replace("_", "")


def candle_color(candle):
    if candle["close"] > candle["open"]:
        return "GREEN"
    if candle["close"] < candle["open"]:
        return "RED"
    return "DOJI"


def load_state(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            raise ValueError("State must be a JSON object")
        state.setdefault("pending", [])
        state.setdefault("last_processed", None)
        return state
    except FileNotFoundError:
        return {"last_processed": None, "pending": []}
    except Exception as exc:
        log.warning("Не вдалося прочитати стан %s (%s). Починаємо з чистого стану", path, exc)
        return {"last_processed": None, "pending": []}


def save_state(state, path):
    temp_path = path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


def send_telegram(text):
    if not TOKEN or not CHAT_IDS:
        log.error("Telegram не налаштовано: додайте TELEGRAM_BOT_TOKEN і TELEGRAM_CHAT_ID у змінні Railway")
        return False
    success_count = 0
    for chat_id in CHAT_IDS:
        try:
            response = SESSION.post(
                f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=15,
            )
            payload = response.json()
            if response.ok and payload.get("ok"):
                success_count += 1
                log.info("TELEGRAM SENT OK | chat=%s", chat_id)
            else:
                log.error("TELEGRAM ERROR | chat=%s | status=%s | body=%s", chat_id, response.status_code, response.text[:300])
        except Exception:
            log.exception("TELEGRAM EXCEPTION | chat=%s", chat_id)
    return success_count == len(CHAT_IDS)


def parse_mexc_timestamp(value):
    ts = int(float(value))
    # MEXC contract K-line timestamps are usually seconds; tolerate milliseconds.
    if ts > 10_000_000_000:
        ts //= 1000
    return ts


def fetch_5m_candles(symbol):
    """Fetch native 5-minute MEXC Futures candles; no local aggregation."""
    now = int(time.time())
    response = SESSION.get(
        API_URL.format(symbol=symbol),
        params={"interval": "Min5", "limit": 300, "start": now - 300 * 5 * 60, "end": now},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("success") is False:
        raise RuntimeError(f"MEXC API error {payload.get('code')}: {payload.get('message')}")
    data = payload.get("data")
    if not data:
        raise RuntimeError(f"MEXC returned empty data: {payload}")

    candles = []
    if isinstance(data, dict) and isinstance(data.get("time"), list):
        times, opens, closes = data["time"], data.get("open", []), data.get("close", [])
        if not (len(times) == len(opens) == len(closes)):
            raise RuntimeError("Масиви свічок MEXC мають різну довжину")
        for i, ts in enumerate(times):
            candles.append({"ts": parse_mexc_timestamp(ts), "open": float(opens[i]), "close": float(closes[i])})
    elif isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                ts, op, cl = row.get("time", row.get("t")), row.get("open", row.get("o")), row.get("close", row.get("c"))
            else:
                ts, op, cl = row[0], row[1], row[2]
            candles.append({"ts": parse_mexc_timestamp(ts), "open": float(op), "close": float(cl)})
    else:
        raise RuntimeError(f"Unknown MEXC response data format: {type(data).__name__}")

    by_ts = {c["ts"]: c for c in candles}
    candles = [by_ts[k] for k in sorted(by_ts)]
    now = int(time.time())
    closed = [c for c in candles if c["ts"] + TIMEFRAME_SECONDS <= now]
    live = next((c for c in reversed(candles) if c["ts"] <= now < c["ts"] + TIMEFRAME_SECONDS), None)
    return closed, live


class SignalEngine:
    """Start is candle #1; #6 is trigger; #7-#8 confirm; #9-#15 check result."""
    def __init__(self, symbol):
        self.symbol = symbol
        self.state_path = os.getenv(f"STATE_FILE_{symbol}", f"state_{symbol}.json")
        self.state = load_state(self.state_path)
        self.candles = OrderedDict()
        self.pending = [p for p in self.state.get("pending", []) if isinstance(p, dict) and "trigger_ts" in p] if isinstance(self.state.get("pending", []), list) else []
        self.pre_sent = set()
        self.initialized = False

    def seed(self, closed):
        self.candles = OrderedDict((c["ts"], c) for c in closed[-250:])
        self.candles = OrderedDict(sorted(self.candles.items()))
        # Do not create historical alerts when the service first starts.
        self.state["last_processed"] = next(reversed(self.candles), None)
        save_state(self.state, self.state_path)
        self.initialized = True
        if self.candles:
            latest = next(reversed(self.candles.values()))
            log.info("INITIALIZED | %s | candles=%d | latest=%s %s", self.symbol, len(self.candles), utc(latest["ts"]), candle_color(latest))

    def maybe_pre_alert(self, live_candle):
        if not self.initialized or not live_candle:
            return
        elapsed = time.time() - live_candle["ts"]
        # PRE is sent in the final minute of candle #8 (between 4:00 and 5:00).
        if elapsed < 240 or elapsed >= TIMEFRAME_SECONDS:
            return
        target_ts = live_candle["ts"]
        if target_ts in self.candles or target_ts in self.pre_sent:
            return

        # Start candle #1 is eight candles before live #8; #6 is 2 intervals before #8.
        start_ts = target_ts - 7 * TIMEFRAME_SECONDS
        c6_ts = target_ts - 2 * TIMEFRAME_SECONDS
        c7_ts = target_ts - TIMEFRAME_SECONDS
        if any(ts not in self.candles for ts in (start_ts, c6_ts, c7_ts)):
            return
        start = self.candles[start_ts]
        start_color = candle_color(start)
        trigger_color = candle_color(self.candles[c6_ts])
        if start_color not in ("GREEN", "RED") or trigger_color not in ("GREEN", "RED"):
            return
        if trigger_color == start_color:
            return
        if candle_color(self.candles[c7_ts]) != trigger_color or candle_color(live_candle) != trigger_color:
            return
        # Start must be the last candle of its same-color run.
        prior_ts = start_ts - TIMEFRAME_SECONDS
        if prior_ts in self.candles and candle_color(self.candles[prior_ts]) == start_color:
            return

        direction = "LONG" if start_color == "GREEN" and trigger_color == "RED" else "SHORT"
        text = (
            f"⚠️ ПОПЕРЕДЖЕННЯ — {symbol_label(self.symbol)}, ф’ючерси\n"
            f"Інтервал: {TIMEFRAME_LABEL}\n"
            "Поточна послідовність: початкова свічка → №6 → №7 → поточна №8\n"
            f"Можливий напрямок: {direction}\n\n"
            "Остаточний сигнал буде надіслано лише після закриття свічки №8."
        )
        log.info("ПОПЕРЕДЖЕННЯ | %s | напрямок=%s | початок=%s", self.symbol, direction, utc(start_ts))
        if send_telegram(text):
            self.pre_sent.add(target_ts)

    def ingest(self, closed):
        if not self.initialized:
            self.seed(closed)
            return 0
        existing = set(self.candles)
        new = [c for c in closed if c["ts"] not in existing]
        if not new:
            return 0
        for candle in sorted(new, key=lambda x: x["ts"]):
            self.candles[candle["ts"]] = candle
            self.candles = OrderedDict(sorted(self.candles.items()))
            while len(self.candles) > 250:
                self.candles.popitem(last=False)
            self.evaluate(candle["ts"])
        self.state["pending"] = self.pending
        self.state["last_processed"] = new[-1]["ts"]
        save_state(self.state, self.state_path)
        return len(new)

    def evaluate(self, ts):
        keys = list(self.candles)
        idx = keys.index(ts)
        current = self.candles[ts]
        keep = []

        # Resolve already-created setups first.
        for setup in self.pending:
            trigger_ts = setup["trigger_ts"]
            rel = int((ts - trigger_ts) // TIMEFRAME_SECONDS)
            trigger_color = setup["trigger_color"]

            # #7 and #8 must match #6. If either changes color, cancel.
            if rel in (1, 2):
                if candle_color(current) != trigger_color:
                    log.info("CANCEL SETUP | %s | candle #%d changed color", self.symbol, rel + 6)
                else:
                    if rel == 2 and not setup.get("signal_sent", False):
                        signal = (
                            f"🔔 СИГНАЛ {setup['direction']}\n\n{symbol_label(self.symbol)}, ф’ючерси\n"
                            f"Інтервал: {TIMEFRAME_LABEL}\nПочаткова свічка: {kyiv(setup['start_ts'])} за київським часом\n"
                            "Умова: свічка №6; підтвердження: №7 і №8\n\n"
                            "Це лише сигнал. Автоматичні угоди не виконуються."
                        )
                        if send_telegram(signal):
                            setup["signal_sent"] = True
                        log.info("СИГНАЛ %s | %s | початок=%s", setup["direction"], self.symbol, utc(setup["start_ts"]))
                    keep.append(setup)
                continue

            # #9-#15: first non-doji candle in the expected direction is WIN.
            if rel < 3:
                keep.append(setup)
                continue
            wanted = "GREEN" if setup["direction"] == "LONG" else "RED"
            result_number = rel + 6
            if result_number <= 15 and candle_color(current) == wanted:
                send_telegram(
                    f"✅ УСПІХ\n{symbol_label(self.symbol)}, ф’ючерси\nНапрямок: {setup['direction']}\n"
                    f"Підтверджувальна свічка: №{result_number} із 15"
                )
                log.info("РЕЗУЛЬТАТ: УСПІХ | %s | %s | свічка №%d", self.symbol, setup["direction"], result_number)
            elif result_number >= 15:
                send_telegram(
                    f"❌ НЕУСПІХ\n{symbol_label(self.symbol)}, ф’ючерси\nНапрямок: {setup['direction']}\n"
                    "У свічках №9–№15 підтвердження не з’явилося."
                )
                log.info("РЕЗУЛЬТАТ: НЕУСПІХ | %s | %s", self.symbol, setup["direction"])
            else:
                keep.append(setup)
        self.pending = keep

        # Candle #6 is exactly five intervals after Start (#1).
        if idx < 5:
            return
        start_ts = keys[idx - 5]
        start = self.candles[start_ts]
        start_color = candle_color(start)
        if start_color not in ("GREEN", "RED"):
            return
        # Start should be the final candle in a run of its color.
        next_ts = start_ts + TIMEFRAME_SECONDS
        if next_ts in self.candles and candle_color(self.candles[next_ts]) == start_color:
            return
        trigger_color = candle_color(current)
        if trigger_color not in ("GREEN", "RED") or trigger_color == start_color:
            return
        if any(p.get("start_ts") == start_ts for p in self.pending):
            return
        direction = "LONG" if start_color == "GREEN" and trigger_color == "RED" else "SHORT"
        self.pending.append({
            "start_ts": start_ts,
            "trigger_ts": ts,
            "trigger_color": trigger_color,
            "direction": direction,
            "signal_sent": False,
        })
        log.info("КАНДИДАТ НА СИГНАЛ | %s | %s | початок=%s %s | №6=%s", self.symbol, direction, utc(start_ts), start_color, trigger_color)


def main():
    if not SYMBOLS:
        raise RuntimeError("MEXC_SYMBOLS is empty; set it to BTCUSDT,ETHUSDT")
    log.info("Запуск бота сигналів MEXC Futures на готових свічках | інтервал=%s | перевірка кожні %s с", TIMEFRAME_LABEL, POLL_SECONDS)
    log.info("Монети: %s", ", ".join(SYMBOLS))
    log.info("Telegram налаштовано: %s | кількість чатів: %d", bool(TOKEN), len(CHAT_IDS))
    if TOKEN and CHAT_IDS:
        send_telegram(f"🤖 БОТ ЗАПУЩЕНО\nMEXC Futures\nМонети: {', '.join(symbol_label(s) for s in SYMBOLS)}\nІнтервал: {TIMEFRAME_LABEL}\nНадсилаються лише сигнали; автоматичні угоди не виконуються.")

    engines = {symbol: SignalEngine(symbol) for symbol in SYMBOLS}
    last_heartbeat = {}
    while True:
        for symbol, engine in engines.items():
            try:
                closed, live = fetch_5m_candles(symbol)
                if live:
                    engine.maybe_pre_alert(live)
                if not closed:
                    log.warning("%s | MEXC поки не повернув закритих свічок %s", symbol, TIMEFRAME_LABEL)
                    continue
                added = engine.ingest(closed)
                latest = closed[-1]
                if added:
                    log.info("НОВІ ДАНІ | %s | готових свічок 5m=%d | закритих=%d | додано=%d | остання=%s %s | відкриття=%s закриття=%s | очікують=%d",
                             symbol, len(closed), len(closed), added, utc(latest["ts"]), candle_color(latest), latest["open"], latest["close"], len(engine.pending))
                elif time.time() - last_heartbeat.get(symbol, 0) >= 60:
                    age = max(0, int(time.time() - (latest["ts"] + TIMEFRAME_SECONDS)))
                    log.info("ЗВ’ЯЗОК Є | %s | остання=%s %s | затримка=%s с | очікують=%d", symbol, utc(latest["ts"]), candle_color(latest), age, len(engine.pending))
                    last_heartbeat[symbol] = time.time()
            except Exception as exc:
                log.exception("ПОМИЛКА ЦИКЛУ | %s | %s", symbol, exc)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    if TEST_MODE:
        # Simple startup/config validation; no real Telegram messages are sent.
        print("ТЕСТОВИЙ РЕЖИМ: ПЕРЕВІРКУ ПРОЙДЕНО")
        print("Монети:", ", ".join(SYMBOLS))
        print("Інтервал:", TIMEFRAME_LABEL)
        print("Джерело даних:", API_URL)
        print("Правило: початкова свічка №1, умова №6, підтвердження №7–№8, результат №9–№15")
    else:
        main()
