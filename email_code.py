from __future__ import annotations

import copy
import json
import math
import os
import sqlite3
import smtplib
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from email.header import Header
from email.mime.text import MIMEText
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional, Tuple

import requests


# ============================================================
# CONFIGURATION — v3.1 USD
# ============================================================

SCRIPT_VERSION = "3.1-USD"

EMAIL_SENDER = os.environ.get("EMAIL_SENDER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER") or EMAIL_SENDER

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_RETRIES = 3
SMTP_RETRY_DELAY_SECONDS = 3

BITSTAMP_BASE_URL = "https://www.bitstamp.net/api/v2/ohlc"
API_RETRIES = 4
API_RETRY_DELAY_SECONDS = 2
API_TIMEOUT_SECONDS = 20

PAIRS = {
    "ETH/USD": "ethusd",
    "SOL/USD": "solusd",
}

DATA_DIR = os.environ.get("DATA_DIR") or "."
os.makedirs(DATA_DIR, exist_ok=True)
STATE_FILE = os.path.join(DATA_DIR, "state.json")
STATE_VERSION = 4
TRADE_JOURNAL_FILE = os.path.join(DATA_DIR, "trade_journal.sqlite3")

# Timeframes in seconds.
DAILY_STEP = 86400
HTF_STEP = 14400
LTF_STEP = 900

# Bitstamp OHLC endpoint accepts limits from 1 to 1000.
DAILY_LIMIT = 40
HTF_LIMIT = 500
LTF_LIMIT = 1000

# Confirmed fractal market structure.
SWING_LEFT = 2
SWING_RIGHT = 2

# Dynamic strategy parameters.
POI_BELOW_SUPPORT_PCT = 0.006
POI_ABOVE_SUPPORT_PCT = 0.018
INVALIDATION_BUFFER_PCT = 0.002

# 15m CHoCH retest tolerance around the broken level.
RETEST_TOLERANCE_PCT = 0.003
RETEST_MAX_DEPTH_PCT = 0.006
CHOCH_MIN_CLOSE_POSITION = 0.65
MIN_RR = 1.5

# Setup lifetimes.
CHOCH_MAX_AGE_SECONDS = 8 * 60 * 60
POI_MAX_AGE_SECONDS = 24 * 60 * 60

# A newly confirmed signal is emailed only if its candle closed within this age.
RECENT_SIGNAL_MAX_AGE_SECONDS = 20 * 60

# Long-only: no RANGE or ambiguous market structure is accepted.
ALLOWED_LONG_HTF_BIAS = {"BULLISH"}

ALLOW_DAILY_OPEN_SWEEP_TRIGGER = True

# Run at :00:15, :15:15, :30:15 and :45:15 UTC.
CANDLE_CLOSE_BUFFER_SECONDS = 15

# Health server.
HEALTH_PORT = int(os.environ.get("PORT", 10000))
recent_pings = deque(maxlen=2)
health_state_lock = threading.Lock()
health_state: Dict[str, Any] = {
    "last_cycle_started_utc": None,
    "last_cycle_finished_utc": None,
    "last_cycle_finished_ts": 0,
    "last_cycle_ok": False,
    "last_cycle_errors": [],
}
HEALTH_STALE_AFTER_SECONDS = 2 * LTF_STEP + 2 * CANDLE_CLOSE_BUFFER_SECONDS


# ============================================================
# HEALTH CHECK SERVER
# ============================================================


class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        with health_state_lock:
            snapshot = dict(health_state)

        now_ts = utc_now_ts()
        last_finished_ts = int(snapshot.get("last_cycle_finished_ts", 0))
        cycle_is_fresh = (
            last_finished_ts > 0
            and 0 <= now_ts - last_finished_ts <= HEALTH_STALE_AFTER_SECONDS
        )
        engine_is_healthy = bool(snapshot.get("last_cycle_ok")) and cycle_is_fresh
        is_readiness_probe = self.path.split("?", 1)[0] == "/ready"
        response_status = 503 if is_readiness_probe and not engine_is_healthy else 200
        recent_pings.append(
            f"[{now_str}] Ping received - HTTP {response_status}"
        )

        response_text = "=== CRYPTO MONITOR LIVE HEARTBEAT ===\n"
        response_text += f"Engine status: {'HEALTHY' if engine_is_healthy else 'DEGRADED'}\n"
        response_text += f"Last cycle: {snapshot.get('last_cycle_finished_utc')}\n"
        response_text += (
            "Probe: readiness (/ready)\n"
            if is_readiness_probe
            else "Probe: liveness\n"
        )
        errors = snapshot.get("last_cycle_errors", [])
        if errors:
            response_text += "Errors: " + " | ".join(str(item) for item in errors) + "\n"
        for index, ping in enumerate(recent_pings, 1):
            response_text += f"{index}. {ping}\n"

        self.send_response(response_status)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(response_text.encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:
        return


def start_health_check_server() -> None:
    server_address = ("0.0.0.0", HEALTH_PORT)
    try:
        httpd = HTTPServer(server_address, HealthCheckHandler)
    except OSError as exc:
        print(f"Health server could not bind port {HEALTH_PORT}: {exc}")
        return

    print(f"Health check server listening on port {HEALTH_PORT}...")
    httpd.serve_forever()


# ============================================================
# HTTP / API
# ============================================================


SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": f"Crypto15mMonitor/{SCRIPT_VERSION}",
        "Accept": "application/json",
    }
)


class DataNotReady(RuntimeError):
    """Raised when exchange data is temporarily unavailable or incomplete."""


# ============================================================
# GENERIC HELPERS
# ============================================================


def utc_now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def format_usd(value: float) -> str:
    return f"{value:,.2f}$"


def pair_clean(name: str) -> str:
    return name.replace("/", " - ")


def is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError, OverflowError):
        return False


def candle_ts(candle: Dict[str, Any]) -> int:
    return int(candle["timestamp"])


def candle_close_ts(candle: Dict[str, Any], step: int) -> int:
    return candle_ts(candle) + step


def candle_float(candle: Dict[str, Any], key: str) -> float:
    value = float(candle[key])
    if not math.isfinite(value):
        raise ValueError(f"Non-finite candle value for {key}.")
    return value


def is_bullish(candle: Dict[str, Any]) -> bool:
    return candle_float(candle, "close") > candle_float(candle, "open")


def candle_touches_zone(
    candle: Dict[str, Any], lower: float, upper: float
) -> bool:
    high = candle_float(candle, "high")
    low = candle_float(candle, "low")
    return low <= upper and high >= lower


def candle_is_recently_closed(candle: Dict[str, Any], now_ts: int) -> bool:
    close_ts = candle_close_ts(candle, LTF_STEP)
    age = now_ts - close_ts
    return 0 <= age <= RECENT_SIGNAL_MAX_AGE_SECONDS


def validate_candle_series(
    candles: List[Dict[str, Any]], step: int, label: str
) -> None:
    """Validate alignment, ordering, OHLC integrity and strict continuity."""
    if not candles:
        raise DataNotReady(f"{label}: empty candle series.")

    previous_ts: Optional[int] = None

    for candle in candles:
        try:
            ts = candle_ts(candle)
        except (KeyError, TypeError, ValueError) as exc:
            raise DataNotReady(f"{label}: invalid timestamp.") from exc

        if ts <= 0 or ts % step != 0:
            raise DataNotReady(f"{label}: invalid timestamp alignment: {ts}.")

        if previous_ts is not None:
            if ts <= previous_ts:
                raise DataNotReady(
                    f"{label}: duplicate or out-of-order timestamps detected."
                )
            if ts != previous_ts + step:
                raise DataNotReady(
                    f"{label}: missing candle gap between {previous_ts} and {ts}."
                )

        try:
            open_price = candle_float(candle, "open")
            high_price = candle_float(candle, "high")
            low_price = candle_float(candle, "low")
            close_price = candle_float(candle, "close")
        except (KeyError, TypeError, ValueError) as exc:
            raise DataNotReady(f"{label}: invalid OHLC at {ts}.") from exc

        if min(open_price, high_price, low_price, close_price) <= 0:
            raise DataNotReady(f"{label}: non-positive price at {ts}.")

        if high_price < max(open_price, close_price, low_price):
            raise DataNotReady(f"{label}: inconsistent high at {ts}.")

        if low_price > min(open_price, close_price, high_price):
            raise DataNotReady(f"{label}: inconsistent low at {ts}.")

        if "volume" in candle:
            volume = candle_float(candle, "volume")
            if volume < 0:
                raise DataNotReady(f"{label}: negative volume at {ts}.")

        previous_ts = ts


# ============================================================
# BITSTAMP DATA
# ============================================================


def get_ohlc(
    pair_code: str,
    step: int,
    limit: int,
    *,
    exclude_current_candle: bool = False,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch validated OHLCV data with bounded retry and Retry-After support."""
    url = f"{BITSTAMP_BASE_URL}/{pair_code}/"
    params = {
        "step": step,
        "limit": limit,
        "exclude_current_candle": str(exclude_current_candle).lower(),
    }

    last_error: Optional[Exception] = None

    for attempt in range(1, API_RETRIES + 1):
        try:
            response = SESSION.get(url, params=params, timeout=API_TIMEOUT_SECONDS)

            if response.status_code == 429 or response.status_code >= 500:
                retry_after_value = response.headers.get("Retry-After")
                try:
                    retry_after = float(retry_after_value) if retry_after_value else 0.0
                except (TypeError, ValueError):
                    retry_after = 0.0

                delay = (
                    min(60.0, max(1.0, retry_after))
                    if retry_after > 0
                    else API_RETRY_DELAY_SECONDS * attempt
                )
                raise requests.HTTPError(
                    f"Transient HTTP {response.status_code}; retry delay {delay:.1f}s.",
                    response=response,
                )

            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise DataNotReady(f"{pair_code}: API returned a non-object payload.")
            data_block = payload.get("data")
            if not isinstance(data_block, dict):
                raise DataNotReady(f"{pair_code}: API response has no valid data object.")
            raw_candles = data_block.get("ohlc", [])

            if not isinstance(raw_candles, list) or not raw_candles:
                return None

            cleaned: List[Dict[str, Any]] = []
            for raw in raw_candles:
                if not isinstance(raw, dict):
                    continue
                try:
                    candle: Dict[str, Any] = {
                        "timestamp": int(raw["timestamp"]),
                        "open": float(raw["open"]),
                        "high": float(raw["high"]),
                        "low": float(raw["low"]),
                        "close": float(raw["close"]),
                    }
                    if "volume" in raw:
                        candle["volume"] = float(raw["volume"])
                except (KeyError, TypeError, ValueError, OverflowError):
                    continue
                cleaned.append(candle)

            if not cleaned:
                return None

            cleaned.sort(key=lambda item: item["timestamp"])
            validate_candle_series(cleaned, step, f"{pair_code}/step={step}")
            return cleaned

        except requests.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            transient = status_code == 429 or (status_code is not None and status_code >= 500)

            if transient and attempt < API_RETRIES:
                retry_after_value = (
                    exc.response.headers.get("Retry-After")
                    if exc.response is not None
                    else None
                )
                try:
                    retry_after = float(retry_after_value) if retry_after_value else 0.0
                except (TypeError, ValueError):
                    retry_after = 0.0
                delay = (
                    min(60.0, max(1.0, retry_after))
                    if retry_after > 0
                    else API_RETRY_DELAY_SECONDS * attempt
                )
                print(
                    f"API retry {attempt}/{API_RETRIES - 1} for {pair_code}, "
                    f"step={step}: {exc}; sleeping {delay:.1f}s"
                )
                time.sleep(delay)
                continue

            if not transient:
                print(f"API non-retryable error ({pair_code}, step={step}): {exc}")
                return None

        except (requests.RequestException, ValueError, TypeError, DataNotReady) as exc:
            last_error = exc
            if attempt < API_RETRIES:
                delay = API_RETRY_DELAY_SECONDS * attempt
                print(
                    f"API retry {attempt}/{API_RETRIES - 1} for {pair_code}, "
                    f"step={step}: {exc}; sleeping {delay}s"
                )
                time.sleep(delay)

        if attempt == API_RETRIES:
            print(f"API error ({pair_code}, step={step}): {last_error}")

    return None


# ============================================================
# SQLITE JOURNAL AND EMAIL OUTBOX
# ============================================================


def get_journal_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(TRADE_JOURNAL_FILE, timeout=20)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            event_key TEXT PRIMARY KEY,
            asset TEXT NOT NULL,
            event_type TEXT NOT NULL,
            candle_ts INTEGER NOT NULL,
            created_at_utc TEXT NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS email_outbox (
            alert_key TEXT PRIMARY KEY,
            asset TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            candle_ts INTEGER NOT NULL,
            subject TEXT NOT NULL,
            body TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at_utc TEXT NOT NULL,
            sent_at_utc TEXT
        )
        """
    )
    connection.commit()
    return connection


def retry_pending_alerts() -> None:
    """Retry alerts that were eligible when created but could not be delivered."""
    with get_journal_connection() as connection:
        rows = connection.execute(
            """
            SELECT alert_key, subject, body
            FROM email_outbox
            WHERE status = 'PENDING'
            ORDER BY created_at_utc ASC
            LIMIT 25
            """
        ).fetchall()

    for alert_key, subject, body in rows:
        try:
            send_email(subject, body)
        except Exception as exc:
            with get_journal_connection() as connection:
                connection.execute(
                    """
                    UPDATE email_outbox
                    SET attempts = attempts + 1, last_error = ?
                    WHERE alert_key = ?
                    """,
                    (str(exc)[:1000], alert_key),
                )
            print(f"Pending email delivery still failing for {alert_key}: {exc}")
            continue

        sent_at = datetime.now(timezone.utc).isoformat()
        with get_journal_connection() as connection:
            connection.execute(
                """
                UPDATE email_outbox
                SET status = 'SENT', attempts = attempts + 1,
                    last_error = NULL, sent_at_utc = ?
                WHERE alert_key = ?
                """,
                (sent_at, alert_key),
            )
        print(f"Pending email delivered: {alert_key}")


def journal_event(
    asset: str,
    event_type: str,
    candle_timestamp: int,
    payload: Dict[str, Any],
) -> None:
    event_key = f"{asset}|{event_type}|{candle_timestamp}"
    created_at = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)

    with get_journal_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO events
            (event_key, asset, event_type, candle_ts, created_at_utc, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_key, asset, event_type, candle_timestamp, created_at, payload_json),
        )


def deliver_alert_once(
    asset: str,
    alert_type: str,
    candle_timestamp: int,
    subject: str,
    body: str,
) -> None:
    """Persist an alert before sending; retry a pending alert without duplicating sent rows."""
    alert_key = f"{asset}|{alert_type}|{candle_timestamp}"
    created_at = datetime.now(timezone.utc).isoformat()

    with get_journal_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO email_outbox
            (alert_key, asset, alert_type, candle_ts, subject, body, status,
             attempts, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?)
            """,
            (alert_key, asset, alert_type, candle_timestamp, subject, body, created_at),
        )
        row = connection.execute(
            "SELECT status, subject, body FROM email_outbox WHERE alert_key = ?",
            (alert_key,),
        ).fetchone()

    if row is None:
        raise RuntimeError("Email outbox record could not be created.")

    status, stored_subject, stored_body = row
    if status == "SENT":
        return

    try:
        send_email(stored_subject, stored_body)
    except Exception as exc:
        with get_journal_connection() as connection:
            connection.execute(
                """
                UPDATE email_outbox
                SET attempts = attempts + 1, last_error = ?
                WHERE alert_key = ?
                """,
                (str(exc)[:1000], alert_key),
            )
        raise

    sent_at = datetime.now(timezone.utc).isoformat()
    with get_journal_connection() as connection:
        connection.execute(
            """
            UPDATE email_outbox
            SET status = 'SENT', attempts = attempts + 1,
                last_error = NULL, sent_at_utc = ?
            WHERE alert_key = ?
            """,
            (sent_at, alert_key),
        )


# ============================================================
# EMAIL
# ============================================================


def send_email(subject: str, body: str) -> None:
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not EMAIL_RECEIVER:
        raise RuntimeError(
            "EMAIL_SENDER, EMAIL_PASSWORD and EMAIL_RECEIVER/EMAIL_SENDER must be configured."
        )

    message = MIMEText(body, "plain", "utf-8")
    message["Subject"] = Header(subject, "utf-8")
    message["From"] = EMAIL_SENDER
    message["To"] = EMAIL_RECEIVER

    last_error: Optional[Exception] = None

    for attempt in range(1, SMTP_RETRIES + 1):
        try:
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=20) as server:
                server.login(EMAIL_SENDER, EMAIL_PASSWORD)
                server.sendmail(
                    EMAIL_SENDER,
                    [EMAIL_RECEIVER],
                    message.as_string(),
                )
            return
        except (OSError, smtplib.SMTPException) as exc:
            last_error = exc
            if attempt < SMTP_RETRIES:
                delay = SMTP_RETRY_DELAY_SECONDS * attempt
                print(
                    f"SMTP retry {attempt}/{SMTP_RETRIES - 1}: {exc}; "
                    f"sleeping {delay}s"
                )
                time.sleep(delay)

    raise RuntimeError(f"Email delivery failed after {SMTP_RETRIES} attempts: {last_error}")


def send_level_2_alert(name: str, price: float, candle_timestamp: int) -> None:
    subject = f"🟠 {pair_clean(name)}: ΣΕ ΕΠΙΦΥΛΑΚΗ"
    body = (
        "Asset:\n"
        f"      {name}\n\n"
        "Τρέχουσα τιμή:\n"
        f"      {format_usd(price)}\n\n"
        "🟠\n"
        "      Κατάσταση: Είσοδος σε Ζώνη Ενδιαφέροντος (POI)"
    )
    deliver_alert_once(name, "LEVEL_2_READY", candle_timestamp, subject, body)


def send_level_2_fail_alert(name: str, price: float, candle_timestamp: int) -> None:
    subject = f"🔴 {pair_clean(name)}: FAIL"
    body = (
        "Asset:\n"
        f"      {name}\n\n"
        "Τρέχουσα τιμή:\n"
        f"      {format_usd(price)}\n\n"
        "🔴\n"
        "      ΑΚΥΡΩΣΗ ΕΤΟΙΜΟΤΗΤΑΣ"
    )
    deliver_alert_once(name, "LEVEL_2_FAIL", candle_timestamp, subject, body)


def send_level_3_alert(
    name: str,
    entry: float,
    invalidation: float,
    take_profit: float,
    candle_timestamp: int,
) -> None:
    subject = f"🚨 LEVEL 3: {pair_clean(name)} EXECUTION"
    body = (
        "Asset:\n"
        f"      {name}\n\n"
        "🟢\n"
        f"      Entry: {format_usd(entry)} 🟢\n\n"
        f"ΑΠΟΤΥΧΙΑ: {format_usd(invalidation)}\n\n"
        f"Take Profit: {format_usd(take_profit)}"
    )
    deliver_alert_once(name, "LEVEL_3_EXECUTION", candle_timestamp, subject, body)


# ============================================================
# STATE
# ============================================================


def fresh_asset_state() -> Dict[str, Any]:
    return {
        "status": "IDLE",
        "last_processed_15m": 0,
    }


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {"_version": STATE_VERSION}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"State file cannot be read: {exc}") from exc

    if not isinstance(data, dict):
        raise RuntimeError("state.json must contain a JSON object.")

    if data.get("_version") != STATE_VERSION:
        print("State version mismatch -> resetting strategy state safely.")
        return {"_version": STATE_VERSION}

    data["_version"] = STATE_VERSION
    return data


def save_state(state: Dict[str, Any]) -> None:
    """Atomically persist state using temp-file + fsync + os.replace."""
    temp_file = f"{STATE_FILE}.tmp"
    directory = os.path.dirname(os.path.abspath(STATE_FILE))

    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, ensure_ascii=False, sort_keys=True)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(temp_file, STATE_FILE)

    try:
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError:
        pass


def normalize_asset_state(state: Dict[str, Any], name: str) -> Dict[str, Any]:
    current = state.get(name)
    if not isinstance(current, dict):
        current = fresh_asset_state()
        state[name] = current

    current.setdefault("status", "IDLE")
    current.setdefault("last_processed_15m", 0)
    return current


def clear_setup(asset_state: Dict[str, Any]) -> None:
    """Clear setup/trade fields but retain the candle processing cursor."""
    last_processed = int(asset_state.get("last_processed_15m", 0))
    asset_state.clear()
    asset_state.update(fresh_asset_state())
    asset_state["last_processed_15m"] = last_processed


# ============================================================
# DAILY OPEN / HISTORICAL HTF CONTEXT
# ============================================================


def get_daily_open_for_timestamp(
    daily_candles: List[Dict[str, Any]], timestamp: int
) -> Optional[float]:
    """Return the UTC calendar-day open for the supplied 15m candle."""
    day_start = timestamp - (timestamp % DAILY_STEP)

    for candle in reversed(daily_candles):
        ts = candle_ts(candle)
        if ts == day_start:
            return candle_float(candle, "open")
        if ts < day_start:
            break

    return None


def htf_candles_closed_by_timestamp(
    htf_closed_now: List[Dict[str, Any]], timestamp: int
) -> List[Dict[str, Any]]:
    """Use only 4H candles whose close time is no later than this 15m open."""
    return [
        candle
        for candle in htf_closed_now
        if candle_ts(candle) + HTF_STEP <= timestamp
    ]


# ============================================================
# FRACTAL MARKET STRUCTURE
# ============================================================


def find_confirmed_swing_lows(
    candles: List[Dict[str, Any]],
) -> List[Tuple[int, float]]:
    swings: List[Tuple[int, float]] = []
    start = SWING_LEFT
    end = len(candles) - SWING_RIGHT

    for index in range(start, end):
        current_low = candle_float(candles[index], "low")
        left_ok = all(
            current_low <= candle_float(candles[j], "low")
            for j in range(index - SWING_LEFT, index)
        )
        right_ok = all(
            current_low <= candle_float(candles[j], "low")
            for j in range(index + 1, index + SWING_RIGHT + 1)
        )
        if left_ok and right_ok:
            swings.append((candle_ts(candles[index]), current_low))

    return swings


def find_confirmed_swing_highs(
    candles: List[Dict[str, Any]],
) -> List[Tuple[int, float]]:
    swings: List[Tuple[int, float]] = []
    start = SWING_LEFT
    end = len(candles) - SWING_RIGHT

    for index in range(start, end):
        current_high = candle_float(candles[index], "high")
        left_ok = all(
            current_high >= candle_float(candles[j], "high")
            for j in range(index - SWING_LEFT, index)
        )
        right_ok = all(
            current_high >= candle_float(candles[j], "high")
            for j in range(index + 1, index + SWING_RIGHT + 1)
        )
        if left_ok and right_ok:
            swings.append((candle_ts(candles[index]), current_high))

    return swings


def determine_htf_bias(htf_closed: List[Dict[str, Any]]) -> str:
    highs = find_confirmed_swing_highs(htf_closed)
    lows = find_confirmed_swing_lows(htf_closed)

    if len(highs) < 2 or len(lows) < 2:
        return "INSUFFICIENT_DATA"

    previous_high = highs[-2][1]
    latest_high = highs[-1][1]
    previous_low = lows[-2][1]
    latest_low = lows[-1][1]

    if latest_high > previous_high and latest_low > previous_low:
        return "BULLISH"

    if latest_high < previous_high and latest_low < previous_low:
        return "BEARISH"

    return "TRANSITIONAL"


def find_htf_key_levels(
    htf_closed: List[Dict[str, Any]], current_price: float
) -> Tuple[Optional[float], Optional[float], str]:
    minimum_candles = SWING_LEFT + SWING_RIGHT + 4
    if len(htf_closed) < minimum_candles:
        return None, None, "INSUFFICIENT_DATA"

    swing_lows = find_confirmed_swing_lows(htf_closed)
    swing_highs = find_confirmed_swing_highs(htf_closed)

    supports = [price for _, price in swing_lows if price < current_price]
    resistances = [price for _, price in swing_highs if price > current_price]

    support = max(supports) if supports else None
    take_profit_area = min(resistances) if resistances else None
    bias = determine_htf_bias(htf_closed)

    return support, take_profit_area, bias


# ============================================================
# 15M STRUCTURE / TRIGGERS
# ============================================================


def find_latest_confirmed_swing_high(
    candles_before_trigger: List[Dict[str, Any]],
) -> Optional[Tuple[int, float]]:
    highs = find_confirmed_swing_highs(candles_before_trigger)
    return highs[-1] if highs else None


def has_recent_bearish_structure(
    candles_before_trigger: List[Dict[str, Any]],
) -> bool:
    highs = find_confirmed_swing_highs(candles_before_trigger)
    lows = find_confirmed_swing_lows(candles_before_trigger)

    if len(highs) < 2 or len(lows) < 2:
        return False

    # Both latest pivots must belong to the recent structure window.
    latest_timestamp = candle_ts(candles_before_trigger[-1])
    structure_window_start = latest_timestamp - 40 * LTF_STEP
    if highs[-1][0] < structure_window_start or lows[-1][0] < structure_window_start:
        return False

    return highs[-1][1] < highs[-2][1] and lows[-1][1] < lows[-2][1]


def candle_close_position(candle: Dict[str, Any]) -> Optional[float]:
    high_price = candle_float(candle, "high")
    low_price = candle_float(candle, "low")
    close_price = candle_float(candle, "close")
    candle_range = high_price - low_price
    if candle_range <= 0:
        return None
    return (close_price - low_price) / candle_range


def detect_bullish_choch(
    ltf_history_before_trigger: List[Dict[str, Any]],
    current_candle: Dict[str, Any],
) -> Tuple[bool, Optional[float]]:
    if len(ltf_history_before_trigger) < 12:
        return False, None

    recent_history = ltf_history_before_trigger[-40:]
    latest_high = find_latest_confirmed_swing_high(recent_history)
    if latest_high is None:
        return False, None

    _, break_level = latest_high
    current_close = candle_float(current_candle, "close")
    bullish = is_bullish(current_candle)
    close_position = candle_close_position(current_candle)

    bearish_structure = has_recent_bearish_structure(recent_history)
    displacement_confirmed = (
        close_position is not None
        and close_position >= CHOCH_MIN_CLOSE_POSITION
    )
    bullish_break = (
        current_close > break_level
        and bullish
        and displacement_confirmed
    )

    return bearish_structure and bullish_break, break_level


def daily_open_sweep_detected(
    current_candle: Dict[str, Any], daily_open: float
) -> bool:
    """Step A only: a 15m wick trades below today's UTC Daily Open."""
    return candle_float(current_candle, "low") < daily_open


def retest_zone_touched(
    current_candle: Dict[str, Any], break_level: float
) -> bool:
    upper_touch = break_level * (1.0 + RETEST_TOLERANCE_PCT)
    lower_depth = break_level * (1.0 - RETEST_MAX_DEPTH_PCT)
    return candle_touches_zone(current_candle, lower_depth, upper_touch)


def detect_retest_of_choch(
    current_candle: Dict[str, Any], break_level: float
) -> bool:
    current_low = candle_float(current_candle, "low")
    current_close = candle_float(current_candle, "close")
    lower_depth = break_level * (1.0 - RETEST_MAX_DEPTH_PCT)
    upper_touch = break_level * (1.0 + RETEST_TOLERANCE_PCT)

    valid_depth = lower_depth <= current_low <= upper_touch
    reclaimed = current_close > break_level
    bullish = is_bullish(current_candle)

    return valid_depth and reclaimed and bullish


# ============================================================
# EXECUTION / RISK
# ============================================================


def build_frozen_poi(support: float, take_profit_area: float, created_at: int) -> Dict[str, Any]:
    lower = support * (1.0 - POI_BELOW_SUPPORT_PCT)
    upper = support * (1.0 + POI_ABOVE_SUPPORT_PCT)

    return {
        "support": support,
        "lower": lower,
        "upper": upper,
        "fta": take_profit_area,
        "created_at": created_at,
    }


def calculate_invalidation(
    current_candle: Dict[str, Any],
    previous_candle: Optional[Dict[str, Any]] = None,
) -> float:
    lows = [candle_float(current_candle, "low")]
    if previous_candle is not None:
        lows.append(candle_float(previous_candle, "low"))

    lowest_wick = min(lows)
    return lowest_wick * (1.0 - INVALIDATION_BUFFER_PCT)


def calculate_rr(entry: float, invalidation: float, take_profit: float) -> Optional[float]:
    risk = entry - invalidation
    reward = take_profit - entry
    if risk <= 0 or reward <= 0:
        return None
    return reward / risk


def validate_execution(
    entry: float, invalidation: float, take_profit: float
) -> Optional[float]:
    rr = calculate_rr(entry, invalidation, take_profit)
    if rr is None or rr < MIN_RR:
        return None
    return rr


def record_stale_signal(
    name: str,
    asset_state: Dict[str, Any],
    current_candle: Dict[str, Any],
    trigger_label: str,
) -> None:
    current_ts = candle_ts(current_candle)
    journal_event(
        name,
        "STALE_SIGNAL_SKIPPED",
        current_ts,
        {
            "trigger": trigger_label,
            "close": candle_float(current_candle, "close"),
            "candle_close_ts": candle_close_ts(current_candle, LTF_STEP),
            "reason": "Signal candle closed more than the allowed notification window ago.",
        },
    )
    clear_setup(asset_state)
    print(
        f"{name}: stale {trigger_label} signal skipped; "
        "setup cleared without silent activation."
    )


def execute_trade(
    name: str,
    asset_state: Dict[str, Any],
    current_candle: Dict[str, Any],
    previous_candle: Dict[str, Any],
    trigger_label: str,
    *,
    send_alerts: bool,
) -> bool:
    """Activate a candle-based signal state; this does not place an exchange order."""
    poi = asset_state.get("poi")
    if not isinstance(poi, dict):
        print(f"{name}: signal activation blocked - no frozen POI.")
        return False

    entry = candle_float(current_candle, "close")
    invalidation = calculate_invalidation(current_candle, previous_candle)
    take_profit = float(poi["fta"])

    rr = validate_execution(entry, invalidation, take_profit)
    if rr is None:
        print(
            f"{name}: signal blocked - R:R below {MIN_RR:.1f} or invalid levels | "
            f"entry={format_usd(entry)} fail={format_usd(invalidation)} "
            f"target={format_usd(take_profit)}"
        )
        return False

    current_ts = candle_ts(current_candle)
    if send_alerts:
        send_level_3_alert(
            name,
            entry,
            invalidation,
            take_profit,
            current_ts,
        )
    else:
        raise RuntimeError(
            f"{name}: stale signal reached execute_trade; refusing silent activation."
        )

    journal_event(
        name,
        "SIGNAL_ACTIVATED",
        current_ts,
        {
            "entry": entry,
            "invalidation": invalidation,
            "take_profit": take_profit,
            "rr": rr,
            "trigger": trigger_label,
            "candle_close_ts": candle_close_ts(current_candle, LTF_STEP),
            "execution_mode": "CANDLE_SIGNAL_MONITOR_NOT_EXCHANGE_ORDER",
        },
    )

    asset_state["status"] = "EXECUTED"
    asset_state["trade"] = {
        "entry": entry,
        "invalidation": invalidation,
        "tp": take_profit,
        "rr": rr,
        "trigger": trigger_label,
        "executed_at": current_ts,
        "entry_candle_close_ts": candle_close_ts(current_candle, LTF_STEP),
        "execution_mode": "CANDLE_SIGNAL_MONITOR_NOT_EXCHANGE_ORDER",
    }
    asset_state.pop("choch", None)
    asset_state.pop("daily_sweep", None)

    print(
        f"LEVEL 3 SIGNAL: {name} | {trigger_label} | "
        f"entry={format_usd(entry)} fail={format_usd(invalidation)} "
        f"tp={format_usd(take_profit)} RR={rr:.2f} | email=YES | "
        "mode=SIGNAL_MONITOR"
    )
    return True


def monitor_executed_trade(
    name: str,
    asset_state: Dict[str, Any],
    current_candle: Dict[str, Any],
) -> None:
    """Monitor the hypothetical OHLC signal; no exchange position is queried."""
    trade = asset_state.get("trade")
    if not isinstance(trade, dict):
        clear_setup(asset_state)
        print(f"{name}: EXECUTED state had no signal data -> IDLE.")
        return

    invalidation = float(trade.get("invalidation", 0.0))
    take_profit = float(trade.get("tp", 0.0))
    current_low = candle_float(current_candle, "low")
    current_high = candle_float(current_candle, "high")
    current_ts = candle_ts(current_candle)

    # Conservative OHLC assumption: if both are touched in one candle, record failure first.
    if invalidation > 0 and current_low <= invalidation:
        journal_event(
            name,
            "SIGNAL_STOP_REACHED",
            current_ts,
            {
                "exit_price": invalidation,
                "entry": trade.get("entry"),
                "trigger": trade.get("trigger"),
                "assumption": "STOP_FIRST_IF_STOP_AND_TARGET_TOUCH_IN_SAME_CANDLE",
            },
        )
        print(f"{name}: signal invalidated at/through {format_usd(invalidation)}")
        clear_setup(asset_state)
        return

    if take_profit > 0 and current_high >= take_profit:
        journal_event(
            name,
            "SIGNAL_TARGET_REACHED",
            current_ts,
            {
                "exit_price": take_profit,
                "entry": trade.get("entry"),
                "trigger": trade.get("trigger"),
            },
        )
        print(f"{name}: signal target reached at/through {format_usd(take_profit)}")
        clear_setup(asset_state)


# ============================================================
# SETUP INVALIDATION / DAILY SWEEP HELPERS
# ============================================================


def poi_is_expired(poi: Dict[str, Any], current_timestamp: int) -> bool:
    created_at = int(poi.get("created_at", 0))
    return created_at <= 0 or current_timestamp - created_at > POI_MAX_AGE_SECONDS


def record_daily_sweep(
    asset_state: Dict[str, Any],
    current_candle: Dict[str, Any],
    daily_open: float,
) -> bool:
    """Record Step A. Returns True when a new/current-day sweep was stored."""
    if not daily_open_sweep_detected(current_candle, daily_open):
        return False

    current_ts = candle_ts(current_candle)
    day_start = current_ts - (current_ts % DAILY_STEP)
    asset_state["daily_sweep"] = {
        "day_start": day_start,
        "sweep_ts": current_ts,
        "sweep_low": candle_float(current_candle, "low"),
    }
    return True


def daily_sweep_reclaim_confirmed(
    asset_state: Dict[str, Any],
    current_candle: Dict[str, Any],
    daily_open: float,
    poi_lower: float,
    poi_upper: float,
) -> bool:
    """Step B: a later bullish 15m candle closes above Daily Open inside frozen POI."""
    sweep = asset_state.get("daily_sweep")
    if not isinstance(sweep, dict):
        return False

    current_ts = candle_ts(current_candle)
    current_day_start = current_ts - (current_ts % DAILY_STEP)

    if int(sweep.get("day_start", -1)) != current_day_start:
        asset_state.pop("daily_sweep", None)
        return False

    sweep_ts = int(sweep.get("sweep_ts", 0))
    if sweep_ts <= 0 or current_ts <= sweep_ts:
        return False

    current_close = candle_float(current_candle, "close")
    in_poi = poi_lower <= current_close <= poi_upper
    return (
        is_bullish(current_candle)
        and current_close > daily_open
        and in_poi
    )


# ============================================================
# PER-CANDLE STATE MACHINE
# ============================================================


def process_one_15m_candle(
    name: str,
    asset_state: Dict[str, Any],
    all_ltf_closed: List[Dict[str, Any]],
    index: int,
    daily_candles: List[Dict[str, Any]],
    htf_closed_now: List[Dict[str, Any]],
    *,
    send_alerts: bool,
) -> None:
    """Process one completed 15m candle using only information available at that time."""
    current = all_ltf_closed[index]
    previous = all_ltf_closed[index - 1] if index > 0 else None
    current_ts = candle_ts(current)

    if previous is None:
        return

    daily_open = get_daily_open_for_timestamp(daily_candles, current_ts)
    if daily_open is None:
        raise DataNotReady(
            f"{name}: Daily Open unavailable for UTC day of candle {current_ts}."
        )

    current_price = candle_float(current, "close")
    htf_closed_as_of_candle = htf_candles_closed_by_timestamp(
        htf_closed_now,
        current_ts,
    )
    htf_support, htf_fta, htf_bias = find_htf_key_levels(
        htf_closed_as_of_candle,
        current_price,
    )

    status = asset_state.get("status", "IDLE")
    print(
        f"{name} | candle={current_ts} close={format_usd(current_price)} "
        f"DO={format_usd(daily_open)} bias={htf_bias} "
        f"support={format_usd(htf_support) if htf_support is not None else 'N/A'} "
        f"FTA={format_usd(htf_fta) if htf_fta is not None else 'N/A'} "
        f"status={status} alerts={'LIVE' if send_alerts else 'SILENT'}"
    )

    # --------------------------------------------------------
    # 1. EXISTING SIGNAL / HYPOTHETICAL POSITION
    # --------------------------------------------------------
    if status == "EXECUTED":
        monitor_executed_trade(name, asset_state, current)
        return

    # --------------------------------------------------------
    # 2. WAITING FOR CHoCH RETEST
    # --------------------------------------------------------
    if status == "CHOCH_ACTIVE":
        choch = asset_state.get("choch")
        poi = asset_state.get("poi")

        if not isinstance(choch, dict) or not isinstance(poi, dict):
            clear_setup(asset_state)
            print(f"{name}: invalid CHOCH_ACTIVE state -> IDLE.")
            return

        break_level = float(choch.get("break_level", 0.0))
        detected_at = int(choch.get("detected_at", 0))
        frozen_fta = float(poi.get("fta", 0.0))
        frozen_lower = float(poi.get("lower", 0.0))
        frozen_upper = float(poi.get("upper", 0.0))

        if (
            break_level <= 0
            or frozen_fta <= 0
            or detected_at <= 0
            or frozen_lower <= 0
            or frozen_upper <= frozen_lower
        ):
            clear_setup(asset_state)
            print(f"{name}: invalid CHoCH/POI data -> IDLE.")
            return

        if current_ts - detected_at > CHOCH_MAX_AGE_SECONDS:
            clear_setup(asset_state)
            print(f"{name}: CHoCH expired -> IDLE.")
            return

        if poi_is_expired(poi, current_ts):
            clear_setup(asset_state)
            print(f"{name}: frozen POI expired -> IDLE.")
            return

        if htf_bias not in ALLOWED_LONG_HTF_BIAS:
            clear_setup(asset_state)
            print(f"{name}: CHoCH cancelled - HTF bias={htf_bias}.")
            return

        if current_price >= frozen_fta:
            clear_setup(asset_state)
            print(f"{name}: CHoCH cancelled - frozen FTA already reached.")
            return

        if current_price < frozen_lower:
            if send_alerts:
                send_level_2_fail_alert(name, current_price, current_ts)
            clear_setup(asset_state)
            print(f"{name}: CHoCH setup invalidated below frozen POI base.")
            return

        # A retest candidate outside the original POI invalidates this setup.
        if retest_zone_touched(current, break_level):
            retest_overlaps_original_poi = candle_touches_zone(
                current,
                frozen_lower,
                frozen_upper,
            )
            if not retest_overlaps_original_poi:
                clear_setup(asset_state)
                print(f"{name}: CHoCH retest occurred outside frozen POI -> cancelled.")
                return

            if detect_retest_of_choch(current, break_level):
                if not send_alerts:
                    record_stale_signal(name, asset_state, current, "CHoCH + Retest")
                    return
                execute_trade(
                    name,
                    asset_state,
                    current,
                    previous,
                    trigger_label="CHoCH + Retest",
                    send_alerts=True,
                )
                return

        return

    # --------------------------------------------------------
    # 3. EXISTING POI
    # --------------------------------------------------------
    if status == "POI_ACTIVE":
        poi = asset_state.get("poi")
        if not isinstance(poi, dict):
            clear_setup(asset_state)
            print(f"{name}: POI_ACTIVE without POI data -> IDLE.")
            return

        poi_lower = float(poi.get("lower", 0.0))
        poi_upper = float(poi.get("upper", 0.0))
        frozen_fta = float(poi.get("fta", 0.0))

        if poi_lower <= 0 or poi_upper <= poi_lower or frozen_fta <= 0:
            clear_setup(asset_state)
            print(f"{name}: invalid frozen POI values -> IDLE.")
            return

        if poi_is_expired(poi, current_ts):
            clear_setup(asset_state)
            print(f"{name}: POI expired after {POI_MAX_AGE_SECONDS // 3600} hours -> IDLE.")
            return

        if htf_bias not in ALLOWED_LONG_HTF_BIAS:
            clear_setup(asset_state)
            print(f"{name}: POI cancelled - HTF bias={htf_bias}; bullish structure required.")
            return

        if current_price >= frozen_fta:
            clear_setup(asset_state)
            print(f"{name}: POI cancelled - frozen FTA already reached.")
            return

        # Invalidation is candle-close based and uses the frozen zone.
        if current_price < poi_lower:
            if send_alerts:
                send_level_2_fail_alert(name, current_price, current_ts)
            journal_event(
                name,
                "POI_INVALIDATED",
                current_ts,
                {"close": current_price, "poi_lower": poi_lower},
            )
            clear_setup(asset_state)
            print(f"{name}: Level 2 FAIL | close={format_usd(current_price)}")
            return

        if current_price > poi_upper and not candle_touches_zone(
            current, poi_lower, poi_upper
        ):
            clear_setup(asset_state)
            print(f"{name}: left POI upward without trigger -> IDLE.")
            return

        # Step B is checked against the previously stored sweep BEFORE a new
        # sweep on this candle can replace it. This allows a reclaim candle to
        # wick below the open and still confirm the earlier sweep event.
        if ALLOW_DAILY_OPEN_SWEEP_TRIGGER and daily_sweep_reclaim_confirmed(
            asset_state,
            current,
            daily_open,
            poi_lower,
            poi_upper,
        ):
            if not send_alerts:
                record_stale_signal(
                    name, asset_state, current, "Daily Open Sweep + Reclaim"
                )
                return

            executed = execute_trade(
                name,
                asset_state,
                current,
                previous,
                trigger_label="Daily Open Sweep + Reclaim",
                send_alerts=True,
            )
            if executed:
                return

        # Step A: after testing a prior sweep, record a new/current-candle sweep.
        if ALLOW_DAILY_OPEN_SWEEP_TRIGGER:
            new_sweep = record_daily_sweep(asset_state, current, daily_open)
            if new_sweep:
                journal_event(
                    name,
                    "DAILY_OPEN_SWEEP_DETECTED",
                    current_ts,
                    {
                        "daily_open": daily_open,
                        "sweep_low": candle_float(current, "low"),
                    },
                )
                print(f"{name}: Daily Open sweep detected below {format_usd(daily_open)}.")

        # ----------------------------------------------------
        # 4. BULLISH 15M CHoCH DETECTION
        # ----------------------------------------------------
        if not (poi_lower <= current_price <= poi_upper):
            return

        history_before_trigger = all_ltf_closed[:index]
        choch_found, break_level = detect_bullish_choch(
            history_before_trigger,
            current,
        )

        if choch_found and break_level is not None:
            asset_state["status"] = "CHOCH_ACTIVE"
            asset_state["choch"] = {
                "break_level": float(break_level),
                "detected_at": current_ts,
            }
            journal_event(
                name,
                "BULLISH_CHOCH_DETECTED",
                current_ts,
                {
                    "break_level": float(break_level),
                    "close": current_price,
                    "close_position": candle_close_position(current),
                },
            )
            print(
                f"{name}: bullish CHoCH detected at {format_usd(float(break_level))}; "
                "waiting for retest inside frozen POI."
            )
        return

    # --------------------------------------------------------
    # 5. IDLE -> CREATE FROZEN POI
    # --------------------------------------------------------
    if status == "IDLE":
        if htf_support is None or htf_fta is None:
            return

        if htf_bias not in ALLOWED_LONG_HTF_BIAS:
            return

        dynamic_poi = build_frozen_poi(htf_support, htf_fta, current_ts)

        if not candle_touches_zone(
            current,
            dynamic_poi["lower"],
            dynamic_poi["upper"],
        ):
            return

        # Do not activate a POI on a candle that already closed below its floor.
        if current_price < dynamic_poi["lower"]:
            print(
                f"{name}: POI touch rejected; candle closed below zone at "
                f"{format_usd(current_price)}."
            )
            return

        if current_price >= dynamic_poi["fta"]:
            print(f"{name}: POI touch rejected; FTA is no longer above price.")
            return

        asset_state["poi"] = dynamic_poi
        asset_state["status"] = "POI_ACTIVE"

        # Capture a sweep on the same candle that first activates the POI.
        if ALLOW_DAILY_OPEN_SWEEP_TRIGGER:
            if record_daily_sweep(asset_state, current, daily_open):
                journal_event(
                    name,
                    "DAILY_OPEN_SWEEP_DETECTED",
                    current_ts,
                    {
                        "daily_open": daily_open,
                        "sweep_low": candle_float(current, "low"),
                    },
                )

        journal_event(
            name,
            "POI_CREATED",
            current_ts,
            {
                "support": dynamic_poi["support"],
                "lower": dynamic_poi["lower"],
                "upper": dynamic_poi["upper"],
                "fta": dynamic_poi["fta"],
                "bias": htf_bias,
            },
        )

        if send_alerts:
            send_level_2_alert(name, current_price, current_ts)

        print(
            f"{name}: POI_ACTIVE | support={format_usd(dynamic_poi['support'])} "
            f"zone={format_usd(dynamic_poi['lower'])}-{format_usd(dynamic_poi['upper'])} "
            f"FTA={format_usd(dynamic_poi['fta'])} | "
            f"alert={'YES' if send_alerts else 'NO'}"
        )
        return


# ============================================================
# PER-ASSET ANALYSIS & PIPELINE
# ============================================================


def analyze_asset(
    name: str,
    pair_code: str,
    state: Dict[str, Any],
) -> None:
    daily_candles = get_ohlc(
        pair_code,
        DAILY_STEP,
        DAILY_LIMIT,
        exclude_current_candle=False,
    )
    htf_closed_now = get_ohlc(
        pair_code,
        HTF_STEP,
        HTF_LIMIT,
        exclude_current_candle=True,
    )
    ltf_closed = get_ohlc(
        pair_code,
        LTF_STEP,
        LTF_LIMIT,
        exclude_current_candle=True,
    )

    if not daily_candles or not htf_closed_now or not ltf_closed:
        raise DataNotReady(f"{name}: insufficient Bitstamp OHLC data.")

    validate_candle_series(daily_candles, DAILY_STEP, f"{name} daily")
    validate_candle_series(htf_closed_now, HTF_STEP, f"{name} 4H")
    validate_candle_series(ltf_closed, LTF_STEP, f"{name} 15m")

    asset_state = normalize_asset_state(state, name)
    latest_closed_ts = candle_ts(ltf_closed[-1])
    last_processed = int(asset_state.get("last_processed_15m", 0))

    # On a fresh deployment, replay the retained candle history. Only signals
    # whose candle closed recently can produce a live email notification.
    if last_processed <= 0:
        asset_state["last_processed_15m"] = candle_ts(ltf_closed[0]) - LTF_STEP
        last_processed = int(asset_state["last_processed_15m"])
        print(
            f"{name}: initial history replay from "
            f"{asset_state['last_processed_15m']}; stale signals will not be activated."
        )

    # If downtime exceeded the retained LTF history, the missing segment cannot
    # be reconstructed from this endpoint response. Drop stale setup and replay
    # from the earliest retained closed candle instead of pretending continuity.
    earliest_ltf_ts = candle_ts(ltf_closed[0])
    if last_processed < earliest_ltf_ts - LTF_STEP:
        journal_event(
            name,
            "HISTORY_GAP_RECOVERY",
            earliest_ltf_ts,
            {
                "last_processed_15m": last_processed,
                "earliest_available_15m": earliest_ltf_ts,
            },
        )
        clear_setup(asset_state)
        asset_state["last_processed_15m"] = earliest_ltf_ts - LTF_STEP
        last_processed = int(asset_state["last_processed_15m"])
        print(f"{name}: history gap exceeds retained data; stale setup cleared and replay restarted.")

    pending_indices = [
        index
        for index, candle in enumerate(ltf_closed)
        if candle_ts(candle) > last_processed
    ]

    if not pending_indices:
        print(f"{name}: no new completed 15m candles; latest={latest_closed_ts}.")
        return

    for index in pending_indices:
        current = ltf_closed[index]
        current_ts = candle_ts(current)
        before_snapshot = copy.deepcopy(asset_state)
        recent = candle_is_recently_closed(current, utc_now_ts())

        try:
            process_one_15m_candle(
                name,
                asset_state,
                ltf_closed,
                index,
                daily_candles,
                htf_closed_now,
                send_alerts=recent,
            )
        except Exception:
            asset_state.clear()
            asset_state.update(before_snapshot)
            raise

        asset_state["last_processed_15m"] = current_ts


def run_cycle(state: Dict[str, Any]) -> None:
    errors: List[str] = []
    cycle_started = datetime.now(timezone.utc)

    with health_state_lock:
        health_state["last_cycle_started_utc"] = cycle_started.isoformat()
        health_state["last_cycle_ok"] = False

    try:
        retry_pending_alerts()
    except Exception as exc:
        message = f"Email outbox retry error: {type(exc).__name__}: {exc}"
        print(message)
        errors.append(message)

    for asset_name, pair_code in PAIRS.items():
        try:
            analyze_asset(asset_name, pair_code, state)
        except Exception as exc:
            message = f"{asset_name}: {type(exc).__name__}: {exc}"
            print(message)
            errors.append(message)

    save_state(state)

    cycle_finished = datetime.now(timezone.utc)
    with health_state_lock:
        health_state["last_cycle_finished_utc"] = cycle_finished.isoformat()
        health_state["last_cycle_finished_ts"] = int(cycle_finished.timestamp())
        health_state["last_cycle_ok"] = not errors
        health_state["last_cycle_errors"] = errors[-5:]

    if errors:
        print("Cycle completed with errors on one or more components.")
    else:
        print("Cycle completed successfully.")


# ============================================================
# 24/7 DAEMON EXECUTION
# ============================================================


def get_seconds_until_next_15m() -> int:
    """Return delay to the next :00:15/:15:15/:30:15/:45:15 UTC checkpoint."""
    now = utc_now_ts()
    current_boundary = now - (now % LTF_STEP)
    target = current_boundary + CANDLE_CLOSE_BUFFER_SECONDS

    if now >= target:
        target += LTF_STEP

    return max(0, target - now)


def main() -> None:
    threading.Thread(
        target=start_health_check_server,
        daemon=True,
        name="health-check-server",
    ).start()

    state = load_state()
    print(f"Starting Always-On 24/7 Crypto Monitor Daemon v{SCRIPT_VERSION}...")
    print("Markets: ETH/USD, SOL/USD | candle interval: 15m | mode: email signal monitor")

    # Ensure the journal schema exists before the first market cycle.
    with get_journal_connection():
        pass

    run_cycle(state)

    while True:
        seconds_to_wait = get_seconds_until_next_15m()
        target_time = datetime.fromtimestamp(
            utc_now_ts() + seconds_to_wait,
            timezone.utc,
        )
        print(
            f"Sleeping {seconds_to_wait}s until next check at "
            f"{target_time.strftime('%H:%M:%S')} UTC..."
        )
        time.sleep(seconds_to_wait)
        run_cycle(state)
        print(
            f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] "
            "Checked ETH/USD & SOL/USD (15m CDC). System Active."
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Daemon stopped by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"FATAL DAEMON ERROR: {type(exc).__name__}: {exc}")
        sys.exit(1)
