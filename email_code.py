import copy
import json
import math
import os
import smtplib
import sys
import time
from datetime import datetime, timezone
from email.header import Header
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple

import requests

# ============================================================
# CONFIGURATION
# ============================================================

EMAIL_SENDER = os.environ.get("EMAIL_SENDER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")
EMAIL_RECEIVER = EMAIL_SENDER

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465
SMTP_RETRIES = 3
SMTP_RETRY_DELAY_SECONDS = 3

BITSTAMP_BASE_URL = "https://www.bitstamp.net/api/v2/ohlc"
API_RETRIES = 4
API_RETRY_DELAY_SECONDS = 2

PAIRS = {
    "ETH/EUR": "etheur",
    "SOL/EUR": "soleur",
}

STATE_FILE = "state.json"
STATE_VERSION = 3

# Timeframes in seconds.
DAILY_STEP = 86400       # 1D
HTF_STEP = 14400         # 4H
LTF_STEP = 900           # 15m

# Bitstamp OHLC limits.
DAILY_LIMIT = 40
HTF_LIMIT = 500
LTF_LIMIT = 1000         # ~10.4 days of 15m history

# Confirmed fractal market structure.
SWING_LEFT = 2
SWING_RIGHT = 2

# Dynamic strategy parameters. No fixed ETH/SOL price levels are used.
POI_BELOW_SUPPORT_PCT = 0.004       # 0.4% below HTF support
POI_ABOVE_SUPPORT_PCT = 0.008       # 0.8% above HTF support
INVALIDATION_BUFFER_PCT = 0.002     # 0.2% below trigger/retest wick

# Retest tolerance around the broken 15m CHoCH level.
RETEST_TOLERANCE_PCT = 0.002        # 0.2% above broken level
RETEST_MAX_DEPTH_PCT = 0.005        # 0.5% penetration below broken level

# Minimum reward/risk required before a Level 3 email is sent.
MIN_RR = 2.0

# A CHoCH setup cannot remain valid forever.
CHOCH_MAX_AGE_SECONDS = 8 * 60 * 60

# Long-only system. Bearish HTF structure blocks long execution.
ALLOWED_LONG_HTF_BIAS = {"BULLISH", "RANGE"}

# Daily Open Sweep is retained as an allowed Level 3 trigger when the price is
# already inside the frozen POI.
ALLOW_DAILY_OPEN_SWEEP_TRIGGER = True

# Safety buffer in seconds after the exact 15m boundary before polling Bitstamp
CANDLE_CLOSE_BUFFER_SECONDS = 15


# ============================================================
# HTTP / API
# ============================================================

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Crypto15mMonitor/3.0",
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


def candle_close_ts(candle: Dict[str, Any]) -> int:
    return candle_ts(candle) + LTF_STEP


def is_finite_number(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def validate_candle_series(candles: List[Dict[str, Any]], step: int, label: str) -> None:
    """Validate ordering, strict step continuity (no gaps), and reject duplicates."""
    previous_ts: Optional[int] = None

    for candle in candles:
        ts = candle_ts(candle)
        if ts <= 0 or ts % step != 0:
            raise DataNotReady(f"{label}: invalid timestamp alignment: {ts}")

        if previous_ts is not None:
            if ts <= previous_ts:
                raise DataNotReady(f"{label}: duplicate/out-of-order timestamps detected.")
            if ts != previous_ts + step:
                raise DataNotReady(
                    f"{label}: missing candle gap detected between {previous_ts} and {ts}."
                )

        for field in ("open", "high", "low", "close"):
            if not is_finite_number(candle.get(field)):
                raise DataNotReady(f"{label}: invalid {field} value at {ts}.")

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
    """Fetch OHLC data with retries and return chronologically sorted candles."""
    url = f"{BITSTAMP_BASE_URL}/{pair_code}/"
    params = {
        "step": step,
        "limit": limit,
        "exclude_current_candle": str(exclude_current_candle).lower(),
    }

    last_error: Optional[Exception] = None

    for attempt in range(1, API_RETRIES + 1):
        try:
            response = SESSION.get(url, params=params, timeout=20)

            if response.status_code == 429 or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    delay = min(30, max(1, int(retry_after)))
                else:
                    delay = API_RETRY_DELAY_SECONDS * attempt
                raise requests.HTTPError(
                    f"HTTP {response.status_code}; retrying in {delay}s",
                    response=response,
                )

            response.raise_for_status()
            payload = response.json()
            candles = payload.get("data", {}).get("ohlc", [])

            if not isinstance(candles, list) or not candles:
                return None

            cleaned: List[Dict[str, Any]] = []
            for candle in candles:
                if not isinstance(candle, dict):
                    continue
                try:
                    int(candle["timestamp"])
                    float(candle["open"])
                    float(candle["high"])
                    float(candle["low"])
                    float(candle["close"])
                except (KeyError, TypeError, ValueError):
                    continue
                cleaned.append(candle)

            if not cleaned:
                return None

            cleaned.sort(key=lambda c: int(c["timestamp"]))
            validate_candle_series(cleaned, step, f"{pair_code}/step={step}")
            return cleaned

        except (requests.RequestException, ValueError, TypeError, DataNotReady) as exc:
            last_error = exc
            if attempt < API_RETRIES:
                delay = API_RETRY_DELAY_SECONDS * attempt
                print(
                    f"API retry {attempt}/{API_RETRIES - 1} for {pair_code}, "
                    f"step={step}: {exc}; sleeping {delay}s"
                )
                time.sleep(delay)
            else:
                print(f"API Error ({pair_code}, step={step}): {exc}")

    if last_error is not None:
        return None
    return None


# ============================================================
# EMAIL
# ============================================================


def send_alert(subject: str, body: str) -> None:
    """Send one UTF-8 email through Gmail SMTP SSL with limited retry."""
    if not EMAIL_SENDER or not EMAIL_PASSWORD:
        raise RuntimeError("EMAIL_SENDER / EMAIL_PASSWORD are not available.")

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
    """Atomically replace state.json."""
    temp_file = f"{STATE_FILE}.tmp"

    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, ensure_ascii=False, sort_keys=True)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(temp_file, STATE_FILE)


def normalize_asset_state(state: Dict[str, Any], name: str) -> Dict[str, Any]:
    current = state.get(name)

    if not isinstance(current, dict):
        current = fresh_asset_state()
        state[name] = current

    current.setdefault("status", "IDLE")
    current.setdefault("last_processed_15m", 0)
    return current


def clear_setup(asset_state: Dict[str, Any]) -> None:
    asset_state.clear()
    asset_state.update(fresh_asset_state())


# ============================================================
# CANDLE HELPERS
# ============================================================


def candle_float(candle: Dict[str, Any], key: str) -> float:
    value = float(candle[key])
    if not math.isfinite(value):
        raise ValueError(f"Non-finite candle value for {key}.")
    return value


def candle_ts(candle: Dict[str, Any]) -> int:
    return int(candle["timestamp"])


def is_bullish(candle: Dict[str, Any]) -> bool:
    return candle_float(candle, "close") > candle_float(candle, "open")


def candle_touches_zone(candle: Dict[str, Any], lower: float, upper: float) -> bool:
    """Return True when the candle's trading range touches the POI."""
    high = candle_float(candle, "high")
    low = candle_float(candle, "low")
    return low <= upper and high >= lower


# ============================================================
# DAILY OPEN / HTF CONTEXT BY HISTORICAL TIMESTAMP
# ============================================================


def get_daily_open_for_timestamp(
    daily_candles: List[Dict[str, Any]],
    timestamp: int,
) -> Optional[float]:
    """Find the Daily Open for the UTC calendar day of a 15m candle."""
    day_start = timestamp - (timestamp % DAILY_STEP)

    for candle in reversed(daily_candles):
        ts = candle_ts(candle)
        if ts == day_start:
            return candle_float(candle, "open")
        if ts < day_start:
            break

    return None


def htf_candles_closed_by_timestamp(
    htf_closed_now: List[Dict[str, Any]],
    timestamp: int,
) -> List[Dict[str, Any]]:
    """Return only 4H candles that were closed at the time of the 15m candle."""
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
    """Return confirmed fractal swing lows as (timestamp, price)."""
    swings: List[Tuple[int, float]] = []

    start = SWING_LEFT
    end = len(candles) - SWING_RIGHT

    for i in range(start, end):
        current_low = candle_float(candles[i], "low")

        left_ok = all(
            current_low <= candle_float(candles[j], "low")
            for j in range(i - SWING_LEFT, i)
        )
        right_ok = all(
            current_low <= candle_float(candles[j], "low")
            for j in range(i + 1, i + SWING_RIGHT + 1)
        )

        if left_ok and right_ok:
            swings.append((candle_ts(candles[i]), current_low))

    return swings


def find_confirmed_swing_highs(
    candles: List[Dict[str, Any]],
) -> List[Tuple[int, float]]:
    """Return confirmed fractal swing highs as (timestamp, price)."""
    swings: List[Tuple[int, float]] = []

    start = SWING_LEFT
    end = len(candles) - SWING_RIGHT

    for i in range(start, end):
        current_high = candle_float(candles[i], "high")

        left_ok = all(
            current_high >= candle_float(candles[j], "high")
            for j in range(i - SWING_LEFT, i)
        )
        right_ok = all(
            current_high >= candle_float(candles[j], "high")
            for j in range(i + 1, i + SWING_RIGHT + 1)
        )

        if left_ok and right_ok:
            swings.append((candle_ts(candles[i]), current_high))

    return swings


def determine_htf_bias(htf_closed: List[Dict[str, Any]]) -> str:
    """
    Determine simple 4H structural bias from confirmed fractals.

    BULLISH = latest confirmed swing high and low are both higher than previous.
    BEARISH = latest confirmed swing high and low are both lower than previous.
    RANGE   = mixed/non-directional structure or insufficient information.
    """
    highs = find_confirmed_swing_highs(htf_closed)
    lows = find_confirmed_swing_lows(htf_closed)

    if len(highs) < 2 or len(lows) < 2:
        return "RANGE"

    prev_high = highs[-2][1]
    last_high = highs[-1][1]
    prev_low = lows[-2][1]
    last_low = lows[-1][1]

    if last_high > prev_high and last_low > prev_low:
        return "BULLISH"

    if last_high < prev_high and last_low < prev_low:
        return "BEARISH"

    return "RANGE"


def find_htf_key_levels(
    htf_closed: List[Dict[str, Any]],
    current_price: float,
) -> Tuple[Optional[float], Optional[float], str]:
    """Find nearest confirmed 4H swing low below price, nearest swing high above price, and bias."""
    if len(htf_closed) < (SWING_LEFT + SWING_RIGHT + 4):
        return None, None, "RANGE"

    swing_lows = find_confirmed_swing_lows(htf_closed)
    swing_highs = find_confirmed_swing_highs(htf_closed)

    supports = [price for _, price in swing_lows if price < current_price]
    resistances = [price for _, price in swing_highs if price > current_price]

    support = max(supports) if supports else None
    fta = min(resistances) if resistances else None
    bias = determine_htf_bias(htf_closed)

    return support, fta, bias


# ============================================================
# 15m STRUCTURE / TRIGGERS
# ============================================================


def find_latest_confirmed_swing_high(
    candles_before_trigger: List[Dict[str, Any]],
) -> Optional[Tuple[int, float]]:
    highs = find_confirmed_swing_highs(candles_before_trigger)
    return highs[-1] if highs else None


def has_recent_bearish_structure(
    candles_before_trigger: List[Dict[str, Any]],
) -> bool:
    """Require a basic recent LH/LL sequence before calling a break CHoCH."""
    highs = find_confirmed_swing_highs(candles_before_trigger)
    lows = find_confirmed_swing_lows(candles_before_trigger)

    if len(highs) < 2 or len(lows) < 2:
        return False

    return highs[-1][1] < highs[-2][1] and lows[-1][1] < lows[-2][1]


def detect_bullish_choch(
    ltf_history_before_trigger: List[Dict[str, Any]],
    current_candle: Dict[str, Any],
) -> Tuple[bool, Optional[float]]:
    """
    Bullish CHoCH requires:
      1) confirmed recent LH/LL structure, and
      2) the newest closed 15m candle closes bullish above its latest
         confirmed swing high.
    """
    if len(ltf_history_before_trigger) < 12:
        return False, None

    latest_high = find_latest_confirmed_swing_high(ltf_history_before_trigger)
    if latest_high is None:
        return False, None

    _, break_level = latest_high
    current_close = candle_float(current_candle, "close")
    current_open = candle_float(current_candle, "open")

    bearish_structure = has_recent_bearish_structure(ltf_history_before_trigger[-40:])
    bullish_break = current_close > break_level and current_close > current_open

    return bearish_structure and bullish_break, break_level


def detect_daily_open_sweep(
    prev_candle: Dict[str, Any],
    current_candle: Dict[str, Any],
    daily_open: float,
) -> bool:
    """Detect a bullish Daily Open sweep/reclaim on the closed 15m candle."""
    prev_low = candle_float(prev_candle, "low")
    curr_low = candle_float(current_candle, "low")
    curr_close = candle_float(current_candle, "close")

    return (
        (prev_low < daily_open or curr_low < daily_open)
        and curr_close > daily_open
        and is_bullish(current_candle)
    )


def detect_retest_of_choch(
    current_candle: Dict[str, Any],
    break_level: float,
) -> bool:
    """Confirm a bullish retest/reclaim of the broken 15m structure level."""
    curr_low = candle_float(current_candle, "low")
    curr_close = candle_float(current_candle, "close")

    upper_touch = break_level * (1.0 + RETEST_TOLERANCE_PCT)
    lower_depth = break_level * (1.0 - RETEST_MAX_DEPTH_PCT)

    touched_zone = lower_depth <= curr_low <= upper_touch
    reclaimed = curr_close > break_level
    bullish = is_bullish(current_candle)

    return touched_zone and reclaimed and bullish


# ============================================================
# EXECUTION / RISK
# ============================================================


def build_frozen_poi(support: float, fta: float) -> Dict[str, float]:
    lower = support * (1.0 - POI_BELOW_SUPPORT_PCT)
    upper = support * (1.0 + POI_ABOVE_SUPPORT_PCT)

    return {
        "support": support,
        "lower": lower,
        "upper": upper,
        "fta": fta,
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


def calculate_rr(entry: float, invalidation: float, tp: float) -> Optional[float]:
    risk = entry - invalidation
    reward = tp - entry

    if risk <= 0 or reward <= 0:
        return None

    return reward / risk


def validate_execution(
    entry: float,
    invalidation: float,
    tp: float,
) -> Optional[float]:
    rr = calculate_rr(entry, invalidation, tp)

    if rr is None or rr < MIN_RR:
        return None

    return rr


# ============================================================
# ALERT TEMPLATES
# ============================================================


def pair_clean(name: str) -> str:
    return name.replace("/", " - ")


def send_level_2_alert(name: str, price: float) -> None:
    subject = f"🟠 {pair_clean(name)}: ΣΕ ΕΠΙΦΥΛΑΚΗ"
    body = (
        "Asset:\n"
        f"      {name}\n\n"
        "Τρέχουσα\n"
        f"      Τιμή: {price:.2f}€\n\n"
        "🟠\n"
        "      Κατάσταση: Είσοδος σε Ζώνη Ενδιαφέροντος (POI)"
    )
    send_alert(subject, body)


def send_level_2_fail_alert(name: str, price: float) -> None:
    subject = f"🔴 {pair_clean(name)}: FAIL"
    body = (
        "Asset:\n"
        f"      {name}\n\n"
        "Τρέχουσα\n"
        f"      Τιμή: {price:.2f}€\n\n"
        "🔴\n"
        "      ΑΚΥΡΩΣΗ ΕΤΟΙΜΟΤΗΤΑΣ"
    )
    send_alert(subject, body)


def send_level_3_alert(
    name: str,
    entry: float,
    invalidation: float,
    tp: float,
) -> None:
    subject = f"🚨 LEVEL 3: {pair_clean(name)} EXECUTION"
    body = (
        "Asset:\n"
        f"      {name}\n\n"
        "🟢\n"
        f"      Entry: {entry:.2f}€ 🟢\n\n"
        "ΑΠΟΤΥΧΙΑ:\n"
        f"      {invalidation:.2f}€\n\n"
        "Take\n"
        f"      Profit: {tp:.2f}€"
    )
    send_alert(subject, body)


# ============================================================
# TRADE MANAGEMENT
# ============================================================


def execute_trade(
    name: str,
    asset_state: Dict[str, Any],
    current_candle: Dict[str, Any],
    previous_candle: Dict[str, Any],
    trigger_label: str,
    *,
    send_alerts: bool,
) -> bool:
    poi = asset_state.get("poi")
    if not isinstance(poi, dict):
        print(f"{name}: execution blocked - no frozen POI.")
        return False

    entry = candle_float(current_candle, "close")
    invalidation = calculate_invalidation(current_candle, previous_candle)
    tp = float(poi["fta"])

    rr = validate_execution(entry, invalidation, tp)
    if rr is None:
        print(
            f"{name}: execution blocked - R:R < {MIN_RR:.1f} or invalid levels | "
            f"entry={entry:.6f} fail={invalidation:.6f} tp={tp:.6f}"
        )
        return False

    if send_alerts:
        send_level_3_alert(name, entry, invalidation, tp)

    asset_state["status"] = "EXECUTED"
    asset_state["trade"] = {
        "entry": entry,
        "invalidation": invalidation,
        "tp": tp,
        "rr": rr,
        "trigger": trigger_label,
        "executed_at": candle_ts(current_candle),
    }
    asset_state.pop("choch", None)

    print(
        f"LEVEL 3 EXECUTION: {name} | {trigger_label} | "
        f"entry={entry:.6f} fail={invalidation:.6f} tp={tp:.6f} RR={rr:.2f} | "
        f"alert={'YES' if send_alerts else 'NO'}"
    )
    return True


def monitor_executed_trade(
    name: str,
    asset_state: Dict[str, Any],
    current_candle: Dict[str, Any],
) -> None:
    trade = asset_state.get("trade")

    if not isinstance(trade, dict):
        clear_setup(asset_state)
        print(f"{name}: EXECUTED state had no trade data -> IDLE.")
        return

    invalidation = float(trade.get("invalidation", 0.0))
    tp = float(trade.get("tp", 0.0))

    current_low = candle_float(current_candle, "low")
    current_high = candle_float(current_candle, "high")

    if invalidation > 0 and current_low <= invalidation:
        print(f"{name}: trade invalidated at/through {invalidation:.6f}")
        clear_setup(asset_state)
        return

    if tp > 0 and current_high >= tp:
        print(f"{name}: TP reached at/through {tp:.6f}")
        clear_setup(asset_state)
        return


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
    """Process exactly one completed 15m candle without future HTF leakage."""
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
        f"{name} | candle={current_ts} close={current_price:.6f} "
        f"DO={daily_open:.6f} bias={htf_bias} "
        f"support={htf_support} FTA={htf_fta} status={status} "
        f"alerts={'LIVE' if send_alerts else 'SILENT'}"
    )

    # --------------------------------------------------------
    # 1. EXISTING TRADE
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

        if break_level <= 0 or frozen_fta <= 0 or detected_at <= 0:
            clear_setup(asset_state)
            print(f"{name}: invalid CHOCH data -> IDLE.")
            return

        if current_ts - detected_at > CHOCH_MAX_AGE_SECONDS:
            clear_setup(asset_state)
            print(f"{name}: CHoCH expired -> IDLE.")
            return

        if htf_bias not in ALLOWED_LONG_HTF_BIAS:
            clear_setup(asset_state)
            print(f"{name}: CHoCH cancelled - HTF bias={htf_bias}.")
            return

        if current_price >= frozen_fta:
            clear_setup(asset_state)
            print(f"{name}: CHoCH cancelled - frozen FTA already reached.")
            return

        # STRICT POI: The Retest must occur strictly inside the frozen POI
        if current_price > frozen_upper:
            clear_setup(asset_state)
            print(f"{name}: CHoCH retest occurred above frozen POI -> invalidated.")
            return

        # Long execution requires price above Daily Open.
        if current_price <= daily_open:
            print(f"{name}: waiting - CHoCH retest candle is below/equal DO.")
            if current_price < frozen_lower:
                clear_setup(asset_state)
                print(f"{name}: CHoCH setup invalidated below frozen POI.")
            return

        if detect_retest_of_choch(current, break_level):
            execute_trade(
                name,
                asset_state,
                current,
                previous,
                trigger_label="CHoCH + Retest",
                send_alerts=send_alerts,
            )
            return

        if current_price < frozen_lower:
            clear_setup(asset_state)
            print(f"{name}: CHoCH setup invalidated below frozen POI.")
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

        poi_lower = float(poi["lower"])
        poi_upper = float(poi["upper"])

        # FAIL is candle-close based and uses the frozen zone.
        if current_price < poi_lower:
            if send_alerts:
                send_level_2_fail_alert(name, current_price)
            clear_setup(asset_state)
            print(f"{name}: Level 2 FAIL | alert={'YES' if send_alerts else 'NO'}")
            return

        if current_price > poi_upper and not candle_touches_zone(current, poi_lower, poi_upper):
            clear_setup(asset_state)
            print(f"{name}: left POI upward without trigger -> IDLE.")
            return

    # --------------------------------------------------------
    # 4. IDLE -> CREATE FROZEN POI
    # --------------------------------------------------------
    if status == "IDLE":
        if htf_support is None or htf_fta is None:
            return

        if htf_bias not in ALLOWED_LONG_HTF_BIAS:
            return

        dynamic_poi = build_frozen_poi(htf_support, htf_fta)

        if not candle_touches_zone(
            current,
            dynamic_poi["lower"],
            dynamic_poi["upper"],
        ):
            return

        asset_state["poi"] = dynamic_poi
        asset_state["status"] = "POI_ACTIVE"

        if send_alerts:
            send_level_2_alert(name, current_price)

        print(
            f"{name}: POI_ACTIVE | support={dynamic_poi['support']:.6f} "
            f"zone={dynamic_poi['lower']:.6f}-{dynamic_poi['upper']:.6f} "
            f"FTA={dynamic_poi['fta']:.6f} | "
            f"alert={'YES' if send_alerts else 'NO'}"
        )
        return

    # Refresh status after the POI block.
    status = asset_state.get("status", "IDLE")
    if status != "POI_ACTIVE":
        return

    poi = asset_state.get("poi")
    if not isinstance(poi, dict):
        return

    poi_lower = float(poi["lower"])
    poi_upper = float(poi["upper"])

    if not (poi_lower <= current_price <= poi_upper):
        return

    if htf_bias not in ALLOWED_LONG_HTF_BIAS:
        return

    if current_price <= daily_open:
        return

    # A) Daily Open sweep/reclaim inside POI.
    if ALLOW_DAILY_OPEN_SWEEP_TRIGGER and detect_daily_open_sweep(
        previous,
        current,
        daily_open,
    ):
        executed = execute_trade(
            name,
            asset_state,
            current,
            previous,
            trigger_label="Daily Open Sweep",
            send_alerts=send_alerts,
        )
        if executed:
            return

    # B) Bullish 15m CHoCH.
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
        print(
            f"{name}: bullish CHoCH detected at {break_level:.6f}; waiting for retest."
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

    # Bootstrap on fresh deployment: set cursor to latest candle without alerts
    if last_processed <= 0:
        asset_state["last_processed_15m"] = latest_closed_ts
        asset_state["status"] = "IDLE"
        asset_state.pop("poi", None)
        asset_state.pop("choch", None)
        asset_state.pop("trade", None)
        print(
            f"{name}: initial bootstrap -> cursor set to {latest_closed_ts}; "
            f"no historical alerts sent."
        )
        return

    pending_indices = [
        i for i, candle in enumerate(ltf_closed)
        if candle_ts(candle) > last_processed
    ]

    if not pending_indices:
        print(f"{name}: no new completed 15m candles.")
        return

    latest_pending_index = pending_indices[-1]

    for index in pending_indices:
        current_ts = candle_ts(ltf_closed[index])
        before_snapshot = copy.deepcopy(asset_state)

        # In always-on daemon mode, the newest closed candle is always live
        is_latest_pending = index == latest_pending_index

        try:
            process_one_15m_candle(
                name,
                asset_state,
                ltf_closed,
                index,
                daily_candles,
                htf_closed_now,
                send_alerts=is_latest_pending,
            )
        except Exception:
            asset_state.clear()
            asset_state.update(before_snapshot)
            raise

        asset_state["last_processed_15m"] = current_ts


# ============================================================
# 24/7 DAEMON EXECUTION
# ============================================================


def get_seconds_until_next_15m() -> int:
    """Calculate exact seconds until next :00, :15, :30, :45 + safety buffer."""
    now = utc_now_ts()
    remainder = now % LTF_STEP
    sleep_needed = (LTF_STEP - remainder) + CANDLE_CLOSE_BUFFER_SECONDS
    return sleep_needed


def run_cycle(state: Dict[str, Any]) -> None:
    errors: List[str] = []
    for asset_name, pair_code in PAIRS.items():
        try:
            analyze_asset(asset_name, pair_code, state)
        except Exception as exc:
            message = f"{asset_name}: {type(exc).__name__}: {exc}"
            print(message)
            errors.append(message)

    save_state(state)
    if errors:
        print("Cycle completed with errors on one or more assets.")
    else:
        print("Cycle completed successfully.")


def main() -> None:
    state = load_state()
    print("Starting Always-On 24/7 Crypto Monitor Daemon...")

    # Immediate first check upon starting
    run_cycle(state)

    while True:
        seconds_to_wait = get_seconds_until_next_15m()
        target_time = datetime.fromtimestamp(utc_now_ts() + seconds_to_wait, timezone.utc)
        print(
            f"Sleeping {seconds_to_wait}s until next check at "
            f"{target_time.strftime('%H:%M:%S')} UTC..."
        )
        time.sleep(seconds_to_wait)
        run_cycle(state)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Daemon stopped by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"FATAL DAEMON ERROR: {exc}")
        sys.exit(1)
