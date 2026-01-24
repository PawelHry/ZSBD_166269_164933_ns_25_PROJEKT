from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, Mapping, TypedDict, cast

import oracledb
import requests


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


ORA_USER = os.getenv("ORA_USER", "projekt")
ORA_PASSWORD = os.getenv("ORA_PASSWORD", "projekt123")
ORA_DSN = os.getenv("ORA_DSN", "localhost:1521/FREEPDB1")


@dataclass(frozen=True)
class City:
    city_id: int
    latitude: float
    longitude: float


class WeatherRow(TypedDict):
    city_id: int
    raw_id: int
    observed_at_utc: datetime
    utc_offset_seconds: int
    temperature_c: Optional[float]
    humidity_pct: Optional[float]
    precipitation_mm: Optional[float]
    wind_speed_kmh: Optional[float]


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_run_id() -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{now}-{uuid.uuid4().hex[:8]}"


def as_mapping(value: object, what: str) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return cast(Mapping[str, object], value)
    raise ValueError(f"{what} nie jest obiektem JSON (dict)")


def as_str(value: object, what: str) -> str:
    if isinstance(value, str):
        return value
    raise ValueError(f"{what} nie jest stringiem")


def as_int(value: object, what: str) -> int:
    # bool jest podtypem int w Pythonie -> odrzucamy
    if isinstance(value, bool):
        raise ValueError(f"{what} nie jest int (to bool)")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    raise ValueError(f"{what} nie jest int")


def as_float_opt(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def get_required(obj: Mapping[str, object], key: str, what: str) -> object:
    if key not in obj:
        raise ValueError(f"Brak {what}: {key}")
    return obj[key]


def get_optional(obj: Mapping[str, object], key: str) -> object:
    return obj.get(key, None)


def get_returning_int(var: oracledb.Var) -> int:
    value: object = var.getvalue()

    if isinstance(value, list):
        if not value:
            raise RuntimeError("RETURNING zwrócił pustą listę")
        value0: object = value[0]
    else:
        value0 = value

    if value0 is None:
        raise RuntimeError("RETURNING zwrócił None")

    if isinstance(value0, bool):
        raise RuntimeError("RETURNING zwrócił bool (niepoprawne)")

    if isinstance(value0, (int, float)):
        return int(value0)

    if isinstance(value0, str) and value0.isdigit():
        return int(value0)

    raise RuntimeError(f"RETURNING zwrócił nieobsługiwany typ: {type(value0)}")


def log_event(
    cur: oracledb.Cursor,
    *,
    log_level: str,
    source: str,
    action: str,
    result: str = "OK",
    sqlcode: Optional[int] = None,
    error_message: Optional[str] = None,
    city_id: Optional[int] = None,
    raw_id: Optional[int] = None,
    weather_id: Optional[int] = None,
    app_user: Optional[str] = None,
    run_id: Optional[str] = None,
    details: Optional[str] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO logs (
            log_level, source, action, result,
            sqlcode, error_message,
            city_id, raw_id, weather_id,
            app_user, run_id, details
        ) VALUES (
            :log_level, :source, :action, :result,
            :sqlcode, :error_message,
            :city_id, :raw_id, :weather_id,
            :app_user, :run_id, :details
        )
        """,
        dict(
            log_level=log_level,
            source=source,
            action=action,
            result=result,
            sqlcode=sqlcode,
            error_message=error_message,
            city_id=city_id,
            raw_id=raw_id,
            weather_id=weather_id,
            app_user=app_user,
            run_id=run_id,
            details=details,
        ),
    )


def fetch_active_cities(cur: oracledb.Cursor) -> list[City]:
    cur.execute(
        """
        SELECT city_id, latitude, longitude
        FROM cities
        WHERE is_active = 1
        ORDER BY city_id
        """
    )
    rows = cur.fetchall()

    out: list[City] = []
    for r in rows:
        city_id = int(r[0])
        lat = float(r[1])
        lon = float(r[2])
        out.append(City(city_id=city_id, latitude=lat, longitude=lon))
    return out


def insert_raw_import_ok(
    cur: oracledb.Cursor,
    *,
    request_url: str,
    payload: str,
    payload_hash: str,
) -> int:
    raw_id_var = cur.var(oracledb.NUMBER)
    cur.execute(
        """
        INSERT INTO raw_import (
            source, request_url, payload_format, payload,
            status, err_msg, payload_hash
        ) VALUES (
            'open-meteo', :request_url, 'JSON', :payload,
            'OK', NULL, :payload_hash
        )
        RETURNING raw_id INTO :raw_id
        """,
        dict(
            request_url=request_url,
            payload=payload,
            payload_hash=payload_hash,
            raw_id=raw_id_var,
        ),
    )
    return get_returning_int(raw_id_var)


def insert_raw_import_error(
    cur: oracledb.Cursor,
    *,
    request_url: str,
    payload: str,
    payload_hash: str,
    err_msg: str,
) -> int:
    raw_id_var = cur.var(oracledb.NUMBER)
    cur.execute(
        """
        INSERT INTO raw_import (
            source, request_url, payload_format, payload,
            status, err_msg, payload_hash
        ) VALUES (
            'open-meteo', :request_url, 'JSON', :payload,
            'ERROR', :err_msg, :payload_hash
        )
        RETURNING raw_id INTO :raw_id
        """,
        dict(
            request_url=request_url,
            payload=payload,
            payload_hash=payload_hash,
            err_msg=err_msg,
            raw_id=raw_id_var,
        ),
    )
    return get_returning_int(raw_id_var)


def merge_weather_rows(cur: oracledb.Cursor, rows: Sequence[WeatherRow]) -> int:
    cur.executemany(
        """
        MERGE INTO weather w
        USING (
            SELECT
                :city_id            AS city_id,
                :raw_id             AS raw_id,
                :observed_at_utc    AS observed_at_utc,
                :utc_offset_seconds AS utc_offset_seconds,
                :temperature_c      AS temperature_c,
                :humidity_pct       AS humidity_pct,
                :precipitation_mm   AS precipitation_mm,
                :wind_speed_kmh     AS wind_speed_kmh
            FROM dual
        ) s
        ON (
            w.city_id = s.city_id
            AND w.observed_at_utc = s.observed_at_utc
        )
        WHEN NOT MATCHED THEN
            INSERT (
                city_id, raw_id, observed_at_utc, utc_offset_seconds,
                temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh
            )
            VALUES (
                s.city_id, s.raw_id, s.observed_at_utc, s.utc_offset_seconds,
                s.temperature_c, s.humidity_pct, s.precipitation_mm, s.wind_speed_kmh
            )
        """,
        rows,
    )

    rc = cur.rowcount
    return int(rc) if isinstance(rc, int) else 0


def call_open_meteo(cities: Sequence[City]) -> tuple[str, str]:
    lat_list = ",".join(f"{c.latitude:.6f}" for c in cities)
    lon_list = ",".join(f"{c.longitude:.6f}" for c in cities)

    params = {
        "latitude": lat_list,
        "longitude": lon_list,
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "timezone": "auto",
    }

    req = requests.Request("GET", OPEN_METEO_URL, params=params).prepare()
    request_url = req.url or OPEN_METEO_URL

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    return request_url, resp.text


def decode_json_payload(payload: str) -> list[object]:
    root: object = json.loads(payload)

    if isinstance(root, list):
        return list(root)
    if isinstance(root, Mapping):
        return [root]
    raise ValueError("Payload JSON nie jest ani listą ani obiektem")


def parse_observation(item_obj: object) -> tuple[datetime, int, Optional[float], Optional[float], Optional[float], Optional[float]]:
    item = as_mapping(item_obj, "Element odpowiedzi")

    offset_obj = get_required(item, "utc_offset_seconds", "utc_offset_seconds")
    offset = as_int(offset_obj, "utc_offset_seconds")

    current_obj = get_optional(item, "current")
    current = as_mapping(current_obj, "current") if current_obj is not None else {}

    time_obj = get_required(current, "time", "current.time")
    local_time_str = as_str(time_obj, "current.time")

    dt_local = datetime.fromisoformat(local_time_str)

    # jeśli przyszłoby z tzinfo, normalizujemy
    if dt_local.tzinfo is not None:
        dt_utc_aware = dt_local.astimezone(timezone.utc)
        dt_utc = dt_utc_aware.replace(tzinfo=None)
    else:
        dt_utc = dt_local - timedelta(seconds=offset)

    temp = as_float_opt(get_optional(current, "temperature_2m"))
    hum = as_float_opt(get_optional(current, "relative_humidity_2m"))
    prec = as_float_opt(get_optional(current, "precipitation"))
    wind = as_float_opt(get_optional(current, "wind_speed_10m"))

    return dt_utc, offset, temp, hum, prec, wind


def validate_values(
    temp_c: Optional[float],
    hum_pct: Optional[float],
    prec_mm: Optional[float],
    wind_kmh: Optional[float],
) -> list[str]:
    errs: list[str] = []

    if temp_c is not None and not (-80 <= temp_c <= 80):
        errs.append(f"temperature_c poza zakresem: {temp_c}")
    if hum_pct is not None and not (0 <= hum_pct <= 100):
        errs.append(f"humidity_pct poza zakresem: {hum_pct}")
    if prec_mm is not None and prec_mm < 0:
        errs.append(f"precipitation_mm < 0: {prec_mm}")
    if wind_kmh is not None and wind_kmh < 0:
        errs.append(f"wind_speed_kmh < 0: {wind_kmh}")

    return errs


def handle_fetch_exception(
    conn: oracledb.Connection,
    *,
    run_id: str,
    exc: requests.RequestException,
) -> int:
    try:
        cur = conn.cursor()

        resp: Optional[requests.Response] = exc.response
        request_url = OPEN_METEO_URL
        payload_text = ""
        status_code: Optional[int] = None

        if resp is not None:
            if isinstance(resp.url, str) and resp.url:
                request_url = resp.url
            payload_text = resp.text
            status_code = resp.status_code

        err_msg = str(exc)
        if status_code is not None:
            err_msg = f"HTTP {status_code}: {err_msg}"

        payload_to_store = payload_text if payload_text else err_msg
        payload_hash = sha256_hex(payload_to_store)

        raw_id = insert_raw_import_error(
            cur,
            request_url=request_url,
            payload=payload_to_store,
            payload_hash=payload_hash,
            err_msg=err_msg,
        )

        log_event(
            cur,
            log_level="ERROR",
            source="LOADER",
            action="FETCH_FAIL",
            result="FAIL",
            raw_id=raw_id,
            run_id=run_id,
            error_message=err_msg,
        )

        conn.commit()
        return 2

    except Exception:
        conn.rollback()
        return 2


def main() -> int:
    run_id = build_run_id()

    conn = oracledb.connect(user=ORA_USER, password=ORA_PASSWORD, dsn=ORA_DSN)
    conn.autocommit = False

    try:
        cur = conn.cursor()

        log_event(
            cur,
            log_level="INFO",
            source="LOADER",
            action="RUN_START",
            result="OK",
            run_id=run_id,
            details="start loader_current",
        )

        cities = fetch_active_cities(cur)
        if not cities:
            log_event(
                cur,
                log_level="WARN",
                source="LOADER",
                action="NO_ACTIVE_CITIES",
                result="SKIP",
                run_id=run_id,
            )
            conn.commit()
            return 0

        request_url, payload = call_open_meteo(cities)
        payload_hash = sha256_hex(payload)

        raw_id = insert_raw_import_ok(cur, request_url=request_url, payload=payload, payload_hash=payload_hash)

        log_event(
            cur,
            log_level="INFO",
            source="LOADER",
            action="FETCH_OK",
            result="OK",
            raw_id=raw_id,
            run_id=run_id,
            details=f"cities={len(cities)}",
        )

        responses = decode_json_payload(payload)

        n = min(len(cities), len(responses))
        rows_to_merge: list[WeatherRow] = []
        validation_warns = 0

        for i in range(n):
            city = cities[i]
            item_obj = responses[i]

            try:
                observed_at_utc, offset, temp, hum, prec, wind = parse_observation(item_obj)
            except Exception as e:
                validation_warns += 1
                log_event(
                    cur,
                    log_level="WARN",
                    source="LOADER",
                    action="PARSE_FAIL",
                    result="SKIP",
                    city_id=city.city_id,
                    raw_id=raw_id,
                    run_id=run_id,
                    error_message=str(e),
                )
                continue

            errs = validate_values(temp, hum, prec, wind)
            if errs:
                validation_warns += 1
                log_event(
                    cur,
                    log_level="WARN",
                    source="LOADER",
                    action="VALIDATE_FAIL",
                    result="SKIP",
                    city_id=city.city_id,
                    raw_id=raw_id,
                    run_id=run_id,
                    details="; ".join(errs),
                )
                continue

            row: WeatherRow = {
                "city_id": city.city_id,
                "raw_id": raw_id,
                "observed_at_utc": observed_at_utc,
                "utc_offset_seconds": offset,
                "temperature_c": temp,
                "humidity_pct": hum,
                "precipitation_mm": prec,
                "wind_speed_kmh": wind,
            }
            rows_to_merge.append(row)

        inserted = 0
        if rows_to_merge:
            inserted = merge_weather_rows(cur, rows_to_merge)

        log_event(
            cur,
            log_level="INFO",
            source="LOADER",
            action="INSERT_WEATHER",
            result="OK",
            raw_id=raw_id,
            run_id=run_id,
            details=f"inserted={inserted}; prepared={len(rows_to_merge)}; validate_warns={validation_warns}",
        )

        conn.commit()
        return 0

    except requests.RequestException as e:
        return handle_fetch_exception(conn, run_id=run_id, exc=e)

    except Exception as e:
        try:
            cur = conn.cursor()
            log_event(
                cur,
                log_level="ERROR",
                source="LOADER",
                action="RUN_FAIL",
                result="FAIL",
                run_id=run_id,
                error_message=str(e),
            )
            conn.commit()
        except Exception:
            conn.rollback()
        return 1

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
