CREATE TABLE cities (
    city_id       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city_name     VARCHAR2(100) NOT NULL,
    country_code  VARCHAR2(2)   DEFAULT 'PL' NOT NULL,
    latitude      NUMBER(9,6)   NOT NULL,
    longitude     NUMBER(9,6)   NOT NULL,
    timezone_name VARCHAR2(64)  DEFAULT 'Europe/Warsaw' NOT NULL,
    is_active     NUMBER(1)     DEFAULT 1 NOT NULL,

    CONSTRAINT cities_lat_chk CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT cities_lon_chk CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT cities_active_chk CHECK (is_active IN (0, 1))
);

CREATE UNIQUE INDEX cities_lat_lon_uq ON cities(latitude, longitude);


CREATE TABLE units (
    unit_id           NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dimension         VARCHAR2(50) NOT NULL,
    unit_symbol       VARCHAR2(20) NOT NULL,
    unit_name         VARCHAR2(50),

    to_canonical_mul  NUMBER(18,8),
    to_canonical_add  NUMBER(18,8),
    canonical_unit_id NUMBER,

    CONSTRAINT units_uq UNIQUE (dimension, unit_symbol),
    CONSTRAINT units_canon_fk FOREIGN KEY (canonical_unit_id) REFERENCES units(unit_id)
);


CREATE TABLE raw_import (
    raw_id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fetched_at_utc  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,

    source         VARCHAR2(50) DEFAULT 'open-meteo' NOT NULL,
    request_url    CLOB,
    payload_format VARCHAR2(10) DEFAULT 'JSON' NOT NULL,
    payload        CLOB NOT NULL,

    status         VARCHAR2(10) DEFAULT 'OK' NOT NULL,
    err_msg        VARCHAR2(4000),

    payload_hash   VARCHAR2(64),

    CONSTRAINT raw_import_status_chk CHECK (status IN ('OK', 'ERROR')),
    CONSTRAINT raw_import_format_chk CHECK (payload_format IN ('JSON', 'CSV'))
);

CREATE INDEX raw_import_fetched_ix ON raw_import(fetched_at_utc);
CREATE INDEX raw_import_status_ix ON raw_import(status);


CREATE TABLE weather (
    weather_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    city_id               NUMBER NOT NULL,
    raw_id                NUMBER NOT NULL,

    observed_at_utc       TIMESTAMP NOT NULL,
    utc_offset_seconds    NUMBER NOT NULL,

    temperature_c     NUMBER,
    humidity_pct        NUMBER,
    precipitation_mm   NUMBER,
    wind_speed_kmh      NUMBER,

    CONSTRAINT weather_city_fk FOREIGN KEY (city_id) REFERENCES cities(city_id),
    CONSTRAINT weather_raw_fk  FOREIGN KEY (raw_id)  REFERENCES raw_import(raw_id),

    CONSTRAINT weather_uq UNIQUE (city_id, observed_at_utc),

    CONSTRAINT weather_offset_chk CHECK (utc_offset_seconds BETWEEN -18*3600 AND 18*3600),

    CONSTRAINT weather_temp_chk CHECK (temperature_c IS NULL OR temperature_c BETWEEN -80 AND 80),
    CONSTRAINT weather_hum_chk  CHECK (humidity_pct IS NULL OR humidity_pct BETWEEN 0 AND 100),
    CONSTRAINT weather_prec_chk CHECK (precipitation_mm IS NULL OR precipitation_mm >= 0),
    CONSTRAINT weather_wind_chk CHECK (wind_speed_kmh IS NULL OR wind_speed_kmh >= 0)
);

CREATE INDEX weather_city_time_ix ON weather(city_id, observed_at_utc);
CREATE INDEX weather_raw_ix ON weather(raw_id);


CREATE TABLE logs (
    log_id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    log_time      TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,

    log_level     VARCHAR2(10) DEFAULT 'INFO' NOT NULL,  -- zamiast LEVEL
    source        VARCHAR2(30) NOT NULL,
    action        VARCHAR2(50) NOT NULL,
    result        VARCHAR2(10) DEFAULT 'OK' NOT NULL,

    sqlcode       NUMBER,
    error_message VARCHAR2(4000),

    city_id       NUMBER,
    raw_id        NUMBER,
    weather_id    NUMBER,

    db_user       VARCHAR2(128) DEFAULT SYS_CONTEXT('USERENV','SESSION_USER') NOT NULL,
    app_user      VARCHAR2(128),
    run_id        VARCHAR2(64),
    details       CLOB,

    CONSTRAINT logs_level_chk  CHECK (log_level IN ('INFO','WARN','ERROR')),
    CONSTRAINT logs_result_chk CHECK (result IN ('OK','FAIL','SKIP')),

    CONSTRAINT logs_city_fk    FOREIGN KEY (city_id)    REFERENCES cities(city_id),
    CONSTRAINT logs_raw_fk     FOREIGN KEY (raw_id)     REFERENCES raw_import(raw_id),
    CONSTRAINT logs_weather_fk FOREIGN KEY (weather_id) REFERENCES weather(weather_id)
);

CREATE INDEX logs_time_ix    ON logs(log_time);
CREATE INDEX logs_action_ix  ON logs(action);
CREATE INDEX logs_level_ix   ON logs(log_level);
CREATE INDEX logs_run_id_ix  ON logs(run_id);
