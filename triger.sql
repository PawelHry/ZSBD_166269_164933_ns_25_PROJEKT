CREATE OR REPLACE TRIGGER trg_weather_bdel_archive
BEFORE DELETE ON weather
FOR EACH ROW
BEGIN
    INSERT INTO weather_archive (
        weather_id, city_id, raw_id,
        observed_at_utc, utc_offset_seconds,
        temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh
    ) VALUES (
        :OLD.weather_id, :OLD.city_id, :OLD.raw_id,
        :OLD.observed_at_utc, :OLD.utc_offset_seconds,
        :OLD.temperature_c, :OLD.humidity_pct, :OLD.precipitation_mm, :OLD.wind_speed_kmh
    );
END;
/


CREATE OR REPLACE TRIGGER trg_cities_bdel_archive
BEFORE DELETE ON cities
FOR EACH ROW
BEGIN
    INSERT INTO cities_archive (
        city_id, city_name, country_code, latitude, longitude, timezone_name, is_active
    ) VALUES (
        :OLD.city_id, :OLD.city_name, :OLD.country_code, :OLD.latitude, :OLD.longitude, :OLD.timezone_name, :OLD.is_active
    );

    INSERT INTO logs (log_level, source, action, result, city_id, details)
    VALUES ('INFO','TRG','CITY_DELETE','OK', :OLD.city_id, 'city archived to cities_archive; weather will be deleted by cascade');
END;
/


CREATE OR REPLACE TRIGGER trg_cities_aiu_log
AFTER INSERT OR UPDATE ON cities
FOR EACH ROW
DECLARE
  v_action  VARCHAR2(50);
  v_details CLOB;
  v_changed NUMBER(1) := 0;
BEGIN
  IF INSERTING THEN
    v_action  := 'CITY_INSERT';
    v_details := 'name=' || :NEW.city_name
              || '; country=' || :NEW.country_code
              || '; lat=' || TO_CHAR(:NEW.latitude)
              || '; lon=' || TO_CHAR(:NEW.longitude)
              || '; tz=' || :NEW.timezone_name
              || '; is_active=' || TO_CHAR(:NEW.is_active);

  ELSIF UPDATING THEN
    -- czy coś faktycznie się zmieniło (żeby nie spamować logów)
    IF NVL(:OLD.city_name,'~')      <> NVL(:NEW.city_name,'~')
    OR NVL(:OLD.country_code,'~')   <> NVL(:NEW.country_code,'~')
    OR NVL(:OLD.timezone_name,'~')  <> NVL(:NEW.timezone_name,'~')
    OR NVL(:OLD.is_active,-1)       <> NVL(:NEW.is_active,-1)
    OR NVL(:OLD.latitude,  -999999) <> NVL(:NEW.latitude,  -999999)
    OR NVL(:OLD.longitude, -999999) <> NVL(:NEW.longitude, -999999)
    THEN
      v_changed := 1;
    END IF;

    IF v_changed = 0 THEN
      RETURN;
    END IF;

    IF NVL(:OLD.is_active,-1) <> NVL(:NEW.is_active,-1) THEN
      IF :NEW.is_active = 1 THEN
        v_action := 'CITY_ACTIVATE';
      ELSE
        v_action := 'CITY_DEACTIVATE';
      END IF;

      v_details := 'is_active ' || TO_CHAR(:OLD.is_active) || ' -> ' || TO_CHAR(:NEW.is_active);

    ELSE
      v_action := 'CITY_UPDATE';
      v_details := 'name=' || :NEW.city_name
                || '; country=' || :NEW.country_code
                || '; lat=' || TO_CHAR(:NEW.latitude)
                || '; lon=' || TO_CHAR(:NEW.longitude)
                || '; tz=' || :NEW.timezone_name;
    END IF;
  END IF;

  INSERT INTO logs (log_level, source, action, result, city_id, app_user, details)
  VALUES (
    'INFO',
    'TRG',
    v_action,
    'OK',
    :NEW.city_id,
    SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER'),
    v_details
  );
END;
/



CREATE OR REPLACE TRIGGER trg_units_aiu_log
AFTER INSERT OR UPDATE ON units
FOR EACH ROW
DECLARE
  v_action  VARCHAR2(50);
  v_details CLOB;
  v_changed NUMBER(1) := 0;
BEGIN
  IF INSERTING THEN
    v_action  := 'UNIT_INSERT';
    v_details := 'dimension=' || :NEW.dimension
              || '; symbol=' || :NEW.unit_symbol
              || '; name=' || NVL(:NEW.unit_name,'(null)')
              || '; mul=' || NVL(TO_CHAR(:NEW.to_canonical_mul),'(null)')
              || '; add=' || NVL(TO_CHAR(:NEW.to_canonical_add),'(null)')
              || '; canonical_unit_id=' || NVL(TO_CHAR(:NEW.canonical_unit_id),'(null)');

  ELSIF UPDATING THEN
    IF NVL(:OLD.dimension,'~') <> NVL(:NEW.dimension,'~')
    OR NVL(:OLD.unit_symbol,'~') <> NVL(:NEW.unit_symbol,'~')
    OR NVL(:OLD.unit_name,'~') <> NVL(:NEW.unit_name,'~')
    OR NVL(:OLD.to_canonical_mul, -999999) <> NVL(:NEW.to_canonical_mul, -999999)
    OR NVL(:OLD.to_canonical_add, -999999) <> NVL(:NEW.to_canonical_add, -999999)
    OR NVL(:OLD.canonical_unit_id, -1) <> NVL(:NEW.canonical_unit_id, -1)
    THEN
      v_changed := 1;
    END IF;

    IF v_changed = 0 THEN
      RETURN;
    END IF;

    v_action  := 'UNIT_UPDATE';
    v_details := 'dimension=' || :NEW.dimension
              || '; symbol=' || :NEW.unit_symbol
              || '; name=' || NVL(:NEW.unit_name,'(null)')
              || '; mul=' || NVL(TO_CHAR(:NEW.to_canonical_mul),'(null)')
              || '; add=' || NVL(TO_CHAR(:NEW.to_canonical_add),'(null)')
              || '; canonical_unit_id=' || NVL(TO_CHAR(:NEW.canonical_unit_id),'(null)');
  END IF;

  INSERT INTO logs (log_level, source, action, result, app_user, details)
  VALUES (
    'INFO',
    'TRG',
    v_action,
    'OK',
    SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER'),
    v_details
  );
END;
/
