CREATE OR REPLACE PACKAGE pkg_cities AS
  e_city_not_found      EXCEPTION;
  e_invalid_input       EXCEPTION;
  e_no_changes          EXCEPTION;
  e_duplicate_location  EXCEPTION;

  PROCEDURE city_add(
    p_city_name     IN  cities.city_name%TYPE,
    p_country_code  IN  cities.country_code%TYPE  DEFAULT 'PL',
    p_latitude      IN  cities.latitude%TYPE,
    p_longitude     IN  cities.longitude%TYPE,
    p_timezone_name IN  cities.timezone_name%TYPE DEFAULT 'Europe/Warsaw',
    p_is_active     IN  cities.is_active%TYPE     DEFAULT 1,
    p_city_id       OUT cities.city_id%TYPE
  );

  PROCEDURE city_update(
    p_city_id       IN cities.city_id%TYPE,
    p_city_name     IN cities.city_name%TYPE     DEFAULT NULL,
    p_country_code  IN cities.country_code%TYPE  DEFAULT NULL,
    p_latitude      IN cities.latitude%TYPE      DEFAULT NULL,
    p_longitude     IN cities.longitude%TYPE     DEFAULT NULL,
    p_timezone_name IN cities.timezone_name%TYPE DEFAULT NULL
  );

  PROCEDURE city_set_active(
    p_city_id    IN cities.city_id%TYPE,
    p_is_active  IN cities.is_active%TYPE
  );

  PROCEDURE city_delete(
    p_city_id IN cities.city_id%TYPE,
    p_reason  IN VARCHAR2 DEFAULT NULL
  );
END pkg_cities;
/
SHOW ERRORS;


CREATE OR REPLACE PACKAGE BODY pkg_cities AS

  PROCEDURE log_fail(
    p_action        IN VARCHAR2,
    p_city_id       IN NUMBER,
    p_error_message IN VARCHAR2,
    p_sqlcode       IN NUMBER,
    p_details       IN CLOB DEFAULT NULL
  ) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    INSERT INTO logs (
      log_level, source, action, result,
      sqlcode, error_message,
      city_id,
      app_user,
      details
    ) VALUES (
      'ERROR', 'PKG', p_action, 'FAIL',
      p_sqlcode, SUBSTR(p_error_message,1,4000),
      p_city_id,
      SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER'),
      p_details
    );

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
  END;

  PROCEDURE validate_city(
    p_city_name     IN VARCHAR2,
    p_country_code  IN VARCHAR2,
    p_latitude      IN NUMBER,
    p_longitude     IN NUMBER,
    p_timezone_name IN VARCHAR2,
    p_is_active     IN NUMBER
  ) IS
  BEGIN
    IF p_city_name IS NULL OR TRIM(p_city_name) IS NULL THEN
      RAISE e_invalid_input;
    END IF;

    IF p_country_code IS NULL OR LENGTH(TRIM(p_country_code)) <> 2 THEN
      RAISE e_invalid_input;
    END IF;

    IF p_latitude IS NULL OR p_latitude < -90 OR p_latitude > 90 THEN
      RAISE e_invalid_input;
    END IF;

    IF p_longitude IS NULL OR p_longitude < -180 OR p_longitude > 180 THEN
      RAISE e_invalid_input;
    END IF;

    IF p_timezone_name IS NULL OR TRIM(p_timezone_name) IS NULL THEN
      RAISE e_invalid_input;
    END IF;

    IF p_is_active NOT IN (0,1) THEN
      RAISE e_invalid_input;
    END IF;
  END;


  PROCEDURE city_add(
    p_city_name     IN  cities.city_name%TYPE,
    p_country_code  IN  cities.country_code%TYPE,
    p_latitude      IN  cities.latitude%TYPE,
    p_longitude     IN  cities.longitude%TYPE,
    p_timezone_name IN  cities.timezone_name%TYPE,
    p_is_active     IN  cities.is_active%TYPE,
    p_city_id       OUT cities.city_id%TYPE
  ) IS
    v_city_id  cities.city_id%TYPE;
    v_name     cities.city_name%TYPE;
    v_cc       cities.country_code%TYPE;
    v_tz       cities.timezone_name%TYPE;
  BEGIN
    v_name := TRIM(p_city_name);
    v_cc   := UPPER(TRIM(NVL(p_country_code,'PL')));
    v_tz   := TRIM(NVL(p_timezone_name,'Europe/Warsaw'));

    validate_city(v_name, v_cc, p_latitude, p_longitude, v_tz, NVL(p_is_active,1));

    INSERT INTO cities (city_name, country_code, latitude, longitude, timezone_name, is_active)
    VALUES (v_name, v_cc, p_latitude, p_longitude, v_tz, NVL(p_is_active,1))
    RETURNING city_id INTO v_city_id;

    p_city_id := v_city_id;

    COMMIT;

  EXCEPTION
    WHEN e_invalid_input THEN
      log_fail('CITY_ADD', NULL, 'Niepoprawne dane wejściowe', -20010,
               'name/country/lat/lon/tz/is_active validation failed');
      RAISE_APPLICATION_ERROR(-20010, 'CITY_ADD: niepoprawne dane wejściowe');

    WHEN DUP_VAL_ON_INDEX THEN
      log_fail('CITY_ADD', NULL, SQLERRM, SQLCODE, 'Prawdopodobnie duplikat (latitude, longitude)');
      RAISE_APPLICATION_ERROR(-20011, 'CITY_ADD: istnieje już miasto o tych współrzędnych');

    WHEN OTHERS THEN
      log_fail('CITY_ADD', NULL, SQLERRM, SQLCODE, NULL);
      RAISE;
  END;


  PROCEDURE city_update(
    p_city_id       IN cities.city_id%TYPE,
    p_city_name     IN cities.city_name%TYPE,
    p_country_code  IN cities.country_code%TYPE,
    p_latitude      IN cities.latitude%TYPE,
    p_longitude     IN cities.longitude%TYPE,
    p_timezone_name IN cities.timezone_name%TYPE
  ) IS
    v_exists NUMBER;
    v_name   cities.city_name%TYPE;
    v_cc     cities.country_code%TYPE;
    v_tz     cities.timezone_name%TYPE;
  BEGIN
    IF p_city_id IS NULL THEN
      RAISE e_invalid_input;
    END IF;

    SELECT 1 INTO v_exists
    FROM cities
    WHERE city_id = p_city_id;

    IF p_city_name IS NULL
       AND p_country_code IS NULL
       AND p_latitude IS NULL
       AND p_longitude IS NULL
       AND p_timezone_name IS NULL
    THEN
      RAISE e_no_changes;
    END IF;

    v_name := CASE WHEN p_city_name IS NULL THEN NULL ELSE TRIM(p_city_name) END;
    v_cc   := CASE WHEN p_country_code IS NULL THEN NULL ELSE UPPER(TRIM(p_country_code)) END;
    v_tz   := CASE WHEN p_timezone_name IS NULL THEN NULL ELSE TRIM(p_timezone_name) END;

    IF v_name IS NOT NULL AND v_name = '' THEN RAISE e_invalid_input; END IF;
    IF v_cc   IS NOT NULL AND LENGTH(v_cc) <> 2 THEN RAISE e_invalid_input; END IF;
    IF p_latitude  IS NOT NULL AND (p_latitude < -90 OR p_latitude > 90) THEN RAISE e_invalid_input; END IF;
    IF p_longitude IS NOT NULL AND (p_longitude < -180 OR p_longitude > 180) THEN RAISE e_invalid_input; END IF;
    IF v_tz IS NOT NULL AND v_tz = '' THEN RAISE e_invalid_input; END IF;

    UPDATE cities
    SET city_name     = NVL(v_name, city_name),
        country_code  = NVL(v_cc, country_code),
        latitude      = NVL(p_latitude, latitude),
        longitude     = NVL(p_longitude, longitude),
        timezone_name = NVL(v_tz, timezone_name)
    WHERE city_id = p_city_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE e_city_not_found;
    END IF;

    COMMIT;

  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      log_fail('CITY_UPDATE', p_city_id, 'Miasto nie istnieje', -20001, NULL);
      RAISE_APPLICATION_ERROR(-20001, 'CITY_UPDATE: miasto nie istnieje');

    WHEN e_no_changes THEN
      log_fail('CITY_UPDATE', p_city_id, 'Brak pól do zmiany', -20012, NULL);
      RAISE_APPLICATION_ERROR(-20012, 'CITY_UPDATE: nie podano żadnych pól do zmiany');

    WHEN e_invalid_input THEN
      log_fail('CITY_UPDATE', p_city_id, 'Niepoprawne dane wejściowe', -20010, NULL);
      RAISE_APPLICATION_ERROR(-20010, 'CITY_UPDATE: niepoprawne dane wejściowe');

    WHEN DUP_VAL_ON_INDEX THEN
      log_fail('CITY_UPDATE', p_city_id, SQLERRM, SQLCODE, 'Prawdopodobnie duplikat (latitude, longitude)');
      RAISE_APPLICATION_ERROR(-20011, 'CITY_UPDATE: istnieje już miasto o tych współrzędnych');

    WHEN OTHERS THEN
      log_fail('CITY_UPDATE', p_city_id, SQLERRM, SQLCODE, NULL);
      RAISE;
  END;


  PROCEDURE city_set_active(
    p_city_id    IN cities.city_id%TYPE,
    p_is_active  IN cities.is_active%TYPE
  ) IS
  BEGIN
    IF p_city_id IS NULL OR p_is_active NOT IN (0,1) THEN
      RAISE e_invalid_input;
    END IF;

    UPDATE cities
    SET is_active = p_is_active
    WHERE city_id = p_city_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE e_city_not_found;
    END IF;

    COMMIT;

  EXCEPTION
    WHEN e_invalid_input THEN
      log_fail('CITY_SET_ACTIVE', p_city_id, 'Niepoprawne dane wejściowe', -20010,
               'is_active musi być 0 albo 1');
      RAISE_APPLICATION_ERROR(-20010, 'CITY_SET_ACTIVE: niepoprawne dane wejściowe');

    WHEN e_city_not_found THEN
      log_fail('CITY_SET_ACTIVE', p_city_id, 'Miasto nie istnieje', -20001, NULL);
      RAISE_APPLICATION_ERROR(-20001, 'CITY_SET_ACTIVE: miasto nie istnieje');

    WHEN OTHERS THEN
      log_fail('CITY_SET_ACTIVE', p_city_id, SQLERRM, SQLCODE, NULL);
      RAISE;
  END;


  PROCEDURE city_delete(
    p_city_id IN cities.city_id%TYPE,
    p_reason  IN VARCHAR2
  ) IS
  BEGIN
    IF p_city_id IS NULL THEN
      RAISE e_invalid_input;
    END IF;

    IF p_reason IS NOT NULL THEN
      INSERT INTO logs (log_level, source, action, result, city_id, app_user, details)
      VALUES ('INFO','PKG','CITY_DELETE_REQUEST','OK', p_city_id, SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER'), p_reason);
    END IF;

    DELETE FROM cities
    WHERE city_id = p_city_id;

    IF SQL%ROWCOUNT = 0 THEN
      RAISE e_city_not_found;
    END IF;

    COMMIT;

  EXCEPTION
    WHEN e_invalid_input THEN
      log_fail('CITY_DELETE', p_city_id, 'Niepoprawne dane wejściowe', -20010, NULL);
      RAISE_APPLICATION_ERROR(-20010, 'CITY_DELETE: niepoprawne dane wejściowe');

    WHEN e_city_not_found THEN
      log_fail('CITY_DELETE', p_city_id, 'Miasto nie istnieje', -20001, NULL);
      RAISE_APPLICATION_ERROR(-20001, 'CITY_DELETE: miasto nie istnieje');

    WHEN OTHERS THEN
      log_fail('CITY_DELETE', p_city_id, SQLERRM, SQLCODE, NULL);
      RAISE;
  END;

END pkg_cities;
/
SHOW ERRORS;
