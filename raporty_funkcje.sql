
CREATE OR REPLACE FUNCTION funkcja_srednia_temp (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_avg_temp NUMBER;
BEGIN
    SELECT AVG(temperature_c)
    INTO v_avg_temp
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end;

    RETURN ROUND(v_avg_temp, 2);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_max_temp (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_max_temp NUMBER;
BEGIN
    SELECT MAX(temperature_c)
    INTO v_max_temp
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN v_max_temp;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_min_temp (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_min_temp NUMBER;
BEGIN
    SELECT MIN(temperature_c)
    INTO v_min_temp
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN v_min_temp;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_avg_wind (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_avg_wind NUMBER;
BEGIN
    SELECT AVG(wind_speed_kmh)
    INTO v_avg_wind
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN ROUND(v_avg_wind,2);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_max_wind (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_max_wind NUMBER;
BEGIN
    SELECT MAX(wind_speed_kmh)
    INTO v_max_wind
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN v_max_wind;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_odchylenie_temp (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_stddev NUMBER;
BEGIN
    SELECT STDDEV(temperature_c)
    INTO v_stddev
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN ROUND(v_stddev,2);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_dni_silny_wiatr (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE,
    p_limit_speed IN NUMBER DEFAULT 30
) RETURN NUMBER IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(DISTINCT TRUNC(observed_at_utc))
    INTO v_count
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end
      AND wind_speed_kmh >= p_limit_speed;
    RETURN v_count;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END;
/

CREATE OR REPLACE FUNCTION funkcja_dni_sucho (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(DISTINCT TRUNC(observed_at_utc))
    INTO v_count
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end
      AND humidity_pct < 40;
    RETURN v_count;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END;
/

CREATE OR REPLACE FUNCTION funkcja_dni_optimalnie (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(DISTINCT TRUNC(observed_at_utc))
    INTO v_count
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end
      AND humidity_pct BETWEEN 40 AND 70;
    RETURN v_count;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END;
/

CREATE OR REPLACE FUNCTION funkcja_dni_wilgotno (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(DISTINCT TRUNC(observed_at_utc))
    INTO v_count
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND w.observed_at_utc BETWEEN p_start AND p_end
      AND humidity_pct > 70;
    RETURN v_count;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END;
/

CREATE OR REPLACE FUNCTION funkcja_srednia_wilgotnosc (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_res NUMBER;
BEGIN
    SELECT AVG(humidity_pct) INTO v_res
    FROM weather w JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN ROUND(v_res, 2);
EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_max_wilgotnosc (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_res NUMBER;
BEGIN
    SELECT MAX(humidity_pct) INTO v_res
    FROM weather w JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN v_res;
EXCEPTION WHEN NO_DATA_FOUND THEN RETURN NULL;
END;
/

CREATE OR REPLACE FUNCTION funkcja_liczba_rekordow (
    p_miasto IN VARCHAR2,
    p_start  IN DATE,
    p_end    IN DATE
) RETURN NUMBER IS
    v_res NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_res
    FROM weather w JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto AND w.observed_at_utc BETWEEN p_start AND p_end;
    RETURN v_res;
EXCEPTION WHEN NO_DATA_FOUND THEN RETURN 0;
END;
/