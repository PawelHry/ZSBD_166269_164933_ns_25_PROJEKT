

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

