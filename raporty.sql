drop table weather_reports;
/
CREATE TABLE weather_reports (
    report_id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    
    city_id          NUMBER NOT NULL,
    city_name        VARCHAR2(100),
    
    report_type      VARCHAR2(20) NOT NULL,
    start_date       DATE NOT NULL,
    end_date         DATE NOT NULL,
    
    avg_temperature  NUMBER(5,2),
    min_temperature  NUMBER(5,2),
    max_temperature  NUMBER(5,2),
    temp_volatility  NUMBER(5,2), 
    
    avg_wind         NUMBER(5,2),
    max_wind         NUMBER(5,2),
    days_strong_wind NUMBER,      
    
    avg_humidity     NUMBER(5,2),
    max_humidity     NUMBER(5,2),
    days_dry         NUMBER,        
    days_optimal     NUMBER,        
    days_humid       NUMBER,        
    
    record_count     NUMBER,
    created_at       TIMESTAMP DEFAULT SYSTIMESTAMP,
    
    CONSTRAINT fk_raport_city FOREIGN KEY (city_id) REFERENCES cities(city_id)
);


CREATE OR REPLACE PROCEDURE generuj_raporty (
    p_report_type IN VARCHAR2,
    p_target_date IN DATE DEFAULT SYSDATE,
    p_city_name   IN VARCHAR2 DEFAULT NULL
) IS
    v_start_date DATE;
    v_end_date   DATE;
    v_avg_temp   NUMBER;
    v_min_temp   NUMBER;
    v_max_temp   NUMBER;
    v_volatility NUMBER;
    v_avg_wind   NUMBER;
    v_max_wind   NUMBER;
    v_days_wind  NUMBER;
    v_days_dry   NUMBER;
    v_days_opt   NUMBER;
    v_days_hum   NUMBER;
    v_avg_hum    NUMBER;
    v_max_hum    NUMBER;
    v_count      NUMBER;
BEGIN
    IF UPPER(p_report_type) = 'WEEK' THEN
        v_start_date := TRUNC(p_target_date, 'IW');
        v_end_date   := TRUNC(p_target_date, 'IW') + 6.99999;
    ELSIF UPPER(p_report_type) = 'MONTH' THEN
        v_start_date := TRUNC(p_target_date, 'MM');
        v_end_date   := LAST_DAY(v_start_date) + 0.99999;
    ELSIF UPPER(p_report_type) = 'QUARTER' THEN
        v_start_date := TRUNC(p_target_date, 'Q');
        v_end_date   := ADD_MONTHS(v_start_date, 3) - 0.00001;
    ELSIF UPPER(p_report_type) = 'YEAR' THEN
        v_start_date := TRUNC(p_target_date, 'YYYY');
        v_end_date   := ADD_MONTHS(v_start_date, 12) - 0.00001;
    ELSE
        RAISE_APPLICATION_ERROR(-20001, 'Nieznany typ raportu');
    END IF;

    DELETE FROM weather_reports 
    WHERE report_type = UPPER(p_report_type) 
      AND start_date = v_start_date
      AND (p_city_name IS NULL OR city_name = p_city_name);

    FOR c IN (
        SELECT city_id, city_name 
        FROM cities
        WHERE p_city_name IS NULL OR city_name = p_city_name
    ) LOOP
        v_avg_temp   := funkcja_srednia_temp(c.city_name, v_start_date, v_end_date);
        v_max_temp   := funkcja_max_temp(c.city_name, v_start_date, v_end_date);
        v_min_temp   := funkcja_min_temp(c.city_name, v_start_date, v_end_date);
        v_volatility := funkcja_odchylenie_temp(c.city_name, v_start_date, v_end_date);
        v_avg_wind   := funkcja_avg_wind(c.city_name, v_start_date, v_end_date);
        v_max_wind   := funkcja_max_wind(c.city_name, v_start_date, v_end_date);
        v_days_wind  := funkcja_dni_silny_wiatr(c.city_name, v_start_date, v_end_date);
        v_days_dry   := funkcja_dni_sucho(c.city_name, v_start_date, v_end_date);
        v_days_opt   := funkcja_dni_optimalnie(c.city_name, v_start_date, v_end_date);
        v_days_hum   := funkcja_dni_wilgotno(c.city_name, v_start_date, v_end_date);
        v_avg_hum    := funkcja_srednia_wilgotnosc(c.city_name, v_start_date, v_end_date);
        v_max_hum    := funkcja_max_wilgotnosc(c.city_name, v_start_date, v_end_date);
        v_count      := funkcja_liczba_rekordow(c.city_name, v_start_date, v_end_date);

        IF v_count > 0 THEN
            INSERT INTO weather_reports (
                city_id, city_name, 
                report_type, start_date, end_date,
                avg_temperature, min_temperature, max_temperature, temp_volatility,
                avg_wind, max_wind, days_strong_wind,
                avg_humidity, max_humidity, days_dry, days_optimal, days_humid,
                record_count
            ) VALUES (
                c.city_id, c.city_name, 
                UPPER(p_report_type), v_start_date, v_end_date,
                v_avg_temp, v_min_temp, v_max_temp, v_volatility,
                v_avg_wind, v_max_wind, v_days_wind,
                v_avg_hum, v_max_hum, v_days_dry, v_days_opt, v_days_hum,
                v_count
            );
        END IF;
    END LOOP;
    
    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Raport wygenerowany: ' || p_report_type);
END;
/

CREATE OR REPLACE TRIGGER trg_raporty_tylko_odczyt
BEFORE UPDATE ON weather_reports
FOR EACH ROW
DECLARE

    PRAGMA AUTONOMOUS_TRANSACTION; 
BEGIN

    IF :NEW.avg_temperature != :OLD.avg_temperature 
       OR :NEW.max_wind != :OLD.max_wind 
       OR :NEW.days_dry != :OLD.days_dry THEN
       
        INSERT INTO logs (
            log_level, 
            source, 
            action, 
            result, 
            city_id, 
            error_message, 
            details,
            db_user
        ) VALUES (
            'ERROR',                  
            'TRIGGER_SECURITY',       
            'UPDATE_ATTEMPT',         
            'FAIL',                   
            :OLD.city_id,             
            'Próba ręcznej modyfikacji danych analitycznych!',
            'Stara średnia temp: ' || :OLD.avg_temperature || 
            ', Próba zmiany na: ' || :NEW.avg_temperature,   
            USER                      
        );
        
        COMMIT; 

        RAISE_APPLICATION_ERROR(-20005, 'BŁĄD: Raporty są tylko do odczytu! Nie wolno ich edytować ręcznie.');
    END IF;
END;
/

CREATE OR REPLACE VIEW v_raporty_miesieczne AS
SELECT 
    TO_CHAR(start_date, 'YYYY-MM') AS "OKRES",
    
    city_name        AS "MIASTO",
    avg_temperature  AS "SREDNIA_TEMP",
    min_temperature  AS "MIN_TEMP",
    max_temperature  AS "MAX_TEMP",
    temp_volatility  AS "STABILNOSC_TEMP",
    avg_wind         AS "SREDNI_WIATR",
    max_wind         AS "MAX_WIATR",
    days_strong_wind AS "DNI_WIETRZNE",
    avg_humidity     AS "SREDNIA_WILGOTNOSC",
    max_humidity     AS "MAX_WILGOTNOSC",
    days_dry         AS "DNI_SUCHE",
    days_optimal     AS "DNI_OPTIMALNE",
    days_humid       AS "DNI_WILGOTNE"
FROM weather_reports
WHERE report_type = 'MONTH'
ORDER BY start_date DESC, city_name ASC;


CREATE OR REPLACE VIEW v_raporty_kwartalne AS
SELECT 
    TO_CHAR(start_date, 'YYYY') || ' Kwartał ' || TO_CHAR(start_date, 'Q') AS "OKRES",
    city_name        AS "MIASTO",
    avg_temperature  AS "SREDNIA_TEMP",
    min_temperature  AS "MIN_TEMP",
    max_temperature  AS "MAX_TEMP",
    temp_volatility  AS "STABILNOSC_TEMP",
    avg_wind         AS "SREDNI_WIATR",
    max_wind         AS "MAX_WIATR",
    days_strong_wind AS "DNI_WIETRZNE",
    avg_humidity     AS "SREDNIA_WILGOTNOSC",
    max_humidity     AS "MAX_WILGOTNOSC",
    days_dry         AS "DNI_SUCHE",
    days_optimal     AS "DNI_OPTIMALNE",
    days_humid       AS "DNI_WILGOTNE"
FROM weather_reports
WHERE report_type = 'QUARTER'
ORDER BY start_date DESC, city_name ASC;


CREATE OR REPLACE VIEW v_raporty_roczne AS
SELECT 

    TO_CHAR(start_date, 'YYYY') AS "ROK",
    city_name        AS "MIASTO",
    avg_temperature  AS "SREDNIA_TEMP",
    min_temperature  AS "MIN_TEMP",
    max_temperature  AS "MAX_TEMP",
    temp_volatility  AS "STABILNOSC_TEMP",
    avg_wind         AS "SREDNI_WIATR",
    max_wind         AS "MAX_WIATR",
    days_strong_wind AS "DNI_WIETRZNE",
    avg_humidity     AS "SREDNIA_WILGOTNOSC",
    max_humidity     AS "MAX_WILGOTNOSC",
    days_dry         AS "DNI_SUCHE",
    days_optimal     AS "DNI_OPTIMALNE",
    days_humid       AS "DNI_WILGOTNE"
FROM weather_reports
WHERE report_type = 'YEAR'
ORDER BY start_date DESC, city_name ASC;

