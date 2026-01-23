SET SERVEROUTPUT ON;

DECLARE
    v_raw_id  NUMBER;
    v_date    DATE;
    v_temp    NUMBER;
    v_month   NUMBER;
    v_count   NUMBER;
BEGIN

    BEGIN
        SELECT raw_id INTO v_raw_id 
        FROM raw_import 
        FETCH FIRST 1 ROW ONLY;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            INSERT INTO raw_import (source, payload, status) 
            VALUES ('generator_danych', '{"info": "dummy_data"}', 'OK')
            RETURNING raw_id INTO v_raw_id;
            DBMS_OUTPUT.PUT_LINE('Utworzono techniczny rekord w raw_import (ID: ' || v_raw_id || ')');
    END;


    FOR c IN (SELECT city_id, city_name FROM cities) LOOP
        
        DBMS_OUTPUT.PUT_LINE('Przetwarzanie miasta: ' || c.city_name || ' (ID: ' || c.city_id || ')...');

        FOR i IN 0..24 LOOP
            
            v_date := SYSDATE - (i * 15);
            v_month := TO_NUMBER(TO_CHAR(v_date, 'MM'));

            IF v_month IN (12, 1, 2) THEN
                v_temp := ROUND(DBMS_RANDOM.VALUE(-10, 5), 1); 
            ELSIF v_month IN (6, 7, 8) THEN
                v_temp := ROUND(DBMS_RANDOM.VALUE(20, 32), 1); 
            ELSE
                v_temp := ROUND(DBMS_RANDOM.VALUE(5, 18), 1);  
            END IF;

            BEGIN
                INSERT INTO weather (
                    city_id, raw_id, observed_at_utc, utc_offset_seconds, 
                    temperature_c, wind_speed_kmh, humidity_pct, precipitation_mm
                ) VALUES (
                    c.city_id,     
                    v_raw_id, 
                    v_date, 
                    3600, 
                    v_temp, 
                    ROUND(DBMS_RANDOM.VALUE(0, 40), 1),   
                    ROUND(DBMS_RANDOM.VALUE(40, 95), 0),  
                    ROUND(DBMS_RANDOM.VALUE(0, 5), 1)     
                );
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN

                    NULL; 
            END;

        END LOOP; 

    END LOOP; 

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Dane wygenerowane dla wszystkich dostępnych miast.');
END;
/