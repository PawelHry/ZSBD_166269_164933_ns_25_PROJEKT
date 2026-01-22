SET SERVEROUTPUT ON;

DECLARE
    v_raw_id  NUMBER;
    v_date    DATE;
    v_temp    NUMBER;
    v_month   NUMBER;
BEGIN

    SELECT NVL(MIN(raw_id), 1) INTO v_raw_id FROM raw_import;

    DBMS_OUTPUT.PUT_LINE('=== START GENEROWANIA DANYCH HISTORYCZNYCH ===');


    FOR c IN (SELECT city_id, city_name FROM cities WHERE city_id BETWEEN 2 AND 12) LOOP
        
        DBMS_OUTPUT.PUT_LINE('Generowanie danych dla miasta: ' || c.city_name || ' (ID: ' || c.city_id || ')...');

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
        END LOOP; 

    END LOOP; 

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('=== SUKCES! Baza zapełniona danymi z ostatniego roku dla miast 2-12 ===');
END;
/