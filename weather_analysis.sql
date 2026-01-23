CREATE OR REPLACE FUNCTION konwertuj_temperature (
    p_celsius     IN NUMBER,
    jednostka_docelowa IN VARCHAR2 DEFAULT 'F' 
) RETURN NUMBER IS
BEGIN

    IF UPPER(jednostka_docelowa) = 'F' THEN
        RETURN ROUND((p_celsius * 1.8) + 32, 1);
        
    ELSIF UPPER(jednostka_docelowa) = 'K' THEN
        RETURN ROUND(p_celsius + 273.15, 1);
        
    ELSE

        RETURN p_celsius;
    END IF;
END;
/



CREATE OR REPLACE PROCEDURE prognozuj_temperature (
    p_city_name       IN VARCHAR2 DEFAULT 'Warszawa',
    p_jednostka       IN VARCHAR2 DEFAULT 'C'
) IS
    v_last_temp      NUMBER;
    v_last_time      TIMESTAMP;
    v_trend_per_hour NUMBER;
    v_hours_gap      NUMBER;
    v_predicted_now  NUMBER;
    v_city_id        NUMBER;
    
    v_hist_avg       NUMBER;
    v_hist_count     NUMBER;
    v_method         VARCHAR2(50); 
BEGIN

    SELECT city_id INTO v_city_id 
    FROM cities 
    WHERE city_name = p_city_name;

    SELECT 
        MAX(temperature_c) KEEP (DENSE_RANK LAST ORDER BY observed_at_utc),
        MAX(observed_at_utc),
        AVG(trend_val)
    INTO v_last_temp, v_last_time, v_trend_per_hour
    FROM (
        SELECT 
            observed_at_utc, temperature_c,
            (temperature_c - LAG(temperature_c) OVER (ORDER BY observed_at_utc)) /
            (GREATEST(0.1, (CAST(observed_at_utc AS DATE) - CAST(LAG(observed_at_utc) OVER (ORDER BY observed_at_utc) AS DATE)) * 24)) 
            AS trend_val
        FROM weather 
        WHERE city_id = v_city_id
    )
    WHERE trend_val IS NOT NULL FETCH FIRST 5 ROWS ONLY;

    v_hours_gap := (CAST(SYSTIMESTAMP AS DATE) - CAST(v_last_time AS DATE)) * 24;

    IF v_hours_gap <= 3 THEN
        v_predicted_now := v_last_temp + (NVL(v_trend_per_hour, 0) * v_hours_gap);
        v_method := 'KRÓTKI TREND (Ostatnie zmiany)';
    ELSE
        SELECT AVG(temperature_c), COUNT(*)
        INTO v_hist_avg, v_hist_count
        FROM weather
        WHERE city_id = v_city_id
          AND TO_CHAR(observed_at_utc, 'HH24') = TO_CHAR(SYSTIMESTAMP, 'HH24')
          AND observed_at_utc < SYSDATE - 0.5;

        IF v_hist_count > 0 THEN
            v_predicted_now := v_hist_avg;
            v_method := 'HISTORIA (Średnia z tej samej godziny)';
        ELSE
            v_predicted_now := v_last_temp + (NVL(v_trend_per_hour, 0) * 3);
            v_method := 'AWARYJNIE (Wygaszony trend)';
        END IF;
    END IF;

    v_predicted_now := konwertuj_temperature(v_predicted_now, p_jednostka);

    DBMS_OUTPUT.PUT_LINE('PROGNOZA DLA MIASTA: ' || UPPER(p_city_name));
    DBMS_OUTPUT.PUT_LINE('Metoda prognozy:    ' || v_method);
    DBMS_OUTPUT.PUT_LINE('Czas obecny:        ' || TO_CHAR(SYSDATE, 'HH24:MI'));
    DBMS_OUTPUT.PUT_LINE('Ostatni pomiar:     ' || TO_CHAR(v_last_time, 'DD.MM HH24:MI') || ' (' || konwertuj_temperature(v_last_temp, p_jednostka) || ' ' || p_jednostka || ')');
    DBMS_OUTPUT.PUT_LINE('Przerwa w danych:   ' || ROUND(v_hours_gap, 1) || ' h');
    IF v_method LIKE 'HISTORIA%' THEN
        DBMS_OUTPUT.PUT_LINE('Znaleziono wpisów historycznych o tej porze: ' || v_hist_count);
    END IF;
    DBMS_OUTPUT.PUT_LINE('SZACOWANA TEMP. TERAZ: ' || ROUND(v_predicted_now, 1) || ' ' || p_jednostka);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Brak danych dla miasta.');
END;


/

CREATE OR REPLACE PROCEDURE ranking_zmiennosci (
    p_limit    IN NUMBER DEFAULT 3,
    p_days_back IN NUMBER DEFAULT 7
) IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('RANKING MIAST: NAJWIĘKSZA AMPLITUDA TEMPERATUR');
    DBMS_OUTPUT.PUT_LINE('Okres: ostatnie ' || p_days_back || ' dni');
    DBMS_OUTPUT.PUT_LINE('MIEJSCE | MIASTO           | RÓŻNICA (MAX - MIN)');
    DBMS_OUTPUT.PUT_LINE('');

    FOR r IN (
        SELECT * FROM (
            SELECT 
                c.city_name,
                MAX(w.temperature_c) - MIN(w.temperature_c) AS amplituda,
                DENSE_RANK() OVER (ORDER BY (MAX(w.temperature_c) - MIN(w.temperature_c)) DESC) AS ranking
            FROM weather w
            JOIN cities c ON w.city_id = c.city_id
            WHERE w.observed_at_utc >= SYSDATE - p_days_back
            GROUP BY c.city_name
        )
        WHERE ranking <= p_limit
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(
            RPAD(r.ranking, 7) || ' | ' || 
            RPAD(r.city_name, 16) || ' | ' || 
            r.amplituda || ' st. C'
        );
    END LOOP;

END;
/



CREATE OR REPLACE PROCEDURE monitoruj_wiatr (
    p_miasto       IN VARCHAR2 DEFAULT 'Warszawa',
    p_ostatnie_dni IN NUMBER   DEFAULT 1,
    p_min_predkosc IN NUMBER   DEFAULT 5 
) IS
    v_liczba_pomiarow NUMBER := 0;
    v_roznica NUMBER;
    v_czy_sa_dane NUMBER;
BEGIN

    SELECT COUNT(*)
    INTO v_czy_sa_dane
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
    AND w.observed_at_utc >= SYSDATE - p_ostatnie_dni;
    
    IF v_czy_sa_dane = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Brak danych dla miasta: ' || p_miasto);
        RETURN;
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('ANALIZA WIATRU');
    DBMS_OUTPUT.PUT_LINE('Miasto:  ' || UPPER(p_miasto));
    DBMS_OUTPUT.PUT_LINE('Zakres:  Ostatnie ' || p_ostatnie_dni || ' dni');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('DATA GODZINA  | WIATR (km/h) | REKORD OKRESU | STATUS');

    FOR r IN (
        SELECT * FROM (
            SELECT 
                observed_at_utc,
                wind_speed_kmh,
                MAX(wind_speed_kmh) OVER (ORDER BY observed_at_utc) AS max_dotychczas,
                LAG(wind_speed_kmh, 1) OVER (ORDER BY observed_at_utc) AS poprzedni_wiatr
            FROM weather w
            JOIN cities c ON w.city_id = c.city_id
            WHERE c.city_name = p_miasto
              AND observed_at_utc >= SYSDATE - p_ostatnie_dni
        )
        WHERE wind_speed_kmh >= p_min_predkosc
        ORDER BY observed_at_utc
    ) LOOP
        v_liczba_pomiarow := v_liczba_pomiarow + 1;
        v_roznica := CASE WHEN r.poprzedni_wiatr IS NULL THEN NULL
                          ELSE r.wind_speed_kmh - r.poprzedni_wiatr
                     END;

        DBMS_OUTPUT.PUT(TO_CHAR(r.observed_at_utc, 'MM-DD HH24:MI') || '   | ' || 
                        RPAD(r.wind_speed_kmh, 12) || ' | ' || 
                        RPAD(r.max_dotychczas, 13) || ' | ');

        IF v_roznica IS NULL THEN
            DBMS_OUTPUT.PUT_LINE('Pierwszy pomiar');
        ELSE
            IF v_roznica > 0 THEN
                IF r.wind_speed_kmh >= r.max_dotychczas THEN
                    DBMS_OUTPUT.PUT_LINE('(!) ROŚNIE ↗ (Rekord)');
                ELSE
                    DBMS_OUTPUT.PUT_LINE('Rośnie ↗');
                END IF;
            ELSIF v_roznica < 0 THEN
                DBMS_OUTPUT.PUT_LINE('Maleje ↘');
            ELSE
                DBMS_OUTPUT.PUT_LINE('Stały =');
            END IF;
        END IF;
    END LOOP;

    IF v_liczba_pomiarow = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Brak wiatru powyżej ' || p_min_predkosc || ' km/h w zadanym okresie.');
    END IF;
END;
/


CREATE OR REPLACE FUNCTION anomalie (
    p_miasto      IN VARCHAR2,
    p_ostatnie_dni IN NUMBER DEFAULT 1
) RETURN BOOLEAN IS
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
      AND observed_at_utc >= SYSDATE - p_ostatnie_dni
      AND (
           temperature_c < -30 OR temperature_c > 40 OR
           wind_speed_kmh > 100 OR
           humidity_pct < 10 OR humidity_pct > 90
      );

    RETURN v_count > 0;
END;
/


CREATE OR REPLACE FUNCTION przypisz_grupe_wilgotnosci (
    p_wilgotnosc IN NUMBER
) RETURN NUMBER IS
BEGIN
    IF p_wilgotnosc <= 25 THEN
        RETURN 1;
    ELSIF p_wilgotnosc <= 50 THEN
        RETURN 2;
    ELSIF p_wilgotnosc <= 75 THEN
        RETURN 3;
    ELSE
        RETURN 4; 
    END IF;
END;
/



CREATE OR REPLACE PROCEDURE ocena_wilgotnosci (
    p_miasto       IN VARCHAR2 DEFAULT 'Warszawa',
    p_ostatnie_dni IN NUMBER   DEFAULT 3
) IS
    v_grupa NUMBER;
    v_opis  VARCHAR2(50);
    v_czy_sa_dane NUMBER;
BEGIN

    SELECT COUNT(*)
    INTO v_czy_sa_dane
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    WHERE c.city_name = p_miasto
    AND w.observed_at_utc >= SYSDATE - p_ostatnie_dni;
    
    IF v_czy_sa_dane = 0 THEN
        DBMS_OUTPUT.PUT_LINE('Brak danych dla miasta: ' || p_miasto);
        RETURN;
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('OCENA WILGOTNOŚCI');
    DBMS_OUTPUT.PUT_LINE('Miasto: ' || UPPER(p_miasto));
    DBMS_OUTPUT.PUT_LINE('Zakres: Ostatnie ' || p_ostatnie_dni || ' dni');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('DATA GODZINA  | WILGOTNOŚĆ | GRUPA (1-4) | OCENA RELATYWNA');

    FOR r IN (
        SELECT observed_at_utc, humidity_pct
        FROM weather w
        JOIN cities c ON w.city_id = c.city_id
        WHERE c.city_name = p_miasto
          AND observed_at_utc >= SYSDATE - p_ostatnie_dni
        ORDER BY observed_at_utc DESC 
        FETCH FIRST 15 ROWS ONLY
    ) LOOP
    
        v_grupa := przypisz_grupe_wilgotnosci(r.humidity_pct);

        CASE v_grupa
            WHEN 1 THEN v_opis := 'Sucho (Dolne 25%)';
            WHEN 2 THEN v_opis := 'Umiarkowanie';
            WHEN 3 THEN v_opis := 'Umiarkowanie';
            WHEN 4 THEN v_opis := 'Wilgotno (Górne 25%)';
        END CASE;

        DBMS_OUTPUT.PUT_LINE(
            TO_CHAR(r.observed_at_utc,'DD-MM HH24:MI') || ' | ' ||
            RPAD(r.humidity_pct||'%', 10) || ' | ' ||
            v_grupa || ' | ' || v_opis
        );
    END LOOP;

END;
/


CREATE OR REPLACE FUNCTION oblicz_indeks_komfortu (
    p_temp_c        IN NUMBER,
    p_wind_kmh      IN NUMBER,
    p_humidity_pct  IN NUMBER
) RETURN NUMBER IS
    v_score NUMBER := 100;
    v_diff  NUMBER;
BEGIN

    v_diff := ABS(p_temp_c - 22);
    v_score := v_score - (v_diff * 2.5);

    IF p_temp_c < 10 THEN

        IF p_wind_kmh > 5 THEN
            v_score := v_score - (p_wind_kmh * 0.8); 
        END IF;
    ELSIF p_wind_kmh > 30 THEN
        v_score := v_score - 15;
    END IF;

    IF p_humidity_pct < 30 THEN
        v_score := v_score - 5; 
    ELSIF p_humidity_pct > 80 THEN 
        v_score := v_score - 10;
    END IF;


    IF v_score < 0 THEN v_score := 0; END IF;
    IF v_score > 100 THEN v_score := 100; END IF;

    RETURN ROUND(v_score);
END;
/

CREATE OR REPLACE PROCEDURE pokaz_aktualny_komfort (
    p_city_name IN VARCHAR2 DEFAULT 'Warszawa'
) IS
    v_temp  NUMBER;
    v_wind  NUMBER;
    v_hum   NUMBER;
    v_score NUMBER;
    v_desc  VARCHAR2(100);
    v_data_pomiaru DATE;
BEGIN
    SELECT temperature_c, wind_speed_kmh, humidity_pct, observed_at_utc
    INTO v_temp, v_wind, v_hum, v_data_pomiaru
    FROM (
        SELECT w.temperature_c, w.wind_speed_kmh, w.humidity_pct, w.observed_at_utc
        FROM weather w
        JOIN cities c ON w.city_id = c.city_id
        WHERE c.city_name = p_city_name
        ORDER BY w.observed_at_utc DESC
    )
    FETCH FIRST 1 ROW ONLY;


    IF v_data_pomiaru < SYSDATE - 7 THEN
        DBMS_OUTPUT.PUT_LINE('Dane dla miasta ' || p_city_name || ' są zbyt stare (>7 dni), brak wiarygodnych informacji.');
        RETURN;
    END IF;

    v_score := oblicz_indeks_komfortu(v_temp, v_wind, v_hum);

    IF v_score >= 80 THEN v_desc := 'Pogoda Idealna️';
    ELSIF v_score >= 60 THEN v_desc := 'Jest OK, przyjemnie';
    ELSIF v_score >= 40 THEN v_desc := 'Średnio, ubierz się dobrze';
    ELSIF v_score >= 20 THEN v_desc := 'Warunki trudne️';
    ELSE v_desc := 'Zostań w domu!';
    END IF;

    DBMS_OUTPUT.PUT_LINE('INDEKS KOMFORTU: ' || UPPER(p_city_name));
    DBMS_OUTPUT.PUT_LINE('Warunki: ' || v_temp || 'C, Wiatr: ' || v_wind || 'km/h, Wilgoć: ' || v_hum || '%');
    DBMS_OUTPUT.PUT_LINE('Data pomiaru: ' || TO_CHAR(v_data_pomiaru, 'DD.MM.YYYY HH24:MI'));
    DBMS_OUTPUT.PUT_LINE('PUNKTY KOMFORTU (0-100):  ' || v_score);
    DBMS_OUTPUT.PUT_LINE('WERDYKT:                  ' || v_desc);

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Brak danych dla miasta ' || p_city_name);
END;
/



