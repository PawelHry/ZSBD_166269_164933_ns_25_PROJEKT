SET SERVEROUTPUT ON;
--pkt 3e
--testy procedur

select * from cities;
EXEC prognozuj_temperature('Warszawa', 'F');
EXEC prognozuj_temperature('Olsztyn', 'C');
EXEC ranking_zmiennosci(5, 10);
EXEC monitoruj_wiatr('Olsztyn', 2);
EXEC ocena_wilgotnosci('Wrocław', 3);
EXEC pokaz_aktualny_komfort('Kraków');

--testy funkcji
DECLARE
    v_city VARCHAR2(50) := 'Warszawa';
    v_weather_id NUMBER;
    v_old_temp NUMBER;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('Funkcja konwertuj_temperature');
    DBMS_OUTPUT.PUT_LINE('Konwersja na F: '|| konwertuj_temperature(25, 'F'));
    DBMS_OUTPUT.PUT_LINE('Konwersja na K: '|| konwertuj_temperature(25, 'K'));
    DBMS_OUTPUT.PUT_LINE('');
    
    DBMS_OUTPUT.PUT_LINE('Funkcja przypisz_grupe_wilgotnosci');
    DBMS_OUTPUT.PUT_LINE('Wilgotność 10%: '|| przypisz_grupe_wilgotnosci(10));
    DBMS_OUTPUT.PUT_LINE('Wilgotność 45%: '|| przypisz_grupe_wilgotnosci(45));
    DBMS_OUTPUT.PUT_LINE('Wilgotność 99%: '|| przypisz_grupe_wilgotnosci(99));
    DBMS_OUTPUT.PUT_LINE('');
    
    DBMS_OUTPUT.PUT_LINE('Funkcja oblicz_indeks_komfortu');
    DBMS_OUTPUT.PUT_LINE('Scenariusz 1: '|| oblicz_indeks_komfortu(20,45,40));
    DBMS_OUTPUT.PUT_LINE('Scenariusz 2: '|| oblicz_indeks_komfortu(8,15,70));
    DBMS_OUTPUT.PUT_LINE('');
    
    
    DBMS_OUTPUT.PUT_LINE('Funkcja anomalie');
    SELECT weather_id, temperature_c
    INTO v_weather_id, v_old_temp
    FROM (
        SELECT w.weather_id, w.temperature_c
        FROM weather w
        JOIN cities c ON w.city_id = c.city_id
        WHERE c.city_name = v_city
        ORDER BY w.observed_at_utc DESC
    ) FETCH FIRST 1 ROW ONLY;
    
    IF anomalie(v_city,1) THEN
        DBMS_OUTPUT.PUT_LINE('Znaleziono anomalie pogodowe');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Brak anomalii pogodowych');
    END IF;
    
    UPDATE weather SET temperature_c = 75
    WHERE weather_id = v_weather_id;
    
    IF anomalie(v_city,1) THEN
        DBMS_OUTPUT.PUT_LINE('Znaleziono anomalie pogodowe');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Brak anomalii pogodowych');
    END IF;
    
    UPDATE weather SET temperature_c = v_old_temp
    WHERE weather_id = v_weather_id;
    DBMS_OUTPUT.PUT_LINE('');
    
    
END;
/


--pkt 4
select * from weather_reports;
/
BEGIN

    FOR i IN 0..12 LOOP
        generuj_raporty('MONTH', ADD_MONTHS(SYSDATE, -i));
    END LOOP;

    FOR i IN 0..4 LOOP
        generuj_raporty('QUARTER', ADD_MONTHS(SYSDATE, -(i*3)));
    END LOOP;
    
    generuj_raporty('YEAR', SYSDATE);
    generuj_raporty('YEAR', ADD_MONTHS(SYSDATE, -12));

    DBMS_OUTPUT.PUT_LINE('Generowanie raportów zakończone');
END;
/

EXEC generuj_raporty('MONTH', SYSDATE, 'Warszawa');
EXEC generuj_raporty('QUARTER', SYSDATE, 'Warszawa');
EXEC generuj_raporty('YEAR', SYSDATE, 'Warszawa');

select * from weather_reports ORDER BY CREATED_AT DESC;


select * from v_raporty_miesieczne;
select * from v_raporty_kwartalne;
select * from v_raporty_roczne;

BEGIN

DBMS_OUTPUT.PUT_LINE('Dni z silnym wiatrem (>30km/h): ' || 
        funkcja_dni_silny_wiatr('Warszawa', SYSDATE - 30, SYSDATE)
    );

    DBMS_OUTPUT.PUT_LINE('Dni suche (wilgotność <40%):    ' || 
        funkcja_dni_sucho('Warszawa', SYSDATE - 30, SYSDATE)
    );


    DBMS_OUTPUT.PUT_LINE('Dni optymalne (40-70%):         ' || 
        funkcja_dni_optimalnie('Warszawa', SYSDATE - 30, SYSDATE)
    );


    DBMS_OUTPUT.PUT_LINE('Dni wilgotne (>70%):            ' || 
        funkcja_dni_wilgotno('Warszawa', SYSDATE - 30, SYSDATE)
    );
END;
/

--test triggera
BEGIN
    UPDATE weather_reports SET avg_temperature = 999 WHERE ROWNUM = 1;

    DBMS_OUTPUT.PUT_LINE('Udało się zmienić dane.');
    ROLLBACK;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Trigger zablokował zmianę.');
        DBMS_OUTPUT.PUT_LINE('   Komunikat: ' || SQLERRM);
END;
/

select * from cities;
select * from weather ORDER BY observed_at_utc DESC;


