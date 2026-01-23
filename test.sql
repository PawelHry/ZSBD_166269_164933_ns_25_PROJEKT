SET SERVEROUTPUT ON;
--pkt 3e
EXEC prognozuj_temperature('Warszawa', 'F');

DECLARE
    v_temp NUMBER;
    wynik BOOLEAN;
    v_wilgotnosc NUMBER := 62;
    v_grupa      NUMBER;
BEGIN
    v_temp := konwertuj_temperature(25, 'K');
    DBMS_OUTPUT.PUT_LINE('Temperatura: ' || v_temp || ' K');
    wynik := anomalie('Warszawa', 3);
    IF wynik THEN
        DBMS_OUTPUT.PUT_LINE('Występują anomalie w ostatnich 3 dniach!');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Brak anomalii.');
    END IF;
    
    v_grupa := przypisz_grupe_wilgotnosci(v_wilgotnosc);

    DBMS_OUTPUT.PUT_LINE('Wilgotność: ' || v_wilgotnosc || '% należy do grupy: ' || v_grupa);
END;
/

EXEC ranking_zmiennosci(5, 10);
EXEC monitoruj_wiatr('Olsztyn', 2);
EXEC ocena_wilgotnosci('Warszawa', 3);
EXEC pokaz_aktualny_komfort('Kraków');


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



select * from v_raporty_miesieczne;
select * from v_raporty_kwartalne;
select * from v_raporty_roczne;

select * from cities;
select * from weather ORDER BY observed_at_utc DESC;


