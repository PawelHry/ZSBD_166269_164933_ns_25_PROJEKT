DECLARE
  v_id NUMBER;
BEGIN
  pkg_cities.city_add(
    p_city_name => 'Testowo',
    p_country_code => 'PL',
    p_latitude => 53.7784,
    p_longitude => 20.4801,
    p_timezone_name => 'Europe/Warsaw',
    p_is_active => 1,
    p_city_id => v_id
  );
END;
/

SELECT * FROM cities WHERE city_name = 'Testowo';
SELECT * FROM logs  WHERE action LIKE 'CITY_%' ORDER BY log_id DESC FETCH FIRST 20 ROWS ONLY;


BEGIN
  pkg_cities.city_update(
    p_city_id => (SELECT city_id FROM cities WHERE city_name = 'Testowo'),
    p_city_name => 'Testowo2',
    p_timezone_name => 'Europe/Warsaw'
  );
END;
/

SELECT * FROM cities WHERE city_name LIKE 'Testowo%';
SELECT * FROM logs  WHERE action LIKE 'CITY_%' ORDER BY log_id DESC FETCH FIRST 20 ROWS ONLY;


BEGIN
  pkg_cities.city_set_active(
    p_city_id => (SELECT city_id FROM cities WHERE city_name = 'Testowo2'),
    p_is_active => 0
  );
END;
/

SELECT city_id, city_name, is_active FROM cities WHERE city_name = 'Testowo2';
SELECT * FROM logs ORDER BY log_id DESC FETCH FIRST 20 ROWS ONLY;


BEGIN
  pkg_cities.city_delete(
    p_city_id => (SELECT city_id FROM cities WHERE city_name = 'Testowo2'),
    p_reason => 'sprzątanie po teście'
  );
END;
/

SELECT * FROM cities WHERE city_name = 'Testowo2';
SELECT * FROM cities_archive WHERE city_name LIKE 'Testowo%' ORDER BY archive_id DESC;
SELECT * FROM logs ORDER BY log_id DESC FETCH FIRST 30 ROWS ONLY;
