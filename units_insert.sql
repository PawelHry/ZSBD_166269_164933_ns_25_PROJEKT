INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES ('temperature', 'C', 'Celsius (canonical)', 1, 0, NULL);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES ('humidity', '%', 'Percent (canonical)', 1, 0, NULL);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES ('precipitation', 'mm', 'Millimeter (canonical)', 1, 0, NULL);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES ('wind_speed', 'kmh', 'Kilometers per hour (canonical)', 1, 0, NULL);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES (
  'temperature', 'F', 'Fahrenheit',
  0.55555556, -17.77777778,
  (SELECT unit_id FROM units WHERE dimension='temperature' AND unit_symbol='C')
);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES (
  'temperature', 'K', 'Kelvin',
  1, -273.15,
  (SELECT unit_id FROM units WHERE dimension='temperature' AND unit_symbol='C')
);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES (
  'wind_speed', 'ms', 'Meters per second',
  3.6, 0,
  (SELECT unit_id FROM units WHERE dimension='wind_speed' AND unit_symbol='kmh')
);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES (
  'wind_speed', 'mph', 'Miles per hour',
  1.609344, 0,
  (SELECT unit_id FROM units WHERE dimension='wind_speed' AND unit_symbol='kmh')
);

INSERT INTO units (dimension, unit_symbol, unit_name, to_canonical_mul, to_canonical_add, canonical_unit_id)
VALUES (
  'precipitation', 'in', 'Inch',
  25.4, 0,
  (SELECT unit_id FROM units WHERE dimension='precipitation' AND unit_symbol='mm')
);
