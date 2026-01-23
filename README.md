
# Projekt na potrzeby przedmiotu  
## Zaawansowane systemy baz danych 2025/2026

**Temat:** Prognoza pogody  
**Autorzy:** Paweł Hryń, Kamila Iwon

## Opis plików w projekcie

- **tabele.sql** – Tworzy szkielet całej bazy danych, czyli miejsca, gdzie będą przechowywane informacje o miastach, pogodzie oraz historii działania systemu.
- **triger.sql** – Dba o automatyczny porządek w danych, na przykład przenosi usuwane informacje do archiwum i zapisuje historię zmian w ustawieniach.
- **cities_insert.sql** – Wgrywa do bazy gotową listę polskich miast wraz ze współrzędnymi, aby system miał na czym pracować od razu po uruchomieniu.
- **pkg_cities.sql** – Zestaw narzędzi do zarządzania miastami, który pozwala bezpiecznie dodawać, edytować i usuwać lokalizacje, pilnując przy tym poprawności danych.
- **raporty_funkcje.sql** – Funkcje pomocnicze wykonujące obliczenia statystyczne, np. zliczanie, ile dni w danym okresie było wietrznych, a ile suchych.
- **raporty.sql** – Odpowiada za generowanie podsumowań (miesięcznych, rocznych) i zawiera zabezpieczenie, które blokuje próby ręcznego zmieniania raportów.
- **import.py** – Program, który łączy się z internetem, pobiera aktualną prognozę pogody z zewnętrznego serwisu i zapisuje ją w naszej bazie danych.
- **test.sql** – Skrypt służący do testowania działania bazy danych.
- **weather_analysis.sql** – Zawiera procedury i funkcje, które na podstawie zgromadzonych danych prognozują temperaturę, wykrywają anomalie, tworzą rankingi zmienności pogody oraz obliczają tzw. indeks komfortu dla mieszkańców.
- **units_insert.sql** – Wgrywa do systemu definicje jednostek miary (np. stopnie Celsjusza, Fahrenheita, km/h).
