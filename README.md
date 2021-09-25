# vaccin_happiness

Тестовое задание.

Хотим исследовать зависимость скорости вакцинации от рейтинга "счастья".

В данный момент в проекте есть: 
- Код, загружающий csv в hive и создающий следующие таблицы (класс DataLoader): 
    - happiness_2019
    - country_vaccinations
    - country_vaccinations_by_manufacturer
- Демонстрационная витрина total_vaccinations_by_manufacturer. В ней посчитано общее количество вакцинаций по производителям вакцины.

В рамках задания необходимо форкнуть репозиторий и разработать код, который объединит два рейтинга и построит следующие витрины:

- vaccines_by_rank - Процент вакцинированных в стране (people_fully_vaccinated_per_hundred) в зависимости от места в рейтинге (Overall_rank), отсортировать по месту
- vaccines_by_gdp - Процент вакцинированных в стране (people_fully_vaccinated_per_hundred) в зависимости от ВВП (GDP_per_capita), отсортировать по ВВП
- vaccines_by_healthy - Процент вакцинированных в стране (people_fully_vaccinated_per_hundred) в зависимости от продолжительности жизни, (Healthy_life_expectancy) отсортировать по продолжительности жизни
- vaccines_by_freedom - Процент вакцинированных в стране (people_fully_vaccinated_per_hundred) в зависимости от Freedom_to_make_life_choices, отсортировать по Freedom_to_make_life_choices
- vaccines_by_corruption - Процент вакцинированных в стране (people_fully_vaccinated_per_hundred) в зависимости от Perceptions_of_corruption, отсортировать по Perceptions_of_corruption

Также в данных возможны ошибки, надо построить 2 витрины с ошибками объединения отчетов.
- error_vaccines_county - Таблица со странами, по которым есть рейтинг счастья, но нет данных по вакцинации.
- error_happiness_county - Таблица со странами, по которым есть данные по вакцинации, но нет рейтинга счастья.

