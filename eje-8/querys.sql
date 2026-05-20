CREATE TABLE raw_delivery
(
    id TEXT PRIMARY KEY,
    delivery_person_id TEXT,
    delivery_person_age NUMERIC,
    delivery_person_ratings NUMERIC,
    restaurant_latitude DOUBLE PRECISION,
    restaurant_longitude DOUBLE PRECISION,
    delivery_location_latitude DOUBLE PRECISION,
    delivery_location_longitude DOUBLE PRECISION,
    order_date DATE,
    time_orderd TIME,
    time_order_picked TIME,
    weather TEXT,
    road_traffic_density TEXT,
    vehicle_condition INTEGER,
    type_of_order TEXT,
    type_of_vehicle TEXT,
    multiple_deliveries NUMERIC,
    festival TEXT,
    city TEXT,
    name TEXT
);

CREATE TABLE delivery_team
(
    team_id INT PRIMARY KEY,
    delivery_person_id TEXT
);

CREATE TABLE citys
(
    city_id INT PRIMARY KEY,
    city TEXT
);
CREATE TABLE weather
(
    weather_id INT PRIMARY KEY,
    weather TEXT
);

CREATE TABLE orders
(
    order_id INT PRIMARY KEY,
    type_of_order TEXT
);
CREATE TABLE vehicle
(
    vehicle_id INT PRIMARY KEY,
    type_of_vehicle TEXT
);

CREATE TABLE traffic
(
    traffic_id INT PRIMARY KEY,
    road_traffic_density TEXT
)

CREATE TABLE fact_deliveries
(
    fact_id INT PRIMARY KEY,
    team_id INT,
    delivery_person_ratings NUMERIC,
    order_date DATE,
    time_orderd TIME,
    time_order_picked TIME,
    weather_id INT,
    traffic_id INT,
    order_id INT,
    vehicle_id INT,
    city_id INT,
    FOREIGN KEY (team_id) REFERENCES delivery_team(team_id),
    FOREIGN KEY (city_id) REFERENCES citys(city_id),
    FOREIGN KEY (weather_id) REFERENCES weather(weather_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (vehicle_id) REFERENCES vehicle(vehicle_id),
    FOREIGN KEY (traffic_id) REFERENCES traffic(traffic_id)
);
--    Qué ciudad tiene más entregas
SELECT c.city, fd.city_id, COUNT(c.city) as citys_deliverys
FROM fact_deliveries fd
JOIN citys c on fd.city_id = c.city_id
GROUP BY c.city, fd.city_id
ORDER BY citys_deliverys DESC
;

--    Qué clima afecta más el rating
SELECT fd.weather_id, w.weather,fd.delivery_person_ratings
FROM fact_deliveries fd
JOIN weather w  on fd.weather_id = w.weather_id
WHERE fd.delivery_person_ratings < 3
ORDER BY fd.delivery_person_ratings ASC
;

--    Cuál es el tiempo promedio entre ordenar y recoger

--    Qué equipos tienen mejores ratings
SELECT dt.delivery_person_id, fd.team_id, AVG(fd.delivery_person_ratings) as sum_ratings
FROM fact_deliveries fd
JOIN delivery_team dt on fd.team_id = dt.team_id
GROUP BY dt.delivery_person_id,fd.team_id
ORDER BY sum_ratings DESC
;