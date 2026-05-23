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
SELECT c.city, COUNT(*) as citys_deliverys
FROM fact_deliveries fd
JOIN citys c on fd.city_id = c.city_id
GROUP BY c.city
ORDER BY citys_deliverys DESC
;

--    Qué clima afecta más el rating
SELECT  w.weather,
        ROUND(AVG(fd.delivery_person_ratings),2) AS avg_rating_weather
FROM fact_deliveries fd
JOIN weather w  
ON fd.weather_id = w.weather_id
GROUP BY w.weather
ORDER BY avg_rating_weather ASC
;

-- Cuál es el tiempo promedio entre ordenar y recoger
SELECT
    AVG(time_order_picked - time_orderd) AS avg_prep_time
FROM fact_deliveries;

-- Qué equipos tienen mejores ratings
SELECT dt.delivery_person_id, 
       fd.team_id, 
       ROUND(AVG(fd.delivery_person_ratings),2) as avg_ratings
FROM fact_deliveries fd
JOIN delivery_team dt 
on fd.team_id = dt.team_id
GROUP BY dt.delivery_person_id,fd.team_id
ORDER BY avg_ratings DESC
;

--Qué ciudades tienen peores ratings
SELECT fd.city_id, c.city ,
       ROUND(AVG(fd.delivery_person_ratings),2) AS average_rating
FROM fact_deliveries fd
JOIN citys c 
on fd.city_id = c.city_id
GROUP BY fd.city_id, c.city 
ORDER BY average_rating ASC;

--    Hay tráfico que afecte entregas
SELECT t.road_traffic_density , ROUND(AVG(fd.delivery_person_ratings),2) as peor_ratings_traffic
FROM fact_deliveries fd
JOIN traffic t on fd.traffic_id = t.traffic_id
GROUP BY t.road_traffic_density 
ORDER BY peor_ratings_traffic ASC;

-- * total deliveries
SELECT COUNT(*) AS total_deliveries
FROM fact_deliveries fd;
-- * avg rating
SELECT AVG(fd.delivery_person_ratings) as avg_rating
FROM fact_deliveries fd;
-- * avg prep time
-- * deliveries by vehicle
SELECT v.type_of_vehicle , COUNT(fd.delivery_person_id) as deliveries_vehicle
FROM fact_deliveries fd
JOIN vehicle v on fd.vehicle_id = v.vehicle_id
GROUP BY v.type_of_vehicle
