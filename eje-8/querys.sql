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