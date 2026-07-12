CREATE TABLE orders_data (
    order_id TEXT,
    customer_id TEXT,
    date TEXT,
    age INTEGER,
    gender TEXT,
    city TEXT,
    product_category TEXT,
    unit_price DOUBLE PRECISION,
    quantity INTEGER,
    discount_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    payment_method TEXT,
    device_type TEXT,
    session_duration_minutes INTEGER,
    pages_viewed INTEGER,
    is_returning_customer BOOLEAN,
    delivery_time_days INTEGER,
    customer_rating INTEGER
);

CREATE TABLE dim_payment(
id_payment INT PRIMARY KEY,
payment_method TEXT
);
CREATE TABLE dim_device(
id_device INT PRIMARY KEY,
device_type TEXT
);

CREATE TABLE dim_customer(
id_cust INT PRIMARY KEY,
customer_id TEXT,
age INT,
gender TEXT,
city TEXT,
is_returning_customer BOOLEAN
);

CREATE TABLE dim_category(
id_category INT PRIMARY KEY,
product_category TEXT
);

CREATE TABLE dim_fact(
id_fact INT PRIMARY KEY,
order_id TEXT,
date date,
id_cust INT,
id_category INT,
id_device INT,
id_payment INT,
unit_price FLOAT,
quantity INT,
discount_amount FLOAT,
total_amount FLOAT,
delivery_time_days INT,
customer_rating INT,
FOREIGN KEY (id_cust) REFERENCES dim_customer(id_cust),
FOREIGN KEY (id_device) REFERENCES dim_device(id_device),
FOREIGN KEY (id_payment) REFERENCES dim_payment(id_payment),
FOREIGN KEY (id_category) REFERENCES dim_category(id_category)
);

--     ¿Cuántas ventas hay en total?
SELECT COUNT(*) AS Total_sales
FROM dim_fact df;

-- 2. ¿Cuál es el monto total vendido?
SELECT SUM(df.total_amount) AS Total_amount
FROM dim_fact df;

-- 3. ¿Cuál es el ticket promedio (total_amount)?
SELECT AVG(dim_fact.total_amount) AS avg_amount
FROM dim_fact df;

-- 4. ¿Cuál fue la venta más cara?
SELECT df.order_id, df.total_amount
FROM dim_fact df
ORDER BY df.total_amount DESC
--limit 3
;

-- 5. ¿Cuál fue la venta más barata (excluyendo las de $0)?
SELECT df.order_id, df.total_amount
FROM dim_fact df
WHERE df.total_amount > 0
ORDER BY df.total_amount ASC
--limit 1
;
-- 6. ¿Cuántos clientes distintos existen?
SELECT COUNT(DISTINCT dc.customer_id)
FROM dim_customer dc;
-- 7. ¿Cuántos métodos de pago existen?
SELECT COUNT(*) AS total_payment_method
FROM dim_payment;

-- 8. ¿Cuántos dispositivos existen?
SELECT COUNT(*) AS total_device
FROM dim_device;

-- 🟡 Intermedias (JOIN)

-- 9. ¿Qué método de pago se utilizó más?
SELECT dp.payment_method , COUNT(dp.payment_method) as total_use_payment
FROM dim_fact df
JOIN dim_payment dp
ON df.id_payment = dp.id_payment
GROUP BY  dp.payment_method
ORDER BY total_use_payment DESC
;
-- 10. ¿Cuál fue el monto vendido por método de pago?
SELECT dp.payment_method , SUM(df.total_amount) as total_amount_payment
FROM dim_fact df
JOIN dim_payment dp
ON df.id_payment = dp.id_payment
GROUP BY  dp.payment_method 
ORDER BY total_amount_payment DESC
;

-- 12. ¿Cuál es el ticket promedio por dispositivo?
SELECT dd.device_type , AVG(df.total_amount) as avg_amount_payment
FROM dim_fact df
JOIN dim_device dd
ON df.id_device = dd.id_device
GROUP BY  dd.device_type
ORDER BY avg_amount_payment DESC;

-- 13. ¿Qué categoría de producto generó más ingresos?
SELECT dc.product_category, SUM(df.total_amount) as total_amount_category
FROM dim_fact df
JOIN dim_category dc
ON df.id_category = dc.id_category
GROUP BY dc.product_category
ORDER BY total_amount_category DESC
--LIMIT 1
;
-- 14. ¿Qué categoría vendió más unidades?
SELECT dc.product_category, SUM(df.quantity) as total_quantity_category
FROM dim_fact df
JOIN dim_category dc
ON df.id_category = dc.id_category
GROUP BY dc.product_category
ORDER BY total_quantity_category DESC
--LIMIT 1
;

-- 15. ¿Qué ciudad tuvo más ventas?
SELECT dc.city, SUM(df.total_amount) as total_sales_city
FROM dim_fact df
JOIN dim_customer dc
ON df.id_cust = dc.id_cust
GROUP BY dc.city
ORDER BY total_sales_city DESC
--LIMIT 1
;
-- 16. ¿Qué ciudad tuvo el mayor ingreso?
SELECT dc.city, SUM(df.total_amount) as total_sales_city
FROM dim_fact df
JOIN dim_customer dc
ON df.id_cust = dc.id_cust
GROUP BY dc.city
ORDER BY total_sales_city DESC
--LIMIT 1
;

-- 19. ¿Qué método de pago tiene la mejor calificación promedio?
SELECT dp.payment_method , AVG(df.customer_rating) as avg_rating
FROM dim_fact df
JOIN dim_payment dp
ON df.id_payment = dp.id_payment
GROUP BY  dp.payment_method
ORDER BY avg_rating DESC
--LIMIT 1
;

-- 20. ¿Cuál es el tiempo promedio de entrega por ciudad?
SELECT dc.city, AVG(df.delivery_time_days) as avg_delivery_time
FROM dim_fact df
JOIN dim_customer dc
ON df.id_cust = dc.id_cust
GROUP BY dc.city
ORDER BY avg_delivery_time DESC
;

-- 🟠 Agregaciones

-- 21. Top 10 clientes que más gastaron.
SELECT dc.customer_id, SUM(df.total_amount) as total_sales_customer
FROM dim_fact df
JOIN dim_customer dc
ON df.id_cust = dc.id_cust
GROUP BY dc.customer_id
ORDER BY total_sales_customer DESC
--LIMIT 10
;

-- 24. Promedio de descuento por categoría.
SELECT dc.product_category, AVG(df.discount_amount) as avg_total_discount
FROM dim_fact df
JOIN dim_category dc
ON df.id_category = dc.id_category
GROUP BY dc.product_category
ORDER BY avg_total_discount DESC
--LIMIT 1
;

-- GOLD 1

-- Encontrar el cliente que más dinero gastó y mostrar:

-- * cliente
-- * ciudad
-- * edad
-- * total gastado
CREATE TABLE gold_total_spent as
    SELECT
        dc.customer_id,
        dc.city,
        dc.age,
        SUM(df.total_amount) AS total_spent
    FROM dim_fact df
    JOIN dim_customer dc
        ON df.id_cust = dc.id_cust
    GROUP BY
        dc.customer_id,
        dc.city,
        dc.age
    ORDER BY total_spent DESC;
;

-- GOLD 2

-- Ranking de ciudades por ingresos utilizando funciones ventana (RANK, DENSE_RANK o ROW_NUMBER).
-- ⸻
CREATE TABLE gold_city_rank as
    SELECT *,
        DENSE_RANK() OVER (ORDER BY total_amount DESC) AS city_rank
    FROM (
        SELECT
            dc.city,
            SUM(df.total_amount) AS total_amount
        FROM dim_fact df
        JOIN dim_customer dc
            ON df.id_cust = dc.id_cust
        GROUP BY dc.city
    ) t
    ORDER BY city_rank;
-- GOLD 3

-- Para cada categoría:

-- * total vendido
-- * promedio vendido
-- * porcentaje respecto al total general
CREATE TABLE gold_category as
    SELECT
        dc.product_category,
        AVG(df.total_amount) AS average_sales,
        SUM(df.total_amount) AS total_sales,
        SUM(df.discount_amount) * 100.0 /
            (SELECT SUM(total_amount) FROM dim_fact) AS discount_percentage
    FROM dim_fact df
    JOIN dim_category dc
        ON df.id_category = dc.id_category
    GROUP BY dc.product_category
    ORDER BY total_sales DESC;
-- ⸻

-- GOLD 4

-- Encontrar clientes cuyo gasto esté por encima del promedio de todos los clientes.
CREATE TABLE gold_customers_avg as
    SELECT *
    FROM (
        SELECT dc.customer_id, dc.city, dc.age,
            SUM(df.total_amount) as total_spent
        FROM dim_fact df
            JOIN dim_customer dc
            ON df.id_cust = dc.id_cust
        GROUP BY dc.customer_id,dc.city,dc.age
    ) t
    WHERE total_spent > (
        SELECT
        AVG(total_spent) as total_spent
    FROM (
            SELECT SUM(total_amount) as total_spent
        FROM dim_fact
        GROUP BY id_cust
        ) avg_customer
    )
    ORDER BY total_spent DESC
    ;
-- ⸻

-- GOLD 7

-- Para cada ciudad obtener:

-- * total ventas
-- * ticket promedio
-- * cantidad de clientes

-- Ordenar por ingreso.

SELECT
    dc.city,
    AVG(df.total_amount) AS average_sales,
    SUM(df.total_amount) AS total_sales,
    COUNT(DISTINCT dc.id_cust) AS total_customers
FROM dim_fact df
JOIN dim_customer dc
    ON df.id_cust = dc.id_cust
GROUP BY dc.city
ORDER BY total_sales DESC;
-- GOLD 10

-- Construir un dashboard solo con SQL que responda:

-- * Total vendido.
-- * Número de órdenes.
-- * Ticket promedio.
-- * Ciudad con mayores ventas.
-- * Categoría más vendida.
-- * Método de pago favorito.
-- * Dispositivo más utilizado.
-- * Calificación promedio.
-- * Tiempo promedio de entrega.
CREATE TABLE gold_dashboard as
    SELECT
        (SELECT SUM(total_amount)
        FROM dim_fact) AS total_sales,

        (SELECT COUNT(*)
        FROM dim_fact) AS total_orders,

        (SELECT AVG(total_amount)
        FROM dim_fact) AS average_ticket,

        (
            SELECT dc.city
            FROM dim_fact df
            JOIN dim_customer dc
                ON df.id_cust = dc.id_cust
            GROUP BY dc.city
            ORDER BY SUM(df.total_amount) DESC
            LIMIT 1
        ) AS top_city,

        (
            SELECT dc.product_category
            FROM dim_fact df
            JOIN dim_category dc
                ON df.id_category = dc.id_category
            GROUP BY dc.product_category
            ORDER BY SUM(df.total_amount) DESC
            LIMIT 1
        ) AS top_category,

        (
            SELECT dp.payment_method
            FROM dim_fact df
            JOIN dim_payment dp
                ON df.id_payment = dp.id_payment
            GROUP BY dp.payment_method
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ) AS favorite_payment_method,

        (
            SELECT dd.device_type
            FROM dim_fact df
            JOIN dim_device dd
                ON df.id_device = dd.id_device
            GROUP BY dd.device_type
            ORDER BY COUNT(*) DESC
            LIMIT 1
        ) AS most_used_device,

        (
            SELECT AVG(customer_rating)
            FROM dim_fact
        ) AS average_rating,

        (
            SELECT AVG(delivery_time_days)
            FROM dim_fact
        ) AS average_delivery_time;