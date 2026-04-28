CREATE TABLE business_raw
(
    id SERIAL PRIMARY KEY,
    product_type TEXT,
    net_quantity INT,
    gross_sales FLOAT,
    discounts FLOAT,
    returns FLOAT,
    total_net_sales FLOAT
)

CREATE TABLE products_type
(
    product_id SERIAL PRIMARY KEY,
    product_type TEXT
)

CREATE TABLE sales
(
    sale_id SERIAL PRIMARY KEY,
    product_id INT,
    net_quantity INT,
    gross_sales FLOAT,
    discounts FLOAT,
    returns FLOAT,
    total_net_sales FLOAT,

    FOREIGN KEY (product_id) REFERENCES products_type (product_id)


);

--products / category con mas ventas
SELECT pt.product_type, SUM(s.total_net_sales) as total_sales
FROM sales s
JOIN products_type pt
ON s.product_id = pt.product_id
GROUP BY pt.product_type
ORDER BY total_sales DESC

--products / category con mas descuentos
SELECT 
  pt.product_type, 
  SUM(ABS(s.discounts)) AS total_discounts
FROM sales s
JOIN products_type pt
  ON s.product_id = pt.product_id
WHERE s.discounts < 0
GROUP BY pt.product_type
ORDER BY total_discounts DESC
--LIMIT 5
;

-- Productos con más devoluciones
SELECT
  pt.product_type, SUM(ABS(s.returns)) as total_returns
FROM sales s
JOIN products_type pt
ON s.product_id = pt.product_id
GROUP BY pt.product_type
ORDER BY total_returns DESC
--LIMIT 10
;

-- 👉 ¿Qué productos:

-- * venden mucho (net_quantity)
-- * pero generan poco dinero?
SELECT
  pt.product_type, 
  SUM(s.net_quantity) AS total_quantity, 
  SUM(s.total_net_sales) AS total_net_sales,
  SUM(s.total_net_sales) / NULLIF(SUM(s.net_quantity), 0) AS avg_price_per_unit
FROM sales s
JOIN products_type pt
  ON s.product_id = pt.product_id
GROUP BY pt.product_type
ORDER BY total_quantity DESC, total_net_sales ASC
--LIMIT 10
;

-- 🟡 GOLD 2: Métricas derivadas

-- Agrega:

-- * margen real
SELECT
  pt.product_type, 
  SUM(s.total_net_sales) / NULLIF(SUM(s.gross_sales ),0) as margen
FROM sales s
JOIN products_type pt
ON s.product_id = pt.product_id
GROUP BY pt.product_type
ORDER BY margen DESC
--LIMIT 10
--;

-- * porcentaje de descuentos
SELECT
  pt.product_type, 
  SUM(ABS(s.discounts)) / NULLIF(SUM(s.gross_sales), 0) as porcentaje
FROM sales s
JOIN products_type pt
ON s.product_id = pt.product_id
GROUP BY pt.product_type
ORDER BY porcentaje DESC
--LIMIT 10
--;


-- * porcentaje de devoluciones
SELECT
  pt.product_type, 
  SUM(ABS(s.returns)) / SNULLIF(SUM(s.gross_sales), 0) as porcentaje
FROM sales s
JOIN products_type pt
ON s.product_id = pt.product_id
GROUP BY pt.product_type
ORDER BY porcentaje DESC
LIMIT 10
;

-- * eficiencia de ventas


-- * product_type
-- * ranking por:
--     * ventas
--     * devoluciones
--     * eficiencia

-- * 🟢 “High Performers” → alta venta, bajas devoluciones
-- * 🟡 “Discount Driven” → muchas ventas pero muchos descuentos
-- * 🔴 “Problemáticos” → muchas devoluciones