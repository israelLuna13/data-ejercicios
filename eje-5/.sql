CREATE TABLE customers (
    customer_id int PRIMARY KEY,
    country_id int,
    customerid int,
    FOREIGN KEY (country_id) REFERENCES country(country_id));

CREATE TABLE products (
    product_id int PRIMARY KEY,
    stockcode text,
    description text);

CREATE TABLE transactions(
    transaction_id int PRIMARY KEY,
    invoiceno text,
    invoicedate date,
    customer_id int,
    type text,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id));

CREATE TABLE transactions_details(
    transaction_details_id int PRIMARY KEY,
    quantity int,
    unitprice float,
    transaction_id int,
    product_id int,
    FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
    );

SELECT * FROM transactions left join transactions_details on transactions.transaction_id = transactions_details.transaction_id;

--total sales
SELECT COUNT(*) as total_sales from transactions WHERE type = 'sale';
-- cuantos clientes unicos han comprado
-- SELECT COUNT(*) as customers_sales FROM(
-- SELECT customers.customer_id,transactions.customer_id from transactions left join customers on transactions.customer_id = customers.customer_id WHERE type = 'sale'
-- GROUP BY customers.customer_id,transactions.customer_id
-- ) t;
SELECT COUNT(DISTINCT customer_id) AS customers_sales
FROM transactions
WHERE type = 'sale';

--cuantos productos distintos se han vendido
SELECT COUNT(DISTINCT product_id) AS products_sales
FROM transactions_details
WHERE quantity > 0;

-- 	4.	¿Cuál es el ingreso total generado?
SELECT SUM(quantity * unitprice) as ingreso_generado from transactions_details WHERE quantity > 0;
-- 	5.	¿Cuánto se vende por día?
SELECT transactions.invoicedate,SUM(transactions_details.quantity*transactions_details.unitprice) as total_sale_per_day 
FROM transactions 
left join transactions_details 
on transactions.transaction_id = transactions_details.transaction_id
WHERE transactions_details.quantity > 0
GROUP BY  transactions.invoicedate;
-- 	6.	¿Cuánto se vende por país?
SELECT country.country, SUM(td.quantity * td.unitprice) as total_sales_per_country
FROM customers c
left join country
    on c.country_id = country.country_id
left join transactions t
    on t.customer_id = c.customer_id
left join transactions_details td
    on td.transaction_id = t.transaction_id
WHERE td.quantity > 0
GROUP BY country.country
ORDER BY total_sales_per_country DESC;
 
-- 	7.	¿Cuál es el top 10 de productos más vendidos (por cantidad)
SELECT p.product_id,p.description,p.stockcode,SUM(td.quantity) as top_product_sales
FROM products p
left join transactions_details td
 on p.product_id = td.product_id
WHERE td.quantity > 0
GROUP BY p.product_id,p.description,p.stockcode
ORDER BY top_product_sales DESC;
--limit 10;

-- 	8.	¿Cuál es el top 10 de productos que más dinero generan?
SELECT p.product_id,p.description,p.stockcode,SUM(td.quantity * td.unitprice) as top_product_money
FROM products p
left join transactions_details td
 on p.product_id = td.product_id
WHERE td.quantity > 0
GROUP BY p.product_id,p.description,p.stockcode
ORDER BY top_product_money DESC;
--limit 10;


-- 	9.	¿Quiénes son los clientes que más gastan? (top 10)
SELECT t.customer_id,SUM(td.quantity * td.unitprice) AS top_customers
FROM customers c
LEFT JOIN transactions t
 on c.customer_id = t.customer_id
LEFT JOIN transactions_details td
 on t.transaction_id = td.transaction_id
WHERE td.quantity > 0
GROUP BY t.customer_id
ORDER BY top_customers DESC;
-- 	10.	¿Cuál es el ticket promedio por venta? (cuánto dinero en promedio tiene cada invoice)
SELECT AVG(promedio_per_invoiceno)
FROM (
    SELECT t.invoiceno, SUM(td.quantity * td.unitprice) as promedio_per_invoiceno
FROM transactions t
LEFT JOIN transactions_details td
 on t.transaction_id = td.transaction_id
WHERE td.quantity > 0
GROUP BY t.invoiceno
) t ;


-- 	11.	¿Cuántas devoluciones hay vs ventas?
SELECT SUM(CASE WHEN type = 'sale' THEN 1 ELSE 0 END) as total_sales,
       SUM(CASE WHEN type = 'return' THEN 1 ELSE 0 END) as total_return
FROM transactions;

-- 	12.	¿Cuánto dinero se pierde en devoluciones?
SELECT ABS( SUM(td.quantity * td.unitprice)) as promedio_per_invoiceno
FROM transactions t
LEFT JOIN transactions_details td
 on t.transaction_id = td.transaction_id
WHERE td.quantity < 0;

-- 	13.	¿Qué país tiene mejor rendimiento?
-- 👉 (puedes definirlo como más ingresos o ticket promedio)
SELECT country , AVG(sum_invoiceno)
FROM(
     SELECT t.invoiceno,co.country, SUM(td.quantity * td.unitprice) as sum_invoiceno
FROM transactions t
LEFT JOIN transactions_details td
 on t.transaction_id = td.transaction_id
LEFT JOIN customers c
 on t.customer_id = c.customer_id
LEFT JOIN country co
 on co.country_id = c.country_id
WHERE td.quantity > 0
GROUP BY t.invoiceno,co.country
)t 
GROUP BY country;
-- 	14.	¿Qué días se vende más?
-- 👉 (puedes agrupar por día de la semana)
SELECT transactions.invoicedate,SUM(transactions_details.quantity*transactions_details.unitprice) as total_sale_per_day 
FROM transactions 
left join transactions_details 
on transactions.transaction_id = transactions_details.transaction_id
WHERE transactions_details.quantity > 0
GROUP BY  transactions.invoicedate;


SELECT 
  TO_CHAR(invoicedate, 'Day') AS day_name,
  SUM(td.quantity * td.unitprice) AS total
FROM transactions t
JOIN transactions_details td
  ON t.transaction_id = td.transaction_id
WHERE td.quantity > 0
GROUP BY TO_CHAR(invoicedate, 'Day');
-- 	15.	¿Clientes frecuentes vs clientes ocasionales?
-- 👉 (cuántas compras hace cada cliente)
SELECT 
    t.customer_id,
    COUNT(*) AS total_sales_per_user,
    CASE
        WHEN COUNT(*) >= 5 THEN 'Frecuente'
        ELSE 'Ocasional'
    END AS customer_type
FROM transactions t
WHERE t.type = 'sale'
GROUP BY t.customer_id
ORDER BY total_sales_per_user DESC;

-- GOLD ()

-- 	•	Ventas por día
CREATE TABLE sales_per_day as
    SELECT DATE(transactions.invoicedate) as day,
    SUM(transactions_details.quantity*transactions_details.unitprice) as total_sale_per_day 
    FROM transactions 
    left join transactions_details 
    on transactions.transaction_id = transactions_details.transaction_id
    WHERE transactions_details.quantity > 0
    GROUP BY  DATE(transactions.invoicedate);
-- 	•	Ventas por país
CREATE TABLE sales_per_country as
    SELECT country.country, SUM(td.quantity * td.unitprice) as total_sales_per_country
    FROM customers c
    left join country
        on c.country_id = country.country_id
    left join transactions t
        on t.customer_id = c.customer_id
    left join transactions_details td
        on td.transaction_id = t.transaction_id
    WHERE td.quantity > 0
    GROUP BY country.country
    HAVING SUM(td.quantity * td.unitprice) > 0
    ORDER BY total_sales_per_country DESC;
    
-- 	•	Top productos por ingresos
CREATE TABLE top_products_income as
    SELECT p.product_id,p.description,p.stockcode,SUM(td.quantity * td.unitprice) as top_product_money
    FROM products p
    left join transactions_details td
    on p.product_id = td.product_id
    WHERE td.quantity > 0
    GROUP BY p.product_id,p.description,p.stockcode
    ORDER BY top_product_money DESC;
-- 	•	Top clientes
CREATE TABLE top_customers as
    SELECT c.customer_id,SUM(td.quantity * td.unitprice) AS top_customers
    FROM customers c
    LEFT JOIN transactions t
    on c.customer_id = t.customer_id
    LEFT JOIN transactions_details td
    on t.transaction_id = td.transaction_id
    WHERE td.quantity > 0
    GROUP BY c.customer_id
    ORDER BY top_customers DESC
    ;
-- 	•	Ticket promedio
CREATE TABLE average_ticket as
    SELECT AVG(promedio_per_invoiceno)
    FROM (
        SELECT t.invoiceno, SUM(td.quantity * td.unitprice) as promedio_per_invoiceno
    FROM transactions t
    LEFT JOIN transactions_details td
    on t.transaction_id = td.transaction_id
    WHERE td.quantity > 0
    GROUP BY t.invoiceno
    ) t ;

-- 	•	Devoluciones (cantidad y dinero)
CREATE TABLE sales_returned as
    SELECT t.transaction_id,
    SUM(ABS(td.quantity)) as quantity_returned,
    SUM(ABS(td.quantity) * td.unitprice) as total_amount_returned
    FROM transactions t
    LEFT JOIN transactions_details td
    on t.transaction_id = td.transaction_id
    WHERE td.quantity < 0
    GROUP BY t.transaction_id
;
-- •	conteos básicos
-- •	productos distintos
-- •	clientes totales