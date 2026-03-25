-- ¿Cuánto dinero total se generó?
-- (solo donde sí hubo compra)
select sum(price) as sum_purchased from orders where (price > 0) ;

-- ¿Cuántas órdenes hay en total?
select count(*) as total_orders from orders;

--¿Qué canal genera más dinero?
 select orders.channel_id,channel.embarked,sum(orders.price) as sum_channel  from orders left join channel on orders.channel_id  = channel.channel_id 
 GROUP BY orders.channel_id,channel.embarked
 ORDER BY sum_channel DESC;
--limit 1
 --¿Qué tipo de producto genera más ingresos?
 select orders.product_id,products.pclass,sum(orders.price) as sum_products  from orders left join products on orders.product_id  = products.product_id
 GROUP BY orders.product_id,products.pclass
 ORDER BY sum_products;

--Top 5 clientes que más dinero han gastado
--  select orders.customer_id,customers.sex,orders.price   from orders left join customers on orders.customer_id  = customers.customer_id;
 select orders.customer_id,customers.sex,sum(orders.price) as sum_customers from orders left join customers on orders.customer_id  = customers.customer_id
 GROUP BY  orders.customer_id,customers.customer_id
 ORDER BY sum_customers DESC;
--¿Cuánto gasta en promedio un cliente?
SELECT AVG(avg_customers)
FROM(
select orders.customer_id,customers.sex,AVG(orders.price) as avg_customers from orders left join customers on orders.customer_id  = customers.customer_id
 GROUP BY  orders.customer_id,customers.sex
 ORDER BY avg_customers DESC
) t;
--¿Cuántas órdenes son caras y cuántas no?

--¿Qué canal tiene más órdenes?
--(no dinero, número de órdenes)

--¿Cuál es el promedio de compra por canal?

--Lista de clientes que sí hicieron compras
--(sin repetir)

-- canal con mayor ticket promedio(no total, promedio)

--TABLES GOLD

CREATE TABLE sales_by_channel as
 select orders.channel_id,channel.embarked,sum(orders.price) as sum_channel  from orders left join channel on orders.channel_id  = channel.channel_id
 GROUP BY orders.channel_id,channel.embarked
 ORDER BY sum_channel DESC;

CREATE TABLE sales_by_product as
 select orders.product_id,products.pclass,sum(orders.price) as total_sales  from orders left join products on orders.product_id  = products.product_id
 GROUP BY orders.product_id,products.pclass
 ORDER BY total_sales DESC;

CREATE TABLE customer_summary as
 select orders.customer_id,customers.sex,sum(orders.price) as sum_customers, COUNT(orders.order_id) as sum_orders from orders left join customers on orders.customer_id  = customers.customer_id
 GROUP BY  orders.customer_id,customers.sex
 ORDER BY sum_customers DESC;