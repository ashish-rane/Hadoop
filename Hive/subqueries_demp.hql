use retail_db;

SELECT DISTINCT * FROM (SELECT order_id, substr(order_date,1,10) AS orderdate, order_customer_id, order_status FROM orders) t1;
