use retail_demo;

SET hive.vectorized.execution.enabled=true;

CREATE TABLE retail_demo.orders_ctas  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS ORC AS SELECT * FROM retail_db.orders;

-- Get the number of orders based on the status of each using vectorized query.

EXPLAIN SELECT order_status, COUNT(*) as cnt FROM orders_ctas WHERE order_status LIKE '%PENDING%' GROUP BY order_status ORDER BY cnt DESC;


