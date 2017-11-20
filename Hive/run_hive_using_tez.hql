SET hive.execution.engine=tez;

use retail_db;

SELECT substr(o.order_date, 1, 10) AS orderdate, SUM(oi.order_item_subtotal) revenue_per_day
FROM orders o INNER JOIN  order_items oi ON o.order_id = oi.order_item_order_id 
GROUP BY substr(o.order_date, 1, 10);


EXPLAIN SELECT substr(o.order_date, 1, 10) AS orderdate, SUM(oi.order_item_subtotal) revenue_per_day
FROM orders o INNER JOIN  order_items oi ON o.order_id = oi.order_item_order_id
GROUP BY substr(o.order_date, 1, 10);
