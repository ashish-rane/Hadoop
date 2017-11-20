
orders = LOAD 'retail_db.orders' USING org.apache.hive.hcatalog.pig.HCatLoader();
order_items = LOAD 'retail_db.order_items' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- This JOIN by default performs a reduce side join
order_order_items_join = JOIN  orders BY order_id, order_items BY order_item_order_id;


-- Order Totals per Order
--order_order_items_transform = FOREACH order_order_items_join GENERATE orders::order_id AS order_id, order_items::order_item_subtotal AS subtotal;
--DESCRIBE order_order_items_transform;
--orders_group = GROUP order_order_items_transform BY order_id;
--orders_totals = FOREACH orders_group GENERATE group AS order_id, SUM(order_order_items_transform.subtotal) AS subtotal;
--ILLUSTRATE  orders_totals;


-- Order Totals per day
order_subtotal_by_date = FOREACH order_order_items_join GENERATE orders::order_date AS order_date, order_items::order_item_subtotal AS subtotal;
order_grouped_by_date = GROUP order_subtotal_by_date BY order_date;
order_totals_by_date = FOREACH order_grouped_by_date GENERATE group AS order_date, SUM(order_subtotal_by_date.subtotal) AS totals;
--DUMP order_totals_by_date;
ILLUSTRATE order_totals_by_date;
