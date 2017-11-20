orders = LOAD 'retail_db.orders' USING org.apache.hive.hcatalog.pig.HCatLoader();
order_items = LOAD 'retail_db.order_items' USING org.apache.hive.hcatalog.pig.HCatLoader();
order_order_item_join = COGROUP orders by order_id, order_items by order_item_order_id;
DESCRIBE order_order_item_join;
test_flattent = FOREACH order_order_item_join GENERATE group, flatten(orders), flatten(order_items);

DESCRIBE test_flattent;
--orders_subtotal = FOREACH test_flattent GENERATE group, SUM(order_items::order_item_subtotal) AS subtotal;
--DUMP orders_subtotal;


orders_subtotal = FOREACH order_order_item_join GENERATE  group, SUM(order_items.order_item_subtotal) AS subtotal;
DESCRIBE orders_subtotal;
--DUMP orders_subtotal;
--EXPLAIN orders_subtotal;



