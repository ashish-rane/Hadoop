orders = LOAD 'retail_db.orders' USING org.apache.hive.hcatalog.pig.HCatLoader();
order_items = LOAD 'retail_db.order_items' USING org.apache.hive.hcatalog.pig.HCatLoader();


order_order_items_left_outer_join = JOIN orders BY order_id LEFT OUTER, order_items BY order_item_order_id;
order_order_items_left_outer_join_filtered = FILTER order_order_items_left_outer_join BY order_items::order_item_order_id IS NULL;
DUMP order_order_items_left_outer_join_filtered ;

orders_group = GROUP  order_order_items_left_outer_join_filtered ALL;
counted = FOREACH orders_group GENERATE COUNT_STAR(order_order_items_left_outer_join_filtered);
DUMP counted;


