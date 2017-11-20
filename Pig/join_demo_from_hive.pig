
orders = LOAD 'retail_db.orders' USING org.apache.hive.hcatalog.pig.HCatLoader();
order_items = LOAD 'retail_db.order_items' USING org.apache.hive.hcatalog.pig.HCatLoader();

join_orders = COGROUP orders BY order_id, order_items BY order_item_order_id;

join_filtered = FILTER join_orders BY group is not null;

totalAmount = FOREACH join_filtered GENERATE group as order_id, SUM(order_items.order_item_subtotal) AS total;

limit10 = limit totalAmount  10;
DUMP limit10;
