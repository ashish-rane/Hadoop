orders = LOAD 'retail_db.orders' USING org.apache.hive.hcatalog.pig.HCatLoader();
order_items = LOAD 'retail_db.order_items' USING org.apache.hive.hcatalog.pig.HCatLoader();


join_orders = JOIN orders BY order_id, order_items BY order_item_order_id;

grp_orders = GROUP join_orders BY orders::order_date;

totalsperday = FOREACH grp_orders GENERATE group AS date, SUM(join_orders.order_items::order_item_subtotal) as total;

limit10 = LIMIT totalsperday 10;

DUMP limit10;


