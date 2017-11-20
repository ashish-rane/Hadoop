set default_parellel 4;

orders = LOAD 'retail_db.orders' USING org.apache.hive.hcatalog.pig.HCatLoader();
grp_by_status = GROUP orders BY order_status;
order_cnt_by_status = FOREACH grp_by_status GENERATE group, COUNT(orders.order_id);
DUMP order_cnt_by_status;

