set default_parellel 4;

orders = LOAD '/user/root/sqoop_import/orders/' USING PigStorage(',');
statuses = FOREACH orders GENERATE $3 AS order_status;
distinct_statuses = DISTINCT statuses;
distinct_statuses_ordered = ORDER distinct_statuses BY order_status;
DUMP distinct_statuses_ordered;

