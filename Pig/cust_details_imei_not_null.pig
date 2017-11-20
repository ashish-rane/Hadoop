customer_details_hive = LOAD 'xademo.customer_details' USING org.apache.hive.hcatalog.pig.HCatLoader();
customer_details_noheader = FILTER customer_details_hive BY phone_number != 'PHONE_NUM';

-- NULL COUNT
customer_details_imei_null = FILTER customer_details_noheader BY imei == '';
customer_details_null_grp = GROUP customer_details_imei_null ALL;
customer_details_null_cnt = FOREACH customer_details_null_grp GENERATE COUNT_STAR(customer_details_imei_null) AS cnt;
DUMP customer_details_null_cnt;

-- NOT NULL COUNT
customer_details_imei_not_null = FILTER customer_details_noheader BY imei != '';
customer_details_imei_not_null_grp = GROUP customer_details_imei_not_null ALL;
customer_details_imei_not_null_cnt = FOREACH customer_details_imei_not_null_grp GENERATE COUNT_STAR(customer_details_imei_not_null) AS cnt;
DUMP customer_details_imei_not_null_cnt;
