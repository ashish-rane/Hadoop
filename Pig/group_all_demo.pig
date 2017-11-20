customer_details_hive = LOAD 'xademo.customer_details' USING org.apache.hive.hcatalog.pig.HCatLoader();
customer_details_noheader = FILTER customer_details_hive BY phone_number != 'PHONE_NUM';
customer_details_grp_all = GROUP customer_details_noheader ALL;
--ILLUSTRATE customer_details_grp_all;
customer_details_rec_count = FOREACH customer_details_grp_all GENERATE COUNT_STAR(customer_details_no_header) AS cnt;
ILLUSTRATE customer_details_rec_count;


