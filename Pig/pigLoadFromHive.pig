customer_details = LOAD 'xademo.customer_details' USING org.apache.hive.hcatalog.pig.HCatLoader();
top10 = LIMIT customer_details 10;
DUMP top10;

