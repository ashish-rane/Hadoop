customer_details_noschema = LOAD '/apps/hive/warehouse/xademo.db/customer_details' USING PigStorage('|');
DESCRIBE customer_details_noschema;
customer_details_noschema_nonulls = FILTER customer_details_noschema BY $5 is not null;
DUMP customer_details_noschema_nonulls;


--customer_details_hive = LOAD 'xademo.customer_details' USING org.apache.hive.hcatalog.pig.HCatLoader();
--customer_details_hive_nonulls = FILTER customer_details_hive BY imei is not null;
--DUMP customer_details_hive_nonulls;


customer_details_schema = LOAD '/apps/hive/warehouse/xademo.db/customer_details' USING PigStorage('|') 
AS (phone_number:chararray, plan:chararray, rec_date:chararray, status:chararray, balance:chararray, imei:chararray, region:chararray); 
customer_details_schema_nonulls = FILTER customer_details_schema BY imei is not null;
DUMP customer_details_schema_nonulls;

