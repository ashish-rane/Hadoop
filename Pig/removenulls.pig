cust = LOAD 'xademo.customer_details' USING org.apache.hive.hcatalog.pig.HCatLoader();

imei_not_null = FILTER cust BY imei is  null;

limit100 = LIMIT imei_not_null 100;
DUMP limit100;
