categories = LOAD 'retail_db.categories' USING org.apache.hive.hcatalog.pig.HCatLoader();
--OR
--categories = LOAD '/user/root/sqoop-import' USING PigStorage(',') AS (cat_id:int, cat_dep_id:int, cat_name:chararray);
ordered = ORDER categories BY $1;
DUMP ordered;
