departments = LOAD '/user/root/sqoop_import/departments/' USING PigStorage(',') AS (dep_id: int, dep_name: chararray);
STORE departments INTO 'pig_demo.Departments' USING org.apache.hive.hcatalog.pig.HCatStorer();

categories = LOAD '/user/root/sqoop_import/categories/' USING PigStorage(',') AS (cat_id:int, cat_dep_id:int, cat_name:chararray);
STORE categories INTO 'pig_demo.Categories' USING org.apache.hive.hcatalog.pig.HCatStorer();

