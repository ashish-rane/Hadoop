departments = LOAD 'sqoop_temp_test1/departments/' USING PigStorage(',') AS (dept_id:int, dept_name:chararray);

STORE departments INTO 'department_test' USING org.apache.hive.hcatalog.pig.HCatStorer();

