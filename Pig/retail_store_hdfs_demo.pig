--departments = LOAD '/user/root/sqoop_import/departments' USING PigStorage(',') AS (department_id:int, department_name:chararray);
--departments = LOAD '/user/root/sqoop_import/departments' USING BinStorage('|');
--STORE departments INTO '/user/root/departments' USING PigStorage('|');
--STORE departments INTO '/user/root/departments' USING BinStorage(',');
--STORE departments INTO '/user/root/departments' USING JsonStorage();

departmentsBin = LOAD '/user/root/departments' USING BinStorage(',');
DESCRIBE departmentsBin;

DUMP departmentsBin;
