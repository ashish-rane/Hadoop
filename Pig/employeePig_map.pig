departments = LOAD '/user/hirw/input/employee-pig/department_dataset_chicago' using PigStorage(';') AS (dept_id:int, dept_name:chararray, address:map[]);

dept_addr = FOREACH departments GENERATE dept_name, address#'street' as street, address#'city' as city, address#'state' as state;

DUMP dept_addr;
