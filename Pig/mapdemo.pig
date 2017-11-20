departments = LOAD 'datafiles/hirw/empployee-pig/' USING PigStorage(';') AS (dept_id: int, dept_name:chararray, address:map[]);

projection = FOREACH departments GENERATE dept_id, dept_name, address#'street' AS street, address#'city' AS city, address#'state' AS state;

DUMP projection;
