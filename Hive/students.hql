CREATE TABLE students (name varchar(64), age int, gpa decimal(3,2))
CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC;

INSERT INTO TABLE students VALUES ('fred flinstone', 35,1.28), ('barney rubble', 32,2.32);

