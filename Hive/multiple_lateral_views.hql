use website;

CREATE TABLE exampleTable(col1 ARRAY<int>, col2 ARRAY<string>)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO TABLE exampleTable
SELECT ARRAY(1,2), ARRAY('a','b','c')
UNION ALL
SELECT ARRAY(3,4), ARRAY('d','e','f');

// explode only col1
SELECT mycol1, col2 FROM exampleTable
LATERAL VIEW explode(col1) myTable1 AS mycol1;

// explode both col1 and then by col2
SELECT mycol1, mycol2 FROM exampleTable
LATERAL VIEW explode(col1) myTable1 AS mycol1
LATERAL VIEW explode(col2) myTable2 AS mycol2;
