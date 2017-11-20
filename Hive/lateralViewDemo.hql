use website;

CREATE TABLE pageAds (	pageid string, adid_list ARRAY<int>)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


INSERT INTO TABLE pageads
 SELECT 'front_page', array(1,2,3)
 UNION ALL
 SELECT 'contact_page', array(3,4,5);


// CREATE LATERAL VIEW
SELECT pageid, adid
FROM pageads LATERAL VIEW explode(adid_list) adTable as adid;

// NUmber of times an ad appears
SELECT adid, count(*)
FROM pageads LATERAL VIEW explode(adid_list) adTable AS adid
GROUP BY adid;

