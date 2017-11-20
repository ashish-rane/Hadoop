CREATE TABLE pageviews (userid varchar(64), link string, came_from string)
PARTITIONED BY (datestamp string) CLUSTERED BY (userid) INTO 4 BUCKETS STORED AS TEXTFILE;

// Static partition
INSERT INTO pageviews PARTITION(datestamp='2014-09-23') VALUES ('jsmith', 'mail.com', 'sports.com'), ('jdoe', 'mail.com',null);

// Dynamic partition
INSERT INTO pageviews PARTITION(datestamp) VALUES ('tjohnson', 'sports.com', 'finance.com', '2014-09-23'), ('tlee', 'finance.com', null, '2014-09-21');

// CREATE another table with same schema as pageviews but no data yet
CREATE TABLE empty_pageviews_copy LIKE pageviews;
