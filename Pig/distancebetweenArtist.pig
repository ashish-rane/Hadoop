REGISTER '/hirw-workshop/pig/songs/datafu-0.0.4.jar';
DEFINE haversineMiles datafu.pig.geo.HaversineDistInMiles();

songs = LOAD '/user/hirw/input/songs' AS (artist_familiarity:double, artist_lat:double, artist_long:double, artist_name:chararray, song_hotness:double, song_title:chararray);

-- keep only less popular artist with great songs
less_popular = FILTER songs BY (artist_familiarity is NOT NULL AND artist_familiarity < 0.5) AND (song_hotness is NOT NULL AND song_hotness > 0.5)
                                        AND artist_lat is NOT NULL AND artist_long is NOT NULL;

-- an artist might have more than one song, so group by artist
grp_less_popular = GROUP less_popular BY (artist_familiarity, artist_lat, artist_long, artist_name);


--an artist might have more than one song, so find  avg hotness to avoid duplicates
avg_song_hotness = FOREACH grp_less_popular GENERATE group.artist_familiarity, group.artist_lat, group.artist_long, group.artist_name, AVG(less_popular.song_hotness) AS avgsong_hotness;

-- we further filter this by having artist only average song hotness above 0.5
only_hot_song_artist = FILTER avg_song_hotness BY avgsong_hotness > 0.5;

only_hot_song_artist2 = FOREACH only_hot_song_artist GENERATE artist_familiarity AS artist_familiarity2, artist_lat AS artist_lat2, artist_long AS artist_long2, artist_name as artist_name2, avgsong_hotness AS avgsong_hotness2;

-- to get distance of each artist from every other, we first generate cross product of the relation with itself and then filter the records where left and right side are same.
-- in order to differentiate between left and right side we project the existing relation into a new one with columun names appended with 2.
crossed = CROSS only_hot_song_artist, only_hot_song_artist2;

only_different = FILTER crossed BY artist_name != artist_name2 AND artist_lat != artist_lat2 AND artist_long != artist_long2;

-- use haversine miles function to calculate distance between two artist
distance = FOREACH only_different GENERATE only_hot_song_artist::artist_lat AS artist_lat, only_hot_song_artist::artist_long as artist_long, only_hot_song_artist::artist_name AS artist_name, only_hot_song_artist::artist_familiarity as artist_familiarity, only_hot_song_artist::avgsong_hotness AS avgsong_hotness, 
haversineMiles(only_hot_song_artist::artist_lat, only_hot_song_artist::artist_long, only_hot_song_artist2::artist_lat2, only_hot_song_artist2::artist_long2) AS distance;

-- group and calculate teh avg distance for each artist from others
group_artist= GROUP distance BY artist_lat..avgsong_hotness;
avg_distance = FOREACH group_artist GENERATE flatten(group), AVG(distance.distance) AS avgDistance;


-- order and pickup the top 1
desc_order = ORDER avg_distance BY avgDistance DESC;
top1 = LIMIT desc_order 1;

-- Write the result in google map format
part2 = FOREACH top1 GENERATE
	CONCAT('[', (chararray)artist_lat), artist_long, CONCAT('"', CONCAT((chararray)artist_name, '"')),
	artist_familiarity, CONCAT((chararray)avgsong_hotness, '],');

STORE part2 INTO 'output/pig/farthestArtist01' USING PigStorage(',');
