REGISTER '/hirw-workshop/pig/songs/datafu-0.0.4.jar';
DEFINE haversineMiles datafu.pig.geo.HaversineDistInMiles();

songs = LOAD '/user/hirw/input/songs' AS (artist_familiarity:double, artist_lat:double, artist_long:double, artist_name:chararray, song_hotness:double, song_title:chararray);

-- keep only less popular artist with great songs
less_popular = FILTER songs BY (artist_familiarity is NOT NULL AND artist_familiarity < 0.5) AND (song_hotness is NOT NULL AND song_hotness > 0.5) 
					AND artist_lat is NOT NULL AND artist_long is NOT NULL;

-- an artist might have more than one song, so group by artist
grp_less_popular = GROUP less_popular BY (artist_familiarity, artist_lat, artist_long, artist_name);


--an artist might have more than one song, so find  avg hotness to avoid duplicates
avg_song_hotness = FOREACH grp_less_popular GENERATE flatten(group), AVG(less_popular.song_hotness) AS avgsong_hotness;

-- we further filter this by having artist only average song hotness above 0.5
only_hot_song_artist = FILTER avg_song_hotness BY avgsong_hotness > 0.5;

-- Generate output in Google map format
part1 = FOREACH only_hot_song_artist GENERATE 
	CONCAT ('[', (chararray)artist_lat),artist_long, CONCAT('"', CONCAT((chararray)artist_name, '"')),
	artist_familiarity, CONCAT((chararray)avgsong_hotness, '],');

STORE part1 INTO 'output/pig/lesspopular01' USING PigStorage(',');
