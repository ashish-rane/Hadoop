import sys
import datetime

for line in sys.stdin:
	line = line.strip()
	user_id, movie_id, rating, unixtime = line.split('\t')
	weekday = datetime.datetime.fromtimestamp(float(unixtime)).isweekday()
	print '\t'.join([user_id, movie_id, rating, str(weekday)])

