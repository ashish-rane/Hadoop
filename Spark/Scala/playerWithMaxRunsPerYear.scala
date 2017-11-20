
case class Batting(player:String, year:Int, runs:Int);

val batting = sc.textFile("/user/root/pigInputs/batting.csv");

val battingRDD = batting.map(x => {val parts = x.split(","); parts match { case Array(a,b,c) =>  ( c.toInt, Batting(a, b.toInt, c.toInt)) } } );

val runsYear = battingRDD.map(x => (x._2.year, x._2.runs);

val maxRunsByYear = runsYear.reduceByKey( (acc, value) => { if(acc > value) acc else value   });

val joined = maxRunsByYear.join(battingRDD);

val playerMaxRunsPerYear = joined.map(x => (x._1, x._2._2.player, x._2._2.runs);
val sorted = playerMaxRunsPerYear.sortByKey();


