val ordersRDD = sc.textFile("/user/hive/warehouse/retail_db.db/orders");

val ordersFiltered = ordersRDD.filter(x => { val parts =  x.split(","); parts match { case Array(a,b,c,d) => (a,b,c,d); d.equals("COMPLETE")  } })
