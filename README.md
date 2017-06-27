# Import Data into Dataframe
This project is done in Spark using the Dataframes and Spark SQL to perform an analysis on a Marketing Dataset<br>
 
<code>import org.apache.spark.sql.hive._</code><br>
<code>val hc = new HiveContext(sc)</code><br>
<code>case class Person(age: Int, job: String, marital: String, education: String, default: String, balance: Int, Housing:  String, loan: String, contact: String, day: Int, month: String, duration: Int, campaign: Int, pdays: Int, previous: Int, poutcome: String, subscription: String)</code><br>
<code>val myFile = sc.textFile("updateddataset.txt")</code><br>
<code>val df= myFile.map( x => x.split(";") ).map( x=> Person(x(0).trim.toInt,x(1),x(2),x(3),x(4),x(5).trim.toInt,x(6),x(7),x(8),x(9).trim.toInt,x(10),x(11).trim.toInt,x(12).trim.toInt,x(13).trim.toInt,x(14).trim.toInt,x(15),x(16))).toDF()</code><br>
<code>df.registerTempTable("person");</code><br>
<code>val data = sqlContext.sql("SELECT * from person");</code><br>
<code>data.show()</code>
<code>val countsub = sqlContext.sql("SELECT * FROM person WHERE subscription='yes'").count()

