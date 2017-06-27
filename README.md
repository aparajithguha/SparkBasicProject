# Simple Spark Project
This project is done in Spark using the Dataframes and Spark SQL to perform an analysis on a Marketing Dataset<br>
 # Import Data into Dataframe
 <pre>import org.apache.spark.sql.hive._
val hc = new HiveContext(sc)
case class Person(age: Int, job: String, marital: String, education: String, default: String, balance: Int, Housing:  String, loan: String, contact: String, day: Int, month: String, duration: Int, campaign: Int, pdays: Int, previous: Int, poutcome: String, subscription: String)
val myFile = sc.textFile("updateddataset.txt")
val df= myFile.map( x => x.split(";") ).map( x=> Person(x(0).trim.toInt,x(1),x(2),x(3),x(4),x(5).trim.toInt,x(6),x(7),x(8),x(9).trim.toInt,x(10),x(11).trim.toInt,x(12).trim.toInt,x(13).trim.toInt,x(14).trim.toInt,x(15),x(16))).toDF()
df.registerTempTable("person");
val data = sqlContext.sql("SELECT * from person");
data.show()</pre>

# Success and Failure Rate
<pre>val countsub = sqlContext.sql("SELECT * FROM person WHERE subscription='yes'").count()
val notsub=sqlContext.sql("SELECT * FROM person WHERE subscription='no'").count()
val data = sqlContext.sql("SELECT * from person").count()
val a = data.toFloat
val b = countsub.toFloat
val c = notsub.toFloat
print("people subscription rate:" +((b)/a))
print("market failure rate:" +((c)/a))</pre>
<br>

# Maximum, Mean, and Minimum age of average targeted customer
<code>df.agg(avg("age"),max("age"),min("age")).show()</code><br>

# Check quality of customers by checking average balance, median balance of customers
<pre>sqlContext.sql("select percentile(balance, 0.5) median from person").show()
df.agg(avg("balance")).show()</pre><br>

# Creating Scala UDF function
create a simple scala UDF function for age category to created a new age group category and add it to the Data frame<br>
<code><b>import org.apache.spark.sql.functions.udf</b></code><br>
<pre>def ageToCategory = udf((age:Int) => {
      age match {
      case t if t < 30 => "young"
      case t if t > 65 => "old"
      case _ => "mid"   } } )</pre><br>
      
<code><b>ageToCategory: org.apache.spark.sql.UserDefinedFunction</b></code><br>
<b>Query</b><br>
<code>newdf.where(newdf("subscription")==="yes").groupBy("agecategory").count().sort($"count".desc).show</code><br>

# marital check, age and marital, age

<code>df.where(df("subscription")==="yes").groupBy(df("marital")).agg(count("marital")).show()</code><br>

<code>newdf.where(newdf("subscription")==="yes").groupBy("agecategory","marital").count().sort($"count".desc).show</code><br>

<code>newdf.groupBy("agecategory","subscription").count().sort($"count".desc).show</code><br>

      
  
