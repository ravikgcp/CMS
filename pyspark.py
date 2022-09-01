# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("spark") \
      .getOrCreate()


############### Spark Create DataFrame #####################

val spark:SparkSession = SparkSession.builder()
   .master("local[1]").appName("Spark")
   .getOrCreate()

import spark.implicits._
val columns = Seq("language","users_count")
val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
data.show()


############## PySpark Create DF ################


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]

columns = ["first_name","middle_name","last_name","dob","gender","salary"]
pysparkDF = spark.createDataFrame(data = data, schema = columns)
pysparkDF.printSchema()
pysparkDF.show(truncate=False)

############## PySpark Read CSV file into DataFrame ###############


df = spark.read.format("csv")
                  .load("/tmp/resources/zipcodes.csv")
//       or
df = spark.read.format("org.apache.spark.sql.csv")
                  .load("/tmp/resources/zipcodes.csv")
df.printSchema()

-------------

df3 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("/tmp/resources/zipcodes.csv")

df3.write.option("header",True) \
 .csv("/tmp/spark_output/zipcodes")

---------------








############## PySpark Repartition() vs Coalesce() ################


rdd = spark.sparkContext.parallelize((0,20))
print("From local[5]"+str(rdd.getNumPartitions()))

rdd1 = spark.sparkContext.parallelize((0,25), 6)
print("parallelize : "+str(rdd1.getNumPartitions()))

rddFromFile = spark.sparkContext.textFile("src/main/resources/test.txt",10)
print("TextFile : "+str(rddFromFile.getNumPartitions()))


From local[5] : 5
Parallelize : 6
TextFile : 10

      
      
