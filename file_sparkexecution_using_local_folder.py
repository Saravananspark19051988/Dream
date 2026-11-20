#This code execute pyspark code using local folder
import os
import sys

python_exe = sys.executable

os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

print("Python Interpreter:", python_exe)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, when # case Statment
from pyspark.sql.window import Window  # windows functions
 



spark = SparkSession.builder \
    .appName("Local_PySpark") \
    .master("local[*]") \
    .getOrCreate()

local_path ="D:\My_Learning_Dream\Grow\PySpark\cust.csv"
# Read local CSV
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(local_path)

df.printSchema()
df.show(5)


df.select("CustomerID","FirstName").show(5)
df.select(col("CustomerID"),col("FirstName")).show(5)
df.select(col("CustomerID").alias("CustID"),col("FirstName").alias("Name")).show(5)
df1= df.withColumn("Year",year(col("DateOfBirth")))
df2= df.withColumn("Year",year(col("DateOfBirth"))).withColumn("Mon",month(col("DateOfBirth")))
df2.select("Year","Mon",  col("CustomerID").alias("CustID"),col("FirstName").alias("Name"), ).show(5)
df2.show(5)
df3= df2.withColumnRenamed("LastName","Lname")
df3.show(5)
df3.filter(col("BranchID") == 115).show(5)
#df3.filter(col("BranchID") == 115).count()


DateOfBirt = ["2002-04-27","2005-04-25"]
df3.filter(col("DateOfBirth").isin(DateOfBirt)).show(5)
df3.filter(col("DateOfBirth").isin(["2002-04-27","2005-04-25"])).show(5)

df3.filter( (col("Year")>=1954) & (col("Year")<=1966)   ).show(5)

df3.filter( 'Year >=1954  AND Year<=1966'   ).show(5)
df3.drop("Mon").show()

print("original_count:",df3.count())
df4=df3.dropDuplicates()
print("After removing count D4 count:",df4.count())
print("After Specific column duplicates",df4.dropDuplicates(["FirstName","Lname","Year"]).count())
df5= df4.dropDuplicates(["FirstName","Lname","Year"])
df5.orderBy(["FirstName","Lname","Year"]).show(20)
df5.orderBy(col("FirstName").desc(),col("Lname").asc(),col("Year").asc()).show(20)

df5.groupby('Year').agg(
        count('*').alias('Total_count'),
        sum('CustomerID').alias('Sum of cust Id'),
        avg('CustomerID').alias('Avg of cust Id'),
        min('CustomerID').alias('Min of cust Id'),
        max('CustomerID').alias('Max of cust id')
        ).show(5)
        
#Accumulator
amounts = [100, 200, -50, 300, -20, 400, 150]
sc = spark.sparkContext

positive_amounts = sc.accumulator(0)
negative_amounts = sc.accumulator(0)

def amount_check(row):
    amount = row
    if amount > 0:
       positive_amounts.add(1)
    if amount < 0:
        negative_amounts.add(1)
       

rdd= sc.parallelize(amounts)
rdd.foreach(amount_check)

print("Positive Amount:",positive_amounts.value)
print("Negative Amount:",negative_amounts.value)



sum_custid = sc.accumulator(0)
df5.foreach(lambda x: sum_custid.add(x['CustomerID']))
print(sum_custid.value)


###  Case when

df5.show(5)

df5.withColumn ( 
    "limits", 
    when(col("Year") <= 1980 ,"Full Limits")
    .when( (col("Year") > 1980) & (col("Year") <= 2000) ,"25%")
    .when( (col("Year") > 2000) & (col("Year") <= 2015) ,"50%")
                            .otherwise("75%")  ).show(20)
from pyspark.sql.window import Window
windspec = Window.partitionBy(col("Year")).orderBy(col("FirstName").asc())
df5.withColumn("dense_rank", dense_rank().over(windspec)).show(20)
df5.withColumn("dense_rank", dense_rank().over(windspec)).select('CustomerID','FirstName','dense_rank').show(20)


windspec1 =Window.partitionBy(col("Year")).orderBy(col("FirstName").asc())
df5.withColumn("running_sum", sum("BranchID").over(windspec1)).show(5)
df5.withColumn("running_sum", sum("BranchID").over(windspec1)).select('CustomerID','FirstName','running_sum').show(5)
#from pyspark.sql.functions import col, when

#df5.withColumn(
#    "limits",
#    when(col("Year") <= 1980, "Full Limits")
#    .when((col("Year") > 1980) & (col("Year") <= 2000), "25%")
#    .when((col("Year") > 2000) & (col("Year") <= 2015), "50%")
#    .otherwise("75%")
#).show(20)




local_path1 = r"D:\My_Learning_Dream\Grow\PySpark\loans.csv"
# Read local CSV
dfl = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(local_path1)

print("loans CSV")
dfl.printSchema()
dfl.show(5)


local_path1 = r"D:\My_Learning_Dream\Grow\PySpark\loans.csv"

# Read local CSV
dfl = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(local_path1)

print("Loans CSV")

dfl.printSchema()
print("new :")
dfl.show(5)

#result = df5.join(dfl,df5.CustomerID==dfl.CustomerID,'inner').drop(dfl.CustomerID)
#result.printSchema()
#result.show(5)

result = df5.alias('old').join(dfl.alias('new'),col("old.CustomerID")==col("new.CustomerID"),'inner').drop(col("new.CustomerID"))
result.printSchema()
result.show(5)


df5.createOrReplaceTempView("cust")
dfl.createOrReplaceTempView("orderstatus")

joindf = spark.sql("select * from cust old INNER join orderstatus new on old.CustomerID=new.CustomerID ")
joindf.printSchema
joindf.show()
output_path = r"D:\My_Learning_Dream\Grow\PySpark\output\result"

result.write \
    .format("csv") \
    .mode("overwrite") \
    .option('header',"true") \
    .option('delimiter',",") \
    .save(output_path)
    
    
## Save as Parquet locally
#df.write \
#    .mode("overwrite") \
#    .parquet("D:/My_Learning_Dream/Grow/PySpark/output/orders")
#
spark.stop()
