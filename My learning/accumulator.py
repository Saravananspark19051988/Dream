from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.appName(Accumulator Example") \
	.getOrCreate()
	
	
sc =spark.sparkContext
data = [15, 20, 25, 12, 30]

#Imagine you have 5 customer records. You want to count how many records have a valid age (age greater than or equal to 18).

valid_age = sc.accumulator(0)
Invalid_age = sc.accumulator(0)

def valid_age(row):
	age = row
	if age >= 18:
		valid_age.add(1)
	if age < 18:
		Invalid_age.add(1)
		

rdd= sc.parallelize(data)
rdd.foreach(valid_age)
print("valid_age:",valid_age.value)
print("Invalid_age:",Invalid_age.value)


amounts = [100, 200, -50, 300, -20, 400, 150]

sc = spark.sparkContext
positive_amt = sc.accumulator(0)
negative_amt = sc.accumulator(0)
def transactions(row):
	amt = row
	if amt < 0:
		negative_amt.add(amt)
	elif am > 0:
		positive_amt.add(amt)

rdd= sc.parallelize(amounts)

rdd.foreach(transactions)

print("Positive Amount:",positive_amt.value)
print("Negative Amount:",negative_amt.value)



amounts = [100, 200, -50, 300, -20, 400, 150]

sc=spark.sparkContext
positive_amt = sc.accumulator(0)
negative_amt = sc.accumulator(0)

rdd= sc.parallelize(amounts)

rdd.foreach(lambda row: positive_amt.add(amounts) if row >0 else None)
rdd.foreach(lambda row: negative_amt.add(amounts) if row <0 else None)
print("Positive Amount:",positive_amt.value)
print("Negative Amount:",negative_amt.value)