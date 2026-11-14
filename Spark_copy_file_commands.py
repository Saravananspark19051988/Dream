
Step 1: Local machine → Dataproc VM
C:\pyspark\employee.csv
gcloud compute scp employee.csv username@hadoop-clust-m:/home/username/
Now the file is available on the Dataproc master VM: :    /home/username/employee.csv

Step 2: Dataproc VM → HDFS
SSH = Secure Shell into your Dataproc master:  (Think of SSH as a remote connection to another computer.)
After SSH connects successfully, your terminal is no longer executing commands on your Windows laptop. You are now interacting with the remote Linux machine.
pwd
It displays your current directory on the Dataproc master.
gcloud compute ssh hadoop-clust-m
Now Execute : hadoop fs -put /home/username/employee.csv /tmp/spark_demo_datasets/
The file is now copied into HDFS:  /tmp/spark_demo_datasets/employee.csv

Step 3: Verify the file in HDFS : hadoop fs -ls /tmp/spark_demo_datasets/
Step 4: Read the file using PySpark
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs:///tmp/spark_demo_datasets/employee.csv")

df.show()