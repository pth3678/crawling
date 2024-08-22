from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HDFS to Database").config("spark.jars", "/home/pth/spark/jars/mysql-connector-j-9.0.0.jar").getOrCreate()

df = spark.read.csv('hdfs://localhost:9000/home/pth/desktop/crawling/products.csv', header=True, inferSchema=True)
db_url = "jdbc:mysql://localhost:3306/pth"
db_properties = {
    "user": "pth1",
    "password": "pth12345",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df.write.jdbc(url=db_url, table="products", mode="overwrite", properties=db_properties)

data_list=df.select('sold').rdd.flatMap(lambda x:x).collect()
for i in range(0,len(data_list)):
    for j in range(0,len(data_list[i])):
        if data_list[i][j]=='K':
            data_list[i]=float(data_list[i][0:j])*1000
            break
        elif data_list[i][j]==' ':
            data_list[i]=float(data_list[i][0:j])
            break
data_list.sort()
max_sold=0
for i in range (-10,0):
    max_sold=max_sold+data_list[i]
new=spark.createDataFrame([('tong 10 sp ban chay nhat','',f'{max_sold}')],['name','price','sold'])
df=df.union(new)
df.show(50)

spark.stop()