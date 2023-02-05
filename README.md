# MsftStockAnalysis
Assumption :
********************************************************** 
1.Stocks Trade data Files will be received daily with all updated values with present day data.
2. Mysql sever is intalled and can accesed.

**********************************************************
Notes : 
 
1. SPARK  
	A :- PySpark Application is written to read the file and upadte the data in mysql.

   	B :- For application logs config files can be passed at spark submit or can be added in spark-defaults.
		# Add below lines in spark-defaults if don't want to add in spark submit :
		spark.driver.extraJavaOptions      -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=msft-stocks
	C :- The name of the log file will be msft.log , It will be created under app-logs. And the logs behaviour defined in log4j.properties

To run the project and test cases simple open the project in Intellij IDE preconfigured with pyspark, and pytest 

To run then code on cluster using spark-submit , msftmain.py file along with dependent py files and lib , needs to passed as zip.

spark-submit  --master yarn --deploy-mode cluster \
--driver-memory 2g --executor-memory 1g --num-executors 3 --executor-cores 4  \
--py-files libs.zip \
--jars mysql-connector-j-8.0.32.jar
MsftStockMain.py filepath:msft.csv



2. Mysql 
Application specific data base and table script need to be executed.
A:- Create Database in my sql 
create database msft_database;

B:- Create table to store stocks data
create table daily_stocks_data (date Date,
open_price double,
high_price double,
low_price double,
close_price double,
adj_close_price double,
volume bigint)

**********************************************************
