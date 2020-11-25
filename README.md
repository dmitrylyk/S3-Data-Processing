# S3-Data-Processing  

#### The real time processing of streaming data application, that works via Spark Streaming.  
The new data that appears in the AWS S3 bucket will be uploaded through Spark Streaming, processed and then it will be written in AWS RDS Database.

AWS S3 Bucket looks like:
![Image of s3](https://github.com/dmitrylyk/S3-Data-Processing/blob/main/img/s3_info.png)

Example records JSON will look as follows:
<br/>
{“user”: “A”, “timestamp”:”2017-12-19 10:41:49”, “spend”:”1.99”,”evtname”:”iap”}  
{“user”: “B”, “timestamp”:”2017-12-20 11:22:18”, ”evtname”:”tutorial”}  
{“user”: “A”, “timestamp”:”2017-12-20 18:20:10”, “spend”:”9.99”,”evtname”:”iap”}  

Spark Streaming processing log:  
![Image of Spark log](https://github.com/dmitrylyk/S3-Data-Processing/blob/main/img/spark_log.png)

Output (AWS RDS Database):  
table users  
![Image of table users](https://github.com/dmitrylyk/S3-Data-Processing/blob/main/img/tableusers_info.png)  
table revenue  
![Image of users](https://github.com/dmitrylyk/S3-Data-Processing/blob/main/img/tablerevenue_info.png)
