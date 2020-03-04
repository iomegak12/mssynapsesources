# Databricks notebook source
# MAGIC %fs
# MAGIC 
# MAGIC ls

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "32e91f26-6199-4325-a72e-7d3ad46a5fe5",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "training-scope", key="dlsauthtoken"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/381a10df-8e85-43db-86e1-8893b075b027/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://data@iomegadatalakestore.dfs.core.windows.net/",
  mount_point = "/mnt/data",
  extra_configs = configs)

# COMMAND ----------

customersDF = spark.read.format("csv").options(
  header = "true", inferschema = "true").load("/mnt/data/customers/customer*.csv")

# COMMAND ----------

productsDF = spark.read.option("multiline", "true").json("/mnt/data/products/*.json")

# COMMAND ----------

ordersDF = spark.read.format("csv").options(
  header = "true", inferschema = "true").load("/mnt/data/orders/orders-*.csv")

# COMMAND ----------

customersDF.printSchema()
productsDF.printSchema()
ordersDF.printSchema()

# COMMAND ----------

customersDF.createOrReplaceTempView("customers")
productsDF.createOrReplaceTempView("products")
ordersDF.createOrReplaceTempView("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS PracticeDB

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS PracticeDB.RawCustomers
# MAGIC USING CSV
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path "/mnt/data/customers/customer*.csv",
# MAGIC   header "true",
# MAGIC   inferSchema "true"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM PracticeDB.RawCustomers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM customers

# COMMAND ----------

def getCustomerType(s):
  if s >= 1 and s <= 1000:
    return "Silver"
  elif s > 1000 and s <= 25000:
    return "Gold"
  else:
    return "Platinum"

# COMMAND ----------

spark.udf.register('getCustomerType', getCustomerType)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT customerid, address, credit, getCustomerType(credit) FROM customers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT o.orderid, o.orderdate, c.fullname, c.address, getCustomerType(c.credit) as customertype, c.status,
# MAGIC   p.title as ProductTitle, p.unitprice * o.units as orderamount, p.unitprice,
# MAGIC   ((p.unitprice * o.units) * p.itemdiscount * 0.01) as discountamount,
# MAGIC   o.billingaddress, o.units, o.remarks
# MAGIC FROM orders o
# MAGIC INNER JOIN customers c on o.customer = c.customerid
# MAGIC INNER JOIN products p on o.product = p.productid

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Learning PySpark Professionally

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC \\( f(\beta)= -Y_t^T X_t \beta + \sum log( 1+{e}^{X_t\bullet\beta}) + \frac{1}{2}\delta^t S_t^{-1}\delta\\)
# MAGIC 
# MAGIC where \\(\delta=(\beta - \mu_{t-1})\\)
# MAGIC 
# MAGIC $$\sum_{i=0}^n i^2 = \frac{(n^2+n)(2n+1)}{6}$$

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val colorsRDD = sc.parallelize(
# MAGIC 	Array(
# MAGIC 		(197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), 
# MAGIC 		(230,245,208), (184,225,134), (127,188,65), (77,146,33)))
# MAGIC 
# MAGIC val colors = colorsRDD.collect()

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC displayHTML(s"""
# MAGIC <!DOCTYPE html>
# MAGIC <meta charset="utf-8">
# MAGIC <style>
# MAGIC 
# MAGIC path {
# MAGIC   fill: yellow;
# MAGIC   stroke: #000;
# MAGIC }
# MAGIC 
# MAGIC circle {
# MAGIC   fill: #fff;
# MAGIC   stroke: #000;
# MAGIC   pointer-events: none;
# MAGIC }
# MAGIC 
# MAGIC .PiYG .q0-9{fill:rgb${colors(0)}}
# MAGIC .PiYG .q1-9{fill:rgb${colors(1)}}
# MAGIC .PiYG .q2-9{fill:rgb${colors(2)}}
# MAGIC .PiYG .q3-9{fill:rgb${colors(3)}}
# MAGIC .PiYG .q4-9{fill:rgb${colors(4)}}
# MAGIC .PiYG .q5-9{fill:rgb${colors(5)}}
# MAGIC .PiYG .q6-9{fill:rgb${colors(6)}}
# MAGIC .PiYG .q7-9{fill:rgb${colors(7)}}
# MAGIC .PiYG .q8-9{fill:rgb${colors(8)}}
# MAGIC 
# MAGIC </style>
# MAGIC <body>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
# MAGIC <script>
# MAGIC 
# MAGIC var width = 960,
# MAGIC     height = 500;
# MAGIC 
# MAGIC var vertices = d3.range(100).map(function(d) {
# MAGIC   return [Math.random() * width, Math.random() * height];
# MAGIC });
# MAGIC 
# MAGIC var svg = d3.select("body").append("svg")
# MAGIC     .attr("width", width)
# MAGIC     .attr("height", height)
# MAGIC     .attr("class", "PiYG")
# MAGIC     .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });
# MAGIC 
# MAGIC var path = svg.append("g").selectAll("path");
# MAGIC 
# MAGIC svg.selectAll("circle")
# MAGIC     .data(vertices.slice(1))
# MAGIC   .enter().append("circle")
# MAGIC     .attr("transform", function(d) { return "translate(" + d + ")"; })
# MAGIC     .attr("r", 2);
# MAGIC 
# MAGIC redraw();
# MAGIC 
# MAGIC function redraw() {
# MAGIC   path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
# MAGIC   path.exit().remove();
# MAGIC   path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
# MAGIC }
# MAGIC 
# MAGIC </script>
# MAGIC   """)

# COMMAND ----------

import requests

# COMMAND ----------

endpoint = "https://iomegatextanalytics.cognitiveservices.azure.com"
sentiment_uri = endpoint + "/text/analytics/v2.1/sentiment"
headers ={"Ocp-Apim-Subscription-Key":"5fcfee9179de48e3b08d70fe36b3aa6d"}

# COMMAND ----------

def get_score(text):
  documents = {'documents' : [
            {'id': 1, 'language': 'en', 'text': text},
            ]}
  response  = requests.post(sentiment_uri, headers=headers, json=documents)
  azure_response = response.json()
  azure_score = 0

  try:
    azure_documents = azure_response['documents']
    azure_score = azure_documents[0]['score']
  except:
    azure_score = -1

  return azure_score

# COMMAND ----------

spark.udf.register('get_score', get_score)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT o.orderid, o.orderdate, c.fullname, c.address, getCustomerType(c.credit) as customertype, c.status,
# MAGIC   p.title as ProductTitle, p.unitprice * o.units as orderamount, p.unitprice,
# MAGIC   ((p.unitprice * o.units) * p.itemdiscount * 0.01) as discountamount,
# MAGIC   o.billingaddress, o.units, o.remarks,
# MAGIC   get_score(o.remarks) as sentiment_score
# MAGIC FROM orders o
# MAGIC INNER JOIN customers c on o.customer = c.customerid
# MAGIC INNER JOIN products p on o.product = p.productid
# MAGIC WHERE c.customerid IN (1,2,3,4,5)

# COMMAND ----------

statement = """SELECT o.orderid, o.orderdate, c.fullname, c.address, getCustomerType(c.credit) as customertype, c.status,
  p.title as ProductTitle, p.unitprice * o.units as orderamount, p.unitprice,
  ((p.unitprice * o.units) * p.itemdiscount * 0.01) as discountamount,
  o.billingaddress, o.units, o.remarks,
  get_score(o.remarks) as sentiment_score
FROM orders o
INNER JOIN customers c on o.customer = c.customerid
INNER JOIN products p on o.product = p.productid"""

results = spark.sql(statement)

results.printSchema()

# COMMAND ----------

display(results)

# COMMAND ----------

results.write.parquet("/mnt/data/optimized-orders-store/data.parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS PracticeDB

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS PracticeDB.ProcessedOrders
# MAGIC USING PARQUET
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path "/mnt/data/optimized-orders-store/data.parquet"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT orderid, sentiment_score
# MAGIC FROM PracticeDB.ProcessedOrders