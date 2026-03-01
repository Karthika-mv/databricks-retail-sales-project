# Databricks notebook source
df = (spark.read.format("csv")
        .option("header" , 'true')
        .option("inferSchema","true")
        .load("/Volumes/workspace/demo/raw-data/orders_csv.csv")
    )
df.show()

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.demo.orders_bronze")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.demo")
spark.sql("SHOW SCHEMAS IN workspace").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM workspace.demo.orders_bronze;

# COMMAND ----------

bronze_df = spark.table("workspace.demo.orders_bronze")
bronze_df.display()

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

silver_df = bronze_df.dropna()
silver_df = silver_df.dropDuplicates()

# COMMAND ----------

# Add Useful Derived Columns
from pyspark.sql.functions import year, month, weekofyear
silver_df = (
    silver_df.withColumns({
        "year" : year("date"),
        "month" : month("date"),
        "weekofyear" : weekofyear("date")
    })
)
silver_df.display()

# COMMAND ----------

# Save Silver Table
silver_df.write.format("delta").mode("overwrite").saveAsTable("workspace.demo.orders_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.demo.orders_silver

# COMMAND ----------

# Sales Category Classification
from pyspark.sql.functions import when ,col
silver_df = (
    silver_df.withColumn("Sales_Category",when(col("Weekly_Sales") > 1000000,'High')
    .when(silver_df.Weekly_Sales > 500000,'Medium')
    .otherwise("Low"))
)

# COMMAND ----------

# Rolling Weekly Average
from pyspark.sql.functions import avg , round
from pyspark.sql.window import Window
window_spec = Window.partitionBy("Store").orderBy("Date").rowsBetween(Window.unboundedPreceding,0)

silver_df = silver_df.withColumn(
    "Rolling_Avg" , round(avg("Weekly_Sales").over(window_spec),2)
)
silver_df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC alter table workspace.demo.orders_silver add columns(
# MAGIC   Sales_Category string,
# MAGIC   Rolling_Avg double
# MAGIC )

# COMMAND ----------

silver_old = (spark.read.format("delta") 
    .option("versionAsOf", 2)  
    .table("workspace.demo.orders_silver"))

silver_old.show()

# COMMAND ----------

# Gold Table 1 — Store Performance
from pyspark.sql.functions import avg,sum
from pyspark.sql.functions import sum 
gold_store = (
    silver_df.groupBy("Weekly_Sales")
    .agg(
        sum("Weekly_Sales").alias("Total_Sales"),
        avg("Weekly_Sales").alias("Avg_Sales")
    )
)


# COMMAND ----------

gold_store.write.format("delta").saveAsTable("workspace.demo.gold_store_sales")
spark.table("workspace.demo.gold_store_sales").display()

# COMMAND ----------

# Gold Table 2 — Monthly Sales Trend
from pyspark.sql.functions import round
gold_month = (
    silver_df.groupBy("year","month")
    .agg(
        round(sum("Weekly_Sales"),2).alias("Monthly_Sales")
    )
)
gold_month.write.format("delta").mode("overwrite").saveAsTable("workspace.demo.gold_monthly_sales")

spark.table("workspace.demo.gold_monthly_sales").display()

# COMMAND ----------

# Gold Table 3 — Holiday Impact
gold_holiday = (
    silver_df.groupBy("Holiday_Flag")
    .agg(
        round(sum("Weekly_Sales"),2).alias("Total_sales")
    )
)
gold_holiday.write.format("delta").mode("overwrite").saveAsTable("workspace.demo.gold_holiday_impact")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from workspace.demo.gold_holiday_impact