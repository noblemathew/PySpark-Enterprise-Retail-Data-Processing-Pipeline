from datetime import datetime

# today's date in YYYY_MM_DD format
today = datetime.today().strftime("%Y_%m_%d")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Enterprise Retail Pipeline") \
    .getOrCreate()
print("Spark Session Started")

df = spark.read.csv(f"../data/raw/sales_{today}.csv",header=True,inferSchema=True)

df.show()

df.printSchema()

import re

df = df.toDF(
    *[
        re.sub(r"_+", "_", col.strip().lower())
          .replace(" ", "_")
          .replace("#", "id")
          .strip("_")
        for col in df.columns
    ]
)

print("Headers cleaned successfully")

print("Before removing duplicates:",df.count())

df = df.dropDuplicates() #removing duplicates

print("After removing duplicates:",df.count())

print("Before removing nulls, row count:",df.count())

df = df.dropna() #dropping all nulls

print("After removing nulls, row count:",df.count())

#removing invalid values
from pyspark.sql.functions import col

df = df.filter(
    (col("order_quantity") > 0) &
    (col("revenue") > 0)
)
print("Invalid values removed")
print("After invalid values removed, row count:",df.count())
df.show()

#Creating new column to find Profir Margin
from pyspark.sql.functions import col, round

df = df.withColumn(
    "profit_margin",
    round((col("profit") / col("revenue")) * 100, 2)
)
df.show(truncate=False)
print("Created Profit Mrgin Column")

#Cleaning the Age Group column to only categories
from pyspark.sql.functions import regexp_replace, trim

# Update the existing dataframe
df = df.withColumn(
    "age_group",
    trim(regexp_replace("age_group", r"\s*\(.*?\)", ""))
)
df.show(truncate=False)

#Creating high value column to check if the order has high or low value
from pyspark.sql.functions import when

df = df.withColumn(
    "high_value_order",
    when(
        col("revenue") > 5000,
        "YES"
    ).otherwise("NO")
)

from pyspark.sql.functions import regexp_replace, trim

# Remove text inside brackets from state column
df = df.withColumn(
    "state",
    trim(regexp_replace("state", r"\s*\(.*?\)", ""))
)
df.show(truncate=False)

from pyspark.sql.functions import when, col

# Update customer_gender column
df = df.withColumn(
    "customer_gender",
    when(col("customer_gender") == "M", "Male")
    .when(col("customer_gender") == "F", "Female")
    .otherwise("null")
)
df.show(truncate=False)

from pyspark.sql.functions import to_date

# Convert string column to DateType
df = df.withColumn(
    "date",
    to_date("date", "MM/dd/yyyy")
)
df.printSchema()

from pyspark.sql.functions import (
    col,
    to_date,
    format_string,
    dayofmonth,
    date_format,
    year
)
# Keep original day and update only year and month
df = df.withColumn(
    "date",
    to_date(
        format_string(
            "2026-05-%02d",
            dayofmonth(col("date"))
        )
    )
)

# Update month and year columns

df = (
    df.withColumn(
        "month",
        date_format(col("date"), "MMMM")
    )
    .withColumn(
        "year",
        year(col("date"))
    )
)

print("Date, month, and year columns updated successfully")

print("Completed Cleaning job")

#KPI GENERATION

#finding the total revenue
from pyspark.sql.functions import sum

total_revenue = df.select(
    sum("revenue")
).collect()[0][0]

print("Total Revenue:", total_revenue)

#finding the total profit
total_profit = df.select(
    sum("profit")
).collect()[0][0]
print("Total Profit:", total_profit)

#finding the total number of rows
total_orders = df.count()
print("Total Orders:",total_orders)

#finding the average revenue generated and rounding the decimal to 2
from pyspark.sql.functions import avg, round
average_revenue = df.select(
    round(avg("revenue"), 2).alias("avg_revenue")
).collect()[0]["avg_revenue"]
print("Average Revenue:", average_revenue)

#finding the max revenue generated
from pyspark.sql.functions import max
max_revenue = df.select(
    max("revenue")
).collect()[0][0]
print("Maximum Revenue Generated:",max_revenue)

#finding the most frequent product by order by descending and showing the 1st row
most_frequent_product = df.groupBy(
    "product_description"
).count().orderBy(
    col("count").desc()
)
most_frequent_product.show(1)

top_product = most_frequent_product.collect()[0][0]
#retreiving the value from order by
print("Most Frequent Product:", top_product)

#Generating the final KPI's for visualization
kpis = {
    "total_revenue": total_revenue,
    "total_profit": total_profit,
    "total_orders": total_orders,
    "average_revenue": average_revenue,
    "most_frequent_product": top_product
}

kpis

#Creating gender based sale evaluvation
gender_sales = {
    row["customer_gender"]: row["sum(revenue)"]
    for row in df.groupBy(
        "customer_gender"
    ).sum(
        "revenue"
    ).collect()

}

#Creating country based sale evaluvation
country_sales = {
    row["country"]: row["sum(revenue)"]
    for row in df.groupBy(
        "country"
    ).sum(
        "revenue"
    ).collect()

}

country_sales

#Creating age group based sale evaluvation
age_group_sales = {
    row["age_group"]: row["sum(revenue)"]
    for row in df.groupBy(
        "age_group"
    ).sum(
        "revenue"
    ).collect()

}

from pyspark.sql.functions import col, sum as spark_sum

day_sales_dict = {

    row["day"]: row["total_sales"]

    for row in df.groupBy(

        "day"

    ).agg(

        spark_sum("revenue").alias("total_sales")

    ).orderBy(

        "day"

    ).collect()

}

print(day_sales_dict)

age_group_sales

#Generating final analytics data ready for visualization
analytics_data = {
    "kpis": kpis,
    "gender_sales": gender_sales,
    "country_sales": country_sales,
    "age_group_sales": age_group_sales,
    "day_sales":day_sales_dict
}

analytics_data

print("Analytics Data Generated!")

# Saving cleaned parquet dataset

# Saving cleaned CSV dataset

df.coalesce(1).write.mode("overwrite").option(

    "header",

    True

).csv(

    f"../data/processed/cleaned_sales_{today}"

)

print("[INFO] Cleaned CSV dataset saved successfully")


import json

with open(

    "../visualize/analytics.json",

    "w"

) as file:

    json.dump(

        analytics_data,

        file,

        indent=4

    )

print("[INFO] Analytics JSON generated successfully")


