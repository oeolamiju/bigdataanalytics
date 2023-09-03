# Databricks notebook source
#checking to confirm that the unzipped csv file is in the filestore 

dbutils.fs.ls("/FileStore/tables/clinicaltrial_2021.csv")

# COMMAND ----------

#passing the filepath into a reusable variable 'clinicaltrial_analysis_filepath'

clinicaltrial_analysis_filepath = "/FileStore/tables/clinicaltrial_2021.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC PART 1 OF TASK 1

# COMMAND ----------

#read the .csv file (clinicaltrial_analysis_filepath) above into a spark dataframe and declare the header as true.

clinicaltrial_df = spark.read.format("csv").option("header", "true").option("delimiter", "|").load(clinicaltrial_analysis_filepath)

# COMMAND ----------

#select the 'Id' column and count the number of distinct records (studies) in the dataframe (clinicaltrial_df) created above

total_distinct_studies = clinicaltrial_df.select('Id').distinct().count()
print("Number of studies in the 2021 dataset:", total_distinct_studies)

# COMMAND ----------

# MAGIC %md
# MAGIC PART 2 OF TASK 1

# COMMAND ----------

#import the count and desc funtions from pyspark.sql
from pyspark.sql.functions import count, desc

#count each type of study in the 'Type' column and group by Type, then arrange in descending order.
clinicaltrial_count_df = clinicaltrial_df.groupBy('Type').agg(count('*').alias('count')).orderBy(desc('count'))

clinicaltrial_count_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC PART 3 OF TASK 1

# COMMAND ----------

#import the explode and split funtions from pyspark.sql
from pyspark.sql.functions import explode, split
#select the 'Conditions' column, split it using the comma as delimiter, group the number of occurence of each condition in the column and arrange in descending order.
top_5_conditions = clinicaltrial_df.select(explode(split("Conditions", ",")).alias("condition")) \
    .groupBy("condition") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(5)
top_5_conditions.show()

# COMMAND ----------

# MAGIC %md
# MAGIC PART 4 OF TASK 1

# COMMAND ----------

#read the pharma.csv file in the FileStore into a spark dataframe, declare the header as true.
pharma_df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/pharma.csv")

# COMMAND ----------

#import the col, array, when and array_remove funtions from pyspark.sql
from pyspark.sql.functions import col, array, when, array_remove
#select the 'Parent_Company' column from the 'pharma_df' dataframe, group the data by "Parent_Company" and count of occurrences for each group. Then sort the resulting DataFrame in descending order by the count column.
pharma_df_Parent_column = pharma_df.select(col('Parent_Company')).groupBy('Parent_Company').count().sort(col('count').desc()).select(col('Parent_Company'), col('count'))
#create another dataframe by selecting only the 'Parent_Company' column from the 'pharma_df_Parent_column' dataframe
pharma_df_new = pharma_df_Parent_column.select("Parent_Company")
#create a nee dataframe and group the data by "Sponsor" column, and count of number of occurrences for each group.
clinicaltrial_df_sponsorcount = clinicaltrial_df.groupby("Sponsor").count()
#A full join operation is used to bring both columns i.e. Sponsor and Parent_company, from their respective dataframes, then every element in 'pharma_df_new' matching elements in 'clinicaltrial_df_sponsorcount' is filtered out to create a new dataframe 'clinicaltrial_df_sponsor' which is then counted and sorted by the count column in descending order.
clinicaltrial_df_sponsor = clinicaltrial_df.groupby("Sponsor").count()
clinicaltrial_df_sponsorcount.join(pharma_df_new, clinicaltrial_df_sponsorcount.Sponsor == pharma_df_new.Parent_Company, "full").select(col("Parent_Company"), col("Sponsor"), col("count")).filter(col("Parent_Company").isNull()).sort(col("count").desc()).select(col("Sponsor"), col("count")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC PART 5 OF TASK 1

# COMMAND ----------



# COMMAND ----------

#filter out studies as 'completed' in the [Status] column and corresponds to year '2021' in the [Completion] column.
clinicaltrial_df_count = clinicaltrial_df.filter((col('Completion').contains("2021")) & (col("Status").contains("Completed")))

#group and sort the counted elements resulting from the dataframe (clinicaltrial_df_count) above
completed_studies_per_month_per_year = clinicaltrial_df_count.groupBy('Completion').count().sort(col("Completion"))
completed_studies_per_month_per_year.show()

# COMMAND ----------

# MAGIC %md
# MAGIC FURTHER ANALYSIS 2

# COMMAND ----------

#filter out completed studies in the Status column that are interventional and corresponds to year '2021' in the [Completion] column.
clinicaltrial_df_count_interventional = clinicaltrial_df.filter((col('Completion').contains("2021")) & (col("Status").contains("Completed")) & (col("Type").contains("Interventional")))
#group and sort the counted elements resulting from the dataframe (clinicaltrial_df_count_interventional) above
completed_studies_and_interventional_per_month_per_year = clinicaltrial_df_count_interventional.groupBy('Completion').count().sort(col("Completion"))
completed_studies_and_interventional_per_month_per_year.show()
