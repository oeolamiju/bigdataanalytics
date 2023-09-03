# Databricks notebook source


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC CREATING A REUSABLE CODE FOR UNZIPPING THE FILES

# COMMAND ----------

#first step here is to write reusable code to unzip both the clinicaltrial and the pharma zipped files
#we assign a variable called 'fileroot' to any of the name of the clinicaltrial_year .zip files
fileroot = "clinicaltrial_2021"
dbutils.fs.cp("/FileStore/tables/" + fileroot + ".zip", "file:/tmp/")

# COMMAND ----------

#making the variable 'fileroot' accessible by the command line
import os
os.environ['fileroot'] = fileroot

# COMMAND ----------

#using the linux command to unzip the .zip file
%sh
unzip -d /tmp /tmp/$fileroot.zip

# COMMAND ----------

#creating a directory in DBFS

dbutils.fs.mkdirs("/FileStore/tables/" + fileroot)

# COMMAND ----------

#moving the unzipped file to created directory in DBFS

dbutils.fs.mv("file:/tmp/" + fileroot + ".csv", "/FileStore/tables/" + fileroot, True)

# COMMAND ----------

#checking to confirm the csv (unzipped) file
dbutils.fs.ls("/FileStore/tables/" + fileroot)


# COMMAND ----------

#Now it's time to start exploring the clinicaltrial_2021 data
dbutils.fs.head("/FileStore/tables/clinicaltrial_2021/clinicaltrial_2021.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC PART 1 OF TASK 1

# COMMAND ----------

#creating the first RDD for the analysis
Rdd = sc.textFile('/FileStore/tables/clinicaltrial_2021')

# COMMAND ----------

# Extracting the header
header = Rdd.first()
Rdd1 = Rdd.filter(lambda row: row != header)

# COMMAND ----------

# Count the number of distinct records in myrdd1
distinct_studies_count = Rdd1.distinct().count()

# Therefore, total number of distinct studies in clinicaltrial_2021 is
print("Number of distinct studies:", distinct_studies_count)

# COMMAND ----------

# Therefore, total number of distinct studies in clinicaltrial_2021 is
print("Number of distinct studies:", distinct_studies_count)

# COMMAND ----------

# MAGIC %md
# MAGIC PART 2 OF TASK 1

# COMMAND ----------

#Assign the column index to be analyzed
type_column = 6

# Map each row to a tuple of the Type column and 1, then reduce by key to count the occurrences
Rdd2 = Rdd1.map(lambda row: row.split("|")[type_column-1]).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

# Sort the type counts by value in descending order
sorted_type_counts = Rdd2.sortBy(lambda x: x[1], ascending=False)
for count in sorted_type_counts.collect():
    print(f"{count[0]}: {count[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC PART 3 OF TASK 1

# COMMAND ----------

# Assign the column index to be analyzed
conditions_column = 8
# Map each row to a tuple of the Conditions column and 1, then reduce by key to count the occurrences
Rdd3 = Rdd1.map(lambda row: row.split("|")[conditions_column-1])
#Remove all the white spaces and empty elements from the mapped 'Conditions' column
Rdd4 = Rdd3.flatMap(lambda x: filter(lambda y: y.strip() != "", x.split(",")))

# COMMAND ----------

#Map each row to a tuple of the Conditions column and 1, then reduce by key to count the occurrences
Rdd5 = Rdd4.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
# Sort the conditions counts by value in descending order and take the top 5
sorted_conditions_counts = Rdd5.sortBy(lambda x: x[1], ascending=False)
for count in sorted_conditions_counts.take(5):
    print(f"{count[0]}: {count[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC PART 4 OF TASK 1

# COMMAND ----------

#Assigning 'pharma' to 'fileroot' Unzip the pharma.zip file using the same code used for to unzip the clinicaltrial_2021.zip

fileroot = "pharma"
dbutils.fs.cp("/FileStore/tables/" + fileroot + ".zip", "file:/tmp/")

# COMMAND ----------

#Now it's time to start exploring the pharma data
dbutils.fs.head("/FileStore/tables/pharma.csv")

# COMMAND ----------

#creating an rdd with the unzipped pharma.csv file

Rdd6 = sc.textFile('/FileStore/tables/pharma.csv')

# COMMAND ----------

# Extract the header of Rdd6
header = Rdd6.first()
Rdd7 = Rdd6.filter(lambda row: row != header)

# COMMAND ----------

#Assign the recommended column index from the pharma file
Parent_Company_column = 2

# Map each row to a tuple of the Parent_Company column and 1, then reduce by key to count the occurrences
Rdd8 = Rdd7.map(lambda row: row.split(',')[Parent_Company_column-1]).distinct()
Rdd8.collect()

# COMMAND ----------

# Extracting the header from Rdd1
header = Rdd1.first()
Rdd9 = Rdd1.filter(lambda row: row != header)

# COMMAND ----------

# Assign the target column index from Rdd1 (clinicaltrial_2021)
sponsor_column = 2
# Map each row to a tuple of the Sponsors column and 1, then reduce by key to count the occurrences
Rdd10 = Rdd9.map(lambda row: row.split("|")[sponsor_column-1])

# COMMAND ----------

#List all elements in Rdd8 (pharma)
rdd8_list = Rdd8.collect()
#Match the list above against elements in Rdd8 and filter out any elements common to both Rdds. The remaining list should be the list of non-pharceutical sponsors
Rdd11 = Rdd10.filter(lambda x: x not in rdd8_list)

# COMMAND ----------

#Map each row remaining in Rdd9 to a tuple, then reduce by key to count the occurrences
Rdd12 = Rdd11.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

# COMMAND ----------

# Sort the non-pharmaceutical sponsors counts by value in descending order and take the top 10

sorted_sponsors_counts = Rdd12.sortBy(lambda x: x[1], ascending=False)
for count in sorted_sponsors_counts.take(10):
    print(f"{count[0]}: {count[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC PART 5 OF TASK 1

# COMMAND ----------

# Assign the column index to be analyzed
status_column = 3
completion_column = 5
# Map each row to a tuple of the Status column and Completion column
Rdd13 = Rdd.map(lambda row: (row.split("|")[status_column-1], row.split("|")[completion_column-1]))

# COMMAND ----------

#Extract the header from Rdd15
header = Rdd13.first()
Rdd13 = Rdd13.filter(lambda x: x != header)

# COMMAND ----------

# Importing required modules
from pyspark.sql.functions import year, month

# Define the list of months in 2021 and the corresponding month numbers
months_2021 = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
months_dict = dict(zip(months_2021, range(1, 13)))

# Filtering the RDD to get only records with status='Completed' and year=2021
completed_2021_rdd = Rdd13.filter(lambda x: x[0] == 'Completed' and '2021' in x[1])

# Creating an RDD with tuples of (month, 1) for each completed trial in 2021
completed_2021_month_rdd = completed_2021_rdd.map(lambda x: (months_dict[x[1][0:3]], 1))

# Reducing by key to count the number of completed trials for each month in 2021
completed_2021_freq_rdd = completed_2021_month_rdd.reduceByKey(lambda x, y: x + y)

# Re-arrange the months to start from Jan and end at Sep
completed_2021_freq_sorted = sorted(completed_2021_freq_rdd.collect(), key=lambda x: x[0])

# Print the results from Jan to Sep
for month, freq in completed_2021_freq_sorted[:9]:
    print(f"Completed trials in {months_2021[month-1]} 2021: {freq}")


# COMMAND ----------

# MAGIC %md
# MAGIC FURTHER ANALYSIS 1

# COMMAND ----------

# Importing required modules
from pyspark.sql.functions import year, month
# Define the list of months in 2021 and the corresponding month numbers
months_2021 = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
months_dict = dict(zip(months_2021, range(1, 13)))
# Filtering the RDD to get only records with status='Completed' and year=2021
completed_2021_rdd = Rdd13.filter(lambda x: x[0] == 'Terminated' and '2021' in x[1])
# Creating an RDD with tuples of (month, 1) for each completed trial in 2021
completed_2021_month_rdd = completed_2021_rdd.map(lambda x: (months_dict[x[1][0:3]], 1))
# Reducing by key to count the number of completed trials for each month in 2021
completed_2021_freq_rdd = completed_2021_month_rdd.reduceByKey(lambda x, y: x + y)
# Re-arrange the months to start from Jan and end at Sep
completed_2021_freq_sorted = sorted(completed_2021_freq_rdd.collect(), key=lambda x: x[0])
# Print the results from Jan to Sep
for month, freq in completed_2021_freq_sorted[:9]:
    print(f"Terminated trials in {months_2021[month-1]} 2021: {freq}")
