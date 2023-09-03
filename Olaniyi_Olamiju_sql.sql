-- Databricks notebook source
--checking the uploaded/created tables in the default database

Select * from default.clinicaltrial_2021 Limit (3)

-- COMMAND ----------

Select * from default.pharma Limit (3)

-- COMMAND ----------

--create a new database named 'clinicaltrial_db'

CREATE DATABASE IF NOT EXISTS clinicaltrial_db

-- COMMAND ----------

--move the uploaded 'clinicaltrial_2021' table from the default database into the new database 'clinicaltrial_db'

CREATE OR REPLACE TABLE clinicaltrial_db.clinicaltrial_2021 AS SELECT * FROM default.clinicaltrial_2021

-- COMMAND ----------

--move the uploaded pharma table from the default database into the new database 'clinicaltrial_db'

CREATE OR REPLACE TABLE clinicaltrial_db.pharma AS SELECT * FROM default.pharma

-- COMMAND ----------

--checking to confirm that the 'clinicaltrial_2021' table is in the new database 

SHOW TABLES IN clinicaltrial_db

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PART 1 OF TASK 1

-- COMMAND ----------

--select the 'Id' column and count the number of distinct studies in the table
SELECT COUNT (DISTINCT (Id)) AS Total_Number_Of_Studies FROM clinicaltrial_db.clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PART 2 OF TASK 1

-- COMMAND ----------

--select the 'Type' column and a count column containing the number of occurrences for each type

SELECT Type, COUNT(Type) AS Frequencies 
FROM clinicaltrial_db.clinicaltrial_2021
WHERE Type IN ('Interventional', 'Observational', 'Observational [Patient Registry]', 'Expanded Access') 
GROUP BY Type
ORDER BY COUNT(Type) DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PART 3 OF TASK 1

-- COMMAND ----------

--creating a view of an array of elements in the 'Conditions' column of 'clinicaltrial_2021' table
CREATE OR REPLACE TEMP VIEW Conditions_view AS SELECT split(Conditions, ',') AS split_conditions FROM clinicaltrial_db.clinicaltrial_2021

-- COMMAND ----------

SELECT * FROM Conditions_view LIMIT 10

-- COMMAND ----------

--the sql function explode in the code below takes out the array of elements in the Conditions_view created above and separates them into unique rows for better performance, thereby creating another view.

CREATE OR REPLACE TEMP VIEW exploded_conditions AS SELECT explode(split_conditions) AS new_split FROM Conditions_view

-- COMMAND ----------

SELECT * FROM exploded_conditions LIMIT 10

-- COMMAND ----------

--select the 'new_split' column and a count column containing the number of occurrences for each condition from the exploded_conditions view above, group by new_split and arrange in descending order.

SELECT new_split AS Conditions, COUNT(new_split) AS Frequencies 
FROM exploded_conditions
GROUP BY new_split
ORDER BY COUNT(new_split) DESC LIMIT(5)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PART 4 OF TASK 1
-- MAGIC

-- COMMAND ----------

--The query below removes all elements in the Parent_Company column of pharma table matching any elements in the Sponsor column of the clinicaltrial_2021 table, groups the remaining elements in the Sponsor column and arranges them in descending order.

SELECT 
  Sponsor, COUNT(Sponsor) AS Frequency 
FROM 
  clinicaltrial_db.clinicaltrial_2021
WHERE 
  Sponsor NOT IN (SELECT Parent_Company FROM clinicaltrial_db.pharma WHERE Parent_Company IS NOT NULL) 
GROUP BY 
  Sponsor 
ORDER BY 
  Frequency DESC LIMIT (10)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC PART 5 OF TASK 1

-- COMMAND ----------

--The query below creates a temporary view named view_completedstudies based on the clinicaltrial_2021 table in the clinicaltrial_db database. The resulting view will show the number of completed studies per month in the year 2021.

--The SUBSTRING function takes the first 3 characters of the Completion column and renames the resulting column as Month.

--The COUNT function counts the number of rows for each group of Month and renames the resulting column as Completed_Studies_Per_Month_Per_Year.

--The WHERE clause filters the rows to only include those where Completion ends with "2021" and Status is "Completed".

--The GROUP BY clause groups the rows by Month.

--The ORDER BY clause orders the output by Month, but it first converts Month to a number using a CASE statement that maps each month to a corresponding number from 1 to 12.

CREATE OR REPLACE TEMP VIEW view_completedstudies AS SELECT SUBSTRING(Completion, 1, 3) AS Month, COUNT(*) 
AS Completed_Studies_Per_Month_Per_Year FROM clinicaltrial_db.clinicaltrial_2021
WHERE Completion LIKE '%2021' AND Status = 'Completed'
GROUP BY Month
ORDER BY CASE
    WHEN Month = 'Jan' THEN 1
    WHEN Month = 'Feb' THEN 2
    WHEN Month = 'Mar' THEN 3
    WHEN Month = 'Apr' THEN 4
    WHEN Month = 'May' THEN 5
    WHEN Month = 'Jun' THEN 6
    WHEN Month = 'Jul' THEN 7
    WHEN Month = 'Aug' THEN 8
    WHEN Month = 'Sep' THEN 9
    WHEN Month = 'Oct' THEN 10
    WHEN Month = 'Nov' THEN 11
    WHEN Month = 'Dec' THEN 12
END

-- COMMAND ----------

SELECT * FROM view_completedstudies

-- COMMAND ----------

-- MAGIC %md
-- MAGIC FURTHER ANALYSIS 3

-- COMMAND ----------

--Selecting unique pharmaceutical (Parent) companies whose secondary offense is related to 'bribery' and are headquartered in Illinois, USA from the pharma table, arranged in descending order

SELECT DISTINCT Parent_Company, Secondary_Offense
FROM clinicaltrial_db.pharma
WHERE Secondary_Offense Like '%bribery%'
      AND HQ_Country_of_Parent = 'USA'
      AND HQ_State_of_Parent = 'Illinois'
ORDER BY Parent_Company desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC FURTHER ANALYSIS 4

-- COMMAND ----------

--To find the number of subsidiary (child) companies belonging to each pharmarceutical parent company and sorting the count column in descending order.
SELECT Parent_Company, COUNT(Parent_Company) AS Number_of_Child_Companies
FROM clinicaltrial_db.pharma
GROUP BY Parent_Company
ORDER BY COUNT(Parent_Company) DESC
