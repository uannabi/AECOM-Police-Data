---
description: Database creation Using the Police UK API (Home | data.police.uk)
---

# AECOM

Using the Police UK API ([Home | data.police.uk](https://data.police.uk/)) extract the data related to street-level crime and outcome data as well as the nearest police stations.

Create a single database that will contain all the street-level crime and outcome data with the nearest police station that has investigated the crimes and remove all the data that have a missing outcome while highlighting which authorities do not provide an outcome.

&#x20;

Present a sample of your data with the code and a brief document explaining your thought process for the technologies you have selected.

Data overview: From API we have received the following data:

```python
// Data overview
data=spark.read.csv('s3a://S3Path/AECOM/*.csv',header=True)
>>> data.printSchema()                                                          
root
 |-- Crime ID: string (nullable = true)
 |-- Month: string (nullable = true)
 |-- Reported by: string (nullable = true)
 |-- Falls within: string (nullable = true)
 |-- Longitude: string (nullable = true)
 |-- Latitude: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- LSOA code: string (nullable = true)
 |-- LSOA name: string (nullable = true)
 |-- Outcome type: string (nullable = true)
 
 data.show()
+--------------------+-------+--------------------+--------------------+---------+---------+--------------------+---------+----------------+--------------------+
|            Crime ID|  Month|         Reported by|        Falls within|Longitude| Latitude|            Location|LSOA code|       LSOA name|        Outcome type|
+--------------------+-------+--------------------+--------------------+---------+---------+--------------------+---------+----------------+--------------------+
|ca8988a7d8a0afca0...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.159064|51.471708|On or near CULVER...|E01004539| Wandsworth 003A|Investigation com...|
|a74cebf4835d4b1b7...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.152554|51.462119|On or near JEDBUR...|E01004587| Wandsworth 009D|Investigation com...|
|ed8064d19fa5025a9...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.181235|51.465984|On or near Shoppi...|E01033101| Wandsworth 004F|Investigation com...|
|5e4c547ce14c7c14d...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.194113|51.457065|On or near BUCKHO...|E01004509| Wandsworth 010A|Investigation com...|
|28ae93768cdca5e3d...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.170370|51.428424|On or near GARRAT...|E01004616| Wandsworth 034C|Investigation com...|
|771fe80e771d47fd4...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.166804|51.427667|On or near Shoppi...|E01004525| Wandsworth 035A|Investigation com...|
|ce9cdda1c69dd6ea7...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.166154|51.458636|On or near MALLIN...|E01004552| Wandsworth 008C|Investigation com...|
|4eacfc320cf781b05...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.238820|51.446990|On or near HORNDE...|E01004571| Wandsworth 023C|Investigation com...|
|22dc129c5167d0404...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.152367|51.480299|On or near Carria...|E01033098| Wandsworth 002F|Investigation com...|
|db76993b6a2df563c...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.117666|51.514044|         On or near |E01004735|Westminster 018B|Investigation com...|
|2dc578ace39da67d1...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.143092|51.515753|On or near John P...|E01004765|Westminster 013D|Investigation com...|
|dd926255a69f82d39...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.175663|51.448677|On or near Wilde ...|E01004624| Wandsworth 024E|Investigation com...|
|f175db0c7483bb8df...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.158182|51.479302|         On or near |E01004560| Wandsworth 001A|Investigation com...|
|501dbc5805d3ba4d1...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.155550|51.454945|On or near Broxas...|E01004477| Wandsworth 017A|Investigation com...|
|f9f2481f9c24a7982...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.194972|51.458625|On or near Armour...|E01004509| Wandsworth 010A|Investigation com...|
|c4fd85cc1ce7790ef...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.133701|51.475030|On or near Deeley...|E01003089|    Lambeth 008A|Investigation com...|
|2416e7ccb3d9bd5e2...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.142148|51.479085|On or near Savona...|E01033132| Wandsworth 002H|Investigation com...|
|2416e7ccb3d9bd5e2...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.142148|51.479085|On or near Savona...|E01033132| Wandsworth 002H|Investigation com...|
|2416e7ccb3d9bd5e2...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.142148|51.479085|On or near Savona...|E01033132| Wandsworth 002H|Investigation com...|
|222b6875cafe2ebda...|2022-12|Metropolitan Poli...|Metropolitan Poli...|-0.166287|51.456381|On or near Bellev...|E01004557| Wandsworth 015B|Investigation com...|
+--------------------+-------+--------------------+--------------------+---------+---------+--------------------+---------+----------------+--------------------+
```

To create a single database that contains all the street-level crime and outcome data with the nearest police station that investigated the crimes and remove all data that have a missing outcome, we can follow these general steps:

1. Create a database in a database management system (DBMS) of our choices, such as MySQL, PostgreSQL, or SQLite.
2. Create tables to store the crime data, police station data and outcome data. Using following of the SQL code for creating tables for this data:

```sql
// Creating Table for database
CREATE TABLE crimes (
  id VARCHAR(255) NOT NULL,
  month DATE,
  reported_by VARCHAR(255),
  falls_within VARCHAR(255),
  longitude FLOAT,
  latitude FLOAT,
  location VARCHAR(255),
  lsoa_code VARCHAR(255),
  lsoa_name VARCHAR(255),
  PRIMARY KEY (id)
);

CREATE TABLE police_stations (
  id INT NOT NULL,
  name VARCHAR(255),
  longitude FLOAT,
  latitude FLOAT,
  PRIMARY KEY (id)
);

CREATE TABLE outcomes (
  crime_id VARCHAR(255) NOT NULL,
  outcome_type VARCHAR(255),
  PRIMARY KEY (crime_id),
  FOREIGN KEY (crime_id) REFERENCES crimes(id)
);

```

* Import the crime data, police station data, and outcome data into their respective tables in the database.
* Use a geocoding service or software to obtain the longitude and latitude of each police station, if they are not already available in the data. This will allow us to calculate the distance between each crime and the police station.
* Calculate the distance between each crime and the police station using a spatial query. Here's a SQL code for finding the nearest police station for each crime:

```sql
// Query to calculate the distance between each crime and the police station using 
// SQL spatial query:
SELECT c.id, p.id as station_id, 
  ST_Distance_Sphere(POINT(c.longitude, c.latitude), POINT(p.longitude, p.latitude)) as distance
FROM crimes c, police_stations p
ORDER BY c.id, distance
LIMIT 1;
```

This query finds the distance between each crime and the police station and then orders the results by crime ID and distance. The `LIMIT 1` clause returns only the nearest police station for each crime.

6. By joining the crime data with the outcome data and the nearest police station data using SQL joins. Here's a  SQL code for joining the crime and outcome data:

```sql
// Query for the outcome and the nearest police station 
SELECT c.*, o.outcome_type
FROM crimes c
LEFT JOIN outcomes o ON c.id = o.crime_id
WHERE o.outcome_type IS NOT NULL;
```

This query returns all crimes with a valid outcome type.

7. If there are any missing outcome types, we can identify the authorities that do not provide an outcome by grouping the data by the "Reported by" field and counting the number of missing outcomes for each authority. Here's a SQL code for doing this:

```sql
// Query 
SELECT "Reported by", COUNT(*) as num_missing_outcomes
FROM crimes c
LEFT JOIN outcomes o ON c.id = o.crime_id
WHERE o.outcome_type IS NULL
GROUP BY "Reported by";
```

This query groups the data by the "Reported by" field, counts the number of missing outcomes for each authority and returns the results.

Here are the general steps if we create a PostgreSQL database that contains all the street-level crime and outcome data with the nearest police station that investigated the crimes and removes all data that have a missing outcome:

1. Set up a PostgreSQL server and connect to it using a tool such as pgAdmin or psql.
2. Create a new database to hold the crime data using the CREATE DATABASE statement.
3. Create a table to hold the crime data using the CREATE TABLE statement, specifying the column names and data types.
4. Import the crime data into the table using the COPY command or an ETL tool such as Apache NiFi or Talend.
5. Create a second table to hold the police station data, specifying the column names and data types.
6. Import the police station data into the table using the COPY command or an ETL tool.
7. Add a column to the crime table to hold the nearest police station ID, using the ALTER TABLE statement.
8. Use a spatial extension such as PostGIS to calculate the nearest police station for each crime, using a query such as:

```sql
// SQL QEURY
UPDATE crime_table
SET police_station_id = (
  SELECT id
  FROM police_station_table
  ORDER BY ST_Distance(crime_table.geom, police_station_table.geom)
  LIMIT 1
);
```

9. Create a third table to hold the outcome data, specifying the column names and data types.
10. Import the outcome data into the table using the COPY command or an ETL tool.
11. Add a foreign key to the crime table that references the outcome table, using the ALTER TABLE statement.
12. Remove all rows from the crime table that have a missing outcome using the DELETE statement and a subquery such as:

```sql
// SQL QUERY
DELETE FROM crime_table
WHERE outcome_id NOT IN (SELECT id FROM outcome_table);

```

13. Optionally, add a column to the outcome table that indicates which authorities do not provide an outcome, using a CASE statement and a query such as:

ALTER TABLE outcome\_table ADD COLUMN missing\_outcome BOOLEAN DEFAULT FALSE;

UPDATE outcome\_table SET missing\_outcome = TRUE WHERE outcome\_type IS NULL;

```sql
// SQL QUERY
ALTER TABLE outcome_table
ADD COLUMN missing_outcome BOOLEAN DEFAULT FALSE;

UPDATE outcome_table
SET missing_outcome = TRUE
WHERE outcome_type IS NULL;

```

These are very high-level steps, but the specific details may vary depending on the exact format of the input data, the tools and extensions used, and the desired schema and data processing requirements.
