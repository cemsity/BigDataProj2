#!/bin/bash

hadoop fs -rm -r gender-data/
hadoop fs -put gender-data/

mysql -uroot -pcloudera < createCountryTable.sql
mysql -uroot -pcloudera < createDataTable.sql

sqoop import --connect jdbc:mysql://localhost/test --username root --password cloudera --table gender_country --columns CountryCode,TableName,ShortName,LongName,Region,IncomeGroup --target-dir /user/cloudera/hdata/genderCountry --delete-target-dir -m 1
sqoop import --connect jdbc:mysql://localhost/test --username root --password cloudera --table gender_data --target-dir /user/cloudera/hdata/genderData --delete-target-dir -m 1 --enclosed-by \"

hive -f loadData.sql
hive -f loadCountry.sql
hive -f execQueries.sql