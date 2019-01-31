#!/usr/bin/env bash

hadoop fs -rm -r hdata/genderCountry
mysql -uroot -pcloudera < maketable.sql
sqoop import --connect jdbc:mysql://localhost/test --username root --password cloudera --table gender_series --columns TableName,ShortName,LongName,Region,IncomeGroup --target-dir /user/cloudera/hdata/genderCountry -m 1
hive -f loadSqoop.sql