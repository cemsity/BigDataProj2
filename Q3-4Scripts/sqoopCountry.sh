#!/usr/bin/env bash

mysql -uroot -pcloudera < createTable.sql
sqoop import --connect jdbc:mysql://localhost/test --username root --password cloudera --table gender_series --columns CountryCode,TableName,ShortName,LongName,Region,IncomeGroup --target-dir /user/cloudera/hdata/genderCountry --delete-target-dir -m 1
hive -f loadSqoop.sql
