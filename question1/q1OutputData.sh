#!/bin/bash

# drop the mysql tables
mysql -uroot -pcloudera <<DESTORY_TABLE
use country_db;
drop table country_graduate_ratios;
drop table region_graduate_ratios;
drop table income_group_graduate_ratios;
drop database country_db;
DESTORY_TABLE

# create the mysql tables
mysql -uroot -pcloudera <<CREATE_TABLE
create database country_db;
use country_db;
create table country_graduate_ratios(country_name varchar(100), country_code varchar(5), region_name varchar(50), income_group_name varchar(25), graduate_ratio decimal(30, 15));
create table region_graduate_ratios(region_name varchar(50), number_of_countries int, average_graduate_ratio decimal(30, 15));
create table income_group_graduate_ratios(income_group_name varchar(25), number_of_countries int, average_graduate_ratio decimal(30, 15));
CREATE_TABLE

# sqoop the hive output from hdfs to mysql
sqoop export --connect jdbc:mysql://localhost/country_db --username root --password cloudera --table country_graduate_ratios --export-dir /user/cloudera/outputData/country_graduate_ratios/000000_0 --input-fields-terminated-by '\t' -m 1
sqoop export --connect jdbc:mysql://localhost/country_db --username root --password cloudera --table region_graduate_ratios --export-dir /user/cloudera/outputData/region_graduate_ratios/000000_0 --input-fields-terminated-by '\t' -m 1
sqoop export --connect jdbc:mysql://localhost/country_db --username root --password cloudera --table income_group_graduate_ratios --export-dir /user/cloudera/outputData/income_group_graduate_ratios/000000_0 --input-fields-terminated-by '\t' -m 1