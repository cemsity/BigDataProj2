#!/usr/bin/env bash

hadoop fs -rm -r gender-data/
hadoop fs -put gender-data/
hive -f tohive.sql
