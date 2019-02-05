use project2;

drop table gender_country;

create table gender_country (
	CountryCode string,
	TableName string,
	ShortName string,
	LongName string,
	Region string,
	IncomeGroup string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE;

load data inpath '/user/cloudera/hdata/genderCountry/part-m-00000' into table gender_country;