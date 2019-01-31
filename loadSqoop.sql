use proj2;

drop table gender_country;

create table gender_country (
	TableName string,
	ShortName string,
	LongName string,
	Region string,
	IncomeGroup string
)
row format delimited
fields terminated by ','
STORED AS TEXTFILE;

load data inpath '/user/cloudera/hdata/genderCountry/part-m-00000' into table gender_country;