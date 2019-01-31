use proj2;
drop table gender_data;
drop table gender_data_partition;
drop database proj2;

create database proj2;
use proj2;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.mapred.mode = nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

create table gender_data (
	CountryName STRING,
	CountryCode STRING,
	IndicatorName STRING,
	IndicatorCode STRING,
	Y1960 DECIMAL(15,8),
        Y1961 DECIMAL(15,8),
        Y1962 DECIMAL(15,8),
        Y1963 DECIMAL(15,8),
        Y1964 DECIMAL(15,8),
        Y1965 DECIMAL(15,8),
        Y1966 DECIMAL(15,8),
        Y1967 DECIMAL(15,8),
        Y1968 DECIMAL(15,8),
        Y1969 DECIMAL(15,8),
        Y1970 DECIMAL(15,8),
        Y1971 DECIMAL(15,8),
        Y1972 DECIMAL(15,8),
        Y1973 DECIMAL(15,8),
        Y1974 DECIMAL(15,8),
        Y1975 DECIMAL(15,8),
        Y1976 DECIMAL(15,8),
        Y1977 DECIMAL(15,8),
        Y1978 DECIMAL(15,8),
        Y1979 DECIMAL(15,8),
        Y1980 DECIMAL(15,8),
        Y1981 DECIMAL(15,8),
        Y1982 DECIMAL(15,8),
        Y1983 DECIMAL(15,8),
        Y1984 DECIMAL(15,8),
        Y1985 DECIMAL(15,8),
        Y1986 DECIMAL(15,8),
        Y1987 DECIMAL(15,8),
        Y1988 DECIMAL(15,8),
        Y1989 DECIMAL(15,8),
        Y1990 DECIMAL(15,8),
        Y1991 DECIMAL(15,8),
        Y1992 DECIMAL(15,8),
        Y1993 DECIMAL(15,8),
        Y1994 DECIMAL(15,8),
        Y1995 DECIMAL(15,8),
        Y1996 DECIMAL(15,8),
        Y1997 DECIMAL(15,8),
        Y1998 DECIMAL(15,8),
        Y1999 DECIMAL(15,8),
        Y2000 DECIMAL(15,8),
        Y2001 DECIMAL(15,8),
        Y2002 DECIMAL(15,8),
        Y2003 DECIMAL(15,8),
        Y2004 DECIMAL(15,8),
        Y2005 DECIMAL(15,8),
        Y2006 DECIMAL(15,8),
        Y2007 DECIMAL(15,8),
        Y2008 DECIMAL(15,8),
        Y2009 DECIMAL(15,8),
        Y2010 DECIMAL(15,8),
        Y2011 DECIMAL(15,8),
        Y2012 DECIMAL(15,8),
        Y2013 DECIMAL(15,8),
        Y2014 DECIMAL(15,8),
        Y2015 DECIMAL(15,8),
        Y2016 DECIMAL(15,8)
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

load data inpath '/user/cloudera/gender-data/Gender_StatsData.csv' into table gender_data;

create table gender_data_partition (
	CountryName varchar(100),
	Y1960 DECIMAL(15,8),
        Y1961 DECIMAL(15,8),
        Y1962 DECIMAL(15,8),
        Y1963 DECIMAL(15,8),
        Y1964 DECIMAL(15,8),
        Y1965 DECIMAL(15,8),
        Y1966 DECIMAL(15,8),
        Y1967 DECIMAL(15,8),
        Y1968 DECIMAL(15,8),
        Y1969 DECIMAL(15,8),
        Y1970 DECIMAL(15,8),
        Y1971 DECIMAL(15,8),
        Y1972 DECIMAL(15,8),
        Y1973 DECIMAL(15,8),
        Y1974 DECIMAL(15,8),
        Y1975 DECIMAL(15,8),
        Y1976 DECIMAL(15,8),
        Y1977 DECIMAL(15,8),
        Y1978 DECIMAL(15,8),
        Y1979 DECIMAL(15,8),
        Y1980 DECIMAL(15,8),
        Y1981 DECIMAL(15,8),
        Y1982 DECIMAL(15,8),
        Y1983 DECIMAL(15,8),
        Y1984 DECIMAL(15,8),
        Y1985 DECIMAL(15,8),
        Y1986 DECIMAL(15,8),
        Y1987 DECIMAL(15,8),
        Y1988 DECIMAL(15,8),
        Y1989 DECIMAL(15,8),
        Y1990 DECIMAL(15,8),
        Y1991 DECIMAL(15,8),
        Y1992 DECIMAL(15,8),
        Y1993 DECIMAL(15,8),
        Y1994 DECIMAL(15,8),
        Y1995 DECIMAL(15,8),
        Y1996 DECIMAL(15,8),
        Y1997 DECIMAL(15,8),
        Y1998 DECIMAL(15,8),
        Y1999 DECIMAL(15,8),
        Y2000 DECIMAL(15,8),
        Y2001 DECIMAL(15,8),
        Y2002 DECIMAL(15,8),
        Y2003 DECIMAL(15,8),
        Y2004 DECIMAL(15,8),
        Y2005 DECIMAL(15,8),
        Y2006 DECIMAL(15,8),
        Y2007 DECIMAL(15,8),
        Y2008 DECIMAL(15,8),
        Y2009 DECIMAL(15,8),
        Y2010 DECIMAL(15,8),
        Y2011 DECIMAL(15,8),
        Y2012 DECIMAL(15,8),
        Y2013 DECIMAL(15,8),
        Y2014 DECIMAL(15,8),
        Y2015 DECIMAL(15,8),
        Y2016 DECIMAL(15,8),
	IC varchar(20)
)
partitioned by (IndicatorCode varchar(20))
row format delimited
fields terminated by ','
STORED AS TEXTFILE;

insert into table gender_data_partition partition(IndicatorCode='FP') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'FP%';

insert into table gender_data_partition partition(IndicatorCode='IC') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'IC%';

insert into table gender_data_partition partition(IndicatorCode='NY') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'NY%';

insert into table gender_data_partition partition(IndicatorCode='SE') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'SE%';

insert into table gender_data_partition partition(IndicatorCode='SG') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'SG%';

insert into table gender_data_partition partition(IndicatorCode='SH') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'SH%';

insert into table gender_data_partition partition(IndicatorCode='SI') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'SI%';

insert into table gender_data_partition partition(IndicatorCode='SL') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'SL%';

insert into table gender_data_partition partition(IndicatorCode='SP') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'SP%';

insert into table gender_data_partition partition(IndicatorCode='WP') select CountryName, Y1960, Y1961, Y1962, Y1963, Y1964, Y1965, Y1966, Y1967, Y1968,
 Y1969, Y1970, Y1971, Y1972, Y1973, Y1974, Y1975, Y1976, Y1977, Y1978, Y1979, Y1980, Y1981, Y1982, Y1983, Y1984, Y1985, Y1986, Y1987, Y1988, Y1989, Y1990, Y1991, Y1992, Y1993,
 Y1994, Y1995, Y1996, Y1997, Y1998, Y1999, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016, IndicatorCode as IC from gender_data where IndicatorCode like 'WP%';

set mapreduce.input.fileinputformat.split.maxsize=256000000;

