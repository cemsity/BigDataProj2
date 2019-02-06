CREATE TABLE temp(
    country_code STRING,
    indicator_code STRING,
    Y1960 DECIMAL (30, 15),
    Y1961 DECIMAL (30, 15),
    Y1962 DECIMAL (30, 15),
    Y1963 DECIMAL (30, 15),
    Y1964 DECIMAL (30, 15),
    Y1965 DECIMAL (30, 15),
    Y1966 DECIMAL (30, 15),
    Y1967 DECIMAL (30, 15),
    Y1968 DECIMAL (30, 15),
    Y1969 DECIMAL (30, 15),
    Y1970 DECIMAL (30, 15),
    Y1971 DECIMAL (30, 15),
    Y1972 DECIMAL (30, 15),
    Y1973 DECIMAL (30, 15),
    Y1974 DECIMAL (30, 15),
    Y1975 DECIMAL (30, 15),
    Y1976 DECIMAL (30, 15),
    Y1977 DECIMAL (30, 15),
    Y1978 DECIMAL (30, 15),
    Y1979 DECIMAL (30, 15),
    Y1980 DECIMAL (30, 15),
    Y1981 DECIMAL (30, 15),
    Y1982 DECIMAL (30, 15),
    Y1983 DECIMAL (30, 15),
    Y1984 DECIMAL (30, 15),
    Y1985 DECIMAL (30, 15),
    Y1986 DECIMAL (30, 15),
    Y1987 DECIMAL (30, 15),
    Y1988 DECIMAL (30, 15),
    Y1989 DECIMAL (30, 15),
    Y1990 DECIMAL (30, 15),
    Y1991 DECIMAL (30, 15),
    Y1992 DECIMAL (30, 15),
    Y1993 DECIMAL (30, 15),
    Y1994 DECIMAL (30, 15),
    Y1995 DECIMAL (30, 15),
    Y1996 DECIMAL (30, 15),
    Y1997 DECIMAL (30, 15),
    Y1998 DECIMAL (30, 15),
    Y1999 DECIMAL (30, 15),
    Y2000 DECIMAL (30, 15),
    Y2001 DECIMAL (30, 15),
    Y2002 DECIMAL (30, 15),
    Y2003 DECIMAL (30, 15),
    Y2004 DECIMAL (30, 15),
    Y2005 DECIMAL (30, 15),
    Y2006 DECIMAL (30, 15),
    Y2007 DECIMAL (30, 15),
    Y2008 DECIMAL (30, 15),
    Y2009 DECIMAL (30, 15),
    Y2010 DECIMAL (30, 15),
    Y2011 DECIMAL (30, 15),
    Y2012 DECIMAL (30, 15),
    Y2013 DECIMAL (30, 15),
    Y2014 DECIMAL (30, 15),
    Y2015 DECIMAL (30, 15),
    Y2016 DECIMAL (30, 15)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH 'project2/staging' INTO TABLE temp;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.mapred.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

CREATE TABLE stats(
    country_code STRING,
    Y1960 DECIMAL (30, 15),
    Y1961 DECIMAL (30, 15),
    Y1962 DECIMAL (30, 15),
    Y1963 DECIMAL (30, 15),
    Y1964 DECIMAL (30, 15),
    Y1965 DECIMAL (30, 15),
    Y1966 DECIMAL (30, 15),
    Y1967 DECIMAL (30, 15),
    Y1968 DECIMAL (30, 15),
    Y1969 DECIMAL (30, 15),
    Y1970 DECIMAL (30, 15),
    Y1971 DECIMAL (30, 15),
    Y1972 DECIMAL (30, 15),
    Y1973 DECIMAL (30, 15),
    Y1974 DECIMAL (30, 15),
    Y1975 DECIMAL (30, 15),
    Y1976 DECIMAL (30, 15),
    Y1977 DECIMAL (30, 15),
    Y1978 DECIMAL (30, 15),
    Y1979 DECIMAL (30, 15),
    Y1980 DECIMAL (30, 15),
    Y1981 DECIMAL (30, 15),
    Y1982 DECIMAL (30, 15),
    Y1983 DECIMAL (30, 15),
    Y1984 DECIMAL (30, 15),
    Y1985 DECIMAL (30, 15),
    Y1986 DECIMAL (30, 15),
    Y1987 DECIMAL (30, 15),
    Y1988 DECIMAL (30, 15),
    Y1989 DECIMAL (30, 15),
    Y1990 DECIMAL (30, 15),
    Y1991 DECIMAL (30, 15),
    Y1992 DECIMAL (30, 15),
    Y1993 DECIMAL (30, 15),
    Y1994 DECIMAL (30, 15),
    Y1995 DECIMAL (30, 15),
    Y1996 DECIMAL (30, 15),
    Y1997 DECIMAL (30, 15),
    Y1998 DECIMAL (30, 15),
    Y1999 DECIMAL (30, 15),
    Y2000 DECIMAL (30, 15),
    Y2001 DECIMAL (30, 15),
    Y2002 DECIMAL (30, 15),
    Y2003 DECIMAL (30, 15),
    Y2004 DECIMAL (30, 15),
    Y2005 DECIMAL (30, 15),
    Y2006 DECIMAL (30, 15),
    Y2007 DECIMAL (30, 15),
    Y2008 DECIMAL (30, 15),
    Y2009 DECIMAL (30, 15),
    Y2010 DECIMAL (30, 15),
    Y2011 DECIMAL (30, 15),
    Y2012 DECIMAL (30, 15),
    Y2013 DECIMAL (30, 15),
    Y2014 DECIMAL (30, 15),
    Y2015 DECIMAL (30, 15),
    Y2016 DECIMAL (30, 15)
)
PARTITIONED BY (indicator_code STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT INTO TABLE stats partition(indicator_code) SELECT
    country_code,
    Y1960,
    Y1961,
    Y1962,
    Y1963,
    Y1964,
    Y1965,
    Y1966,
    Y1967,
    Y1968,
    Y1969,
    Y1970,
    Y1971,
    Y1972,
    Y1973,
    Y1974,
    Y1975,
    Y1976,
    Y1977,
    Y1978,
    Y1979,
    Y1980,
    Y1981,
    Y1982,
    Y1983,
    Y1984,
    Y1985,
    Y1986,
    Y1987,
    Y1988,
    Y1989,
    Y1990,
    Y1991,
    Y1992,
    Y1993,
    Y1994,
    Y1995,
    Y1996,
    Y1997,
    Y1998,
    Y1999,
    Y2000,
    Y2001,
    Y2002,
    Y2003,
    Y2004,
    Y2005,
    Y2006,
    Y2007,
    Y2008,
    Y2009,
    Y2010,
    Y2011,
    Y2012,
    Y2013,
    Y2014,
    Y2015,
    Y2016,
    indicator_code
    FROM temp
    WHERE
    country_code LIKE 'S%' OR
    country_code LIKE 'T%' OR
    country_code LIKE 'U%' OR
    country_code LIKE 'V%' OR
    country_code LIKE 'W%' OR
    country_code LIKE 'X%' OR
    country_code LIKE 'Y%' OR
    country_code LIKE 'Z%';

CREATE TABLE f_stats AS
SELECT indicator_code, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016
FROM stats 
WHERE country_code = 'USA' 
AND indicator_code 
IN('SL.AGR.EMPL.FE.ZS', 'SL.SRV.EMPL.FE.ZS', 'SL.IND.EMPL.FE.ZS');

CREATE TABLE m_stats AS
SELECT indicator_code, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016
FROM stats 
WHERE country_code = 'USA' 
AND indicator_code 
IN('SL.IND.EMPL.MA.ZS', 'SL.AGR.EMPL.MA.ZS', 'SL.SRV.EMPL.MA.ZS');

INSERT OVERWRITE LOCAL DIRECTORY '/media/sf_Shared/Revature/project-workspaces/project2/female_output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT indicator_code, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016
FROM stats 
WHERE country_code = 'USA' 
AND indicator_code 
IN('SL.AGR.EMPL.FE.ZS', 'SL.SRV.EMPL.FE.ZS', 'SL.IND.EMPL.FE.ZS');

INSERT OVERWRITE LOCAL DIRECTORY '/media/sf_Shared/Revature/project-workspaces/project2/male_output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT indicator_code, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016
FROM stats 
WHERE country_code = 'USA' 
AND indicator_code 
IN('SL.IND.EMPL.MA.ZS', 'SL.AGR.EMPL.MA.ZS', 'SL.SRV.EMPL.MA.ZS');

Low & middle income	LMY
Low income	LIC
Lower middle income	LMC
Middle income	MIC

SELECT country_code, indicator_code, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016
FROM stats 
WHERE country_code
IN('LMY', 'LIC', 'LMC');
AND indicator_code 
IN('SL.AGR.EMPL.FE.ZS', 'SL.SRV.EMPL.FE.ZS', 'SL.IND.EMPL.FE.ZS');