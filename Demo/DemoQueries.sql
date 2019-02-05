use project2;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Demo/Q1Answer'
row format delimited
fields terminated by ','
select c.tablename, g.countrycode, c.region, c.incomegroup,
	coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996) as latest_graduation_percentage
	from gender_data_partition g 
	join gender_country c on (g.countrycode = c.countrycode)
	where indicatorcode = 'SE.TER.CMPL.FE.ZS' and
	coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996) < 30.0;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Demo/Q2Answer'
row format delimited
fields terminated by ','
SELECT YEARS, DATUM
	FROM (
	SELECT MAP(
	'2000to2001', (y2001 - y2000) / y2000 * 100,
	'2001to2002', (y2002 - y2001) / y2001 * 100,
	'2002to2003', (y2003 - y2002) / y2002 * 100,
	'2003to2004', (y2004 - y2003) / y2003 * 100,
	'2004to2005', (y2005 - y2004) / y2004 * 100,
	'2005to2006', (y2006 - y2005) / y2005 * 100,
	'2006to2007', (y2007 - y2006) / y2006 * 100,
	'2007to2008', (y2008 - y2007) / y2007 * 100,
	'2008to2009', (y2009 - y2008) / y2008 * 100,
	'2009to2010', (y2010 - y2009) / y2009 * 100,
	'2010to2011', (y2011 - y2010) / y2010 * 100,
	'2011to2012', (y2012 - y2011) / y2011 * 100,
	'2012to2013', (y2013 - y2012) / y2012 * 100,
	'2013to2014', (y2014 - y2013) / y2013 * 100,
	'2014to2015', (y2015 - y2014) / y2014 * 100,
	'2015to2016', (y2016 - y2015) / y2015 * 100) as tmp
	FROM gender_data_partition where countrycode ='USA' and indicatorcode = 'SE.TER.CMPL.FE.ZS') x
	LATERAL VIEW EXPLODE(tmp) explode_table AS years, datum;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Demo/Q3Answer'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.EMP.TOTL.SP.MA.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Demo/Q4Answer'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.EMP.TOTL.SP.FE.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

INSERT OVERWRITE LOCAL DIRECTORY '/media/sf_shared/BigDataProj2/Demo/Q5Answer' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	SELECT indicatorcode, Y2000, Y2001, Y2002, Y2003, Y2004, Y2005, Y2006, Y2007, Y2008, Y2009, Y2010, Y2011, Y2012, Y2013, Y2014, Y2015, Y2016
	FROM gender_data_partition
	WHERE countrycode = 'USA' 
	AND indicatorcode IN('SL.IND.EMPL.MA.ZS', 'SL.AGR.EMPL.MA.ZS', 'SL.SRV.EMPL.MA.ZS');

