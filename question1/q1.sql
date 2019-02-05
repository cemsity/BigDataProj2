use country_db;
-- individual country graduate ratios
drop table country_graduate_ratios;

create external table country_graduate_ratios(
    country_name string,
    country_code string,
	region_name string,
	income_group_name string,
    graduation_ratio decimal(30, 15))
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/cloudera/outputData/country_graduate_ratios';

insert overwrite table country_graduate_ratios
	select c.table_name, g.country_code, c.region, c.income_group,
	coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996) as latest_graduation_percentage
	from country_gender_partition g 
	join country_info c on (g.country_code = c.country_code)
	where indicator_code = 'SE.TER.CMPL.FE.ZS' and
	coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996) < 30.0;

-- average graduate ratios for each region
drop table region_graduate_ratios;

create external table region_graduate_ratios(
	region_name string,
	number_of_countries int,
	average_graduation_ratio decimal(30, 15))
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/cloudera/outputData/region_graduate_ratios';

insert overwrite table region_graduate_ratios
	select c.region as region, count(c.region) as number_of_countries,
	avg(coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996)) as average_graduation_ratio
	from country_gender_partition g
	join country_info c on (g.country_code = c.country_code)
	where g.indicator_code = 'SE.TER.CMPL.FE.ZS' and
	coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996) < 30.0
	group by c.region;

-- average graduate ratios for each income group
drop table income_group_graduate_ratios;

create external table income_group_graduate_ratios(
	income_group_name string,
	number_of_countries int,
	average_graduation_ratio decimal(30, 15))
row format delimited
fields terminated by '\t'
stored as textfile
location '/user/cloudera/outputData/income_group_graduate_ratios';

insert overwrite table income_group_graduate_ratios
	select c.income_group as income_group, count(c.income_group) as number_of_countries,
	avg(coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996)) as average_graduation_ratio
	from country_gender_partition g
	join country_info c on (g.country_code = c.country_code)
	where g.indicator_code = 'SE.TER.CMPL.FE.ZS' and
	coalesce(g.y2016, g.y2015, g.y2014, g.y2013, g.y2012, g.y2011, g.y2010, g.y2009, g.y2008, g.y2007, g.y2006,
	g.y2005, g.y2004, g.y2003, g.y2002, g.y2001, g.y2000, g.y1999, g.y1998, g.y1997, g.y1996) < 30.0
	group by c.income_group;