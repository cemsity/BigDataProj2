use proj2;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q3Answers/employAll'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.EMP.TOTL.SP.MA.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q3Answers/employ1524'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.EMP.1524.SP.MA.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q3Answers/laborAll'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.TLF.CACT.MA.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q3Answers/labor1524'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.TLF.ACTI.1524.MA.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q4Answers/employAll'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.EMP.TOTL.SP.FE.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q4Answers/employ1524'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.EMP.1524.SP.FE.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q4Answers/laborAll'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.TLF.CACT.FE.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

insert overwrite local directory '/media/sf_shared/BigDataProj2/Q4Answers/labor1524'
row format delimited
fields terminated by ','
select avg(y2000), avg(y2001), avg(y2002), avg(y2003), avg(y2004), avg(y2005), avg(y2006), avg(y2007), avg(y2008), avg(y2009), avg(y2010), avg(y2011), avg(y2012), avg(y2013), avg(y2014), avg(y2015), avg(y2016), region, incomegroup
	from gender_country join gender_data_partition on gender_country.countrycode = gender_data_partition.countrycode
	where gender_data_partition.indicatorcode = 'SL.TLF.ACTI.1524.FE.ZS'
	and gender_country.incomegroup <> '' group by region, incomegroup;

