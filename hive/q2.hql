DROP TABLE IF EXISTS Q2;

CREATE TABLE Q2 AS
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
FROM gender_data_partition where country_code ='USA' and indicator_code = 'SE.TER.CMPL.FE.ZS') x
LATERAL VIEW EXPLODE(tmp) explode_table AS years, datum;

insert overwrite local directory '/media/sf_BigDataProj2/q2ans'
row format delimited
fields terminated by ','
select * from q2;