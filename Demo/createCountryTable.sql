use test;

drop table if exists gender_country;

create table gender_country (
	CountryCode varchar(3),
	ShortName varchar(255),
	TableName varchar(255),
	LongName varchar(255),
	AlphaCode varchar(2),
	CurrencyUnit varchar(255),
	Notes varchar(255),
	Region varchar(255),
	IncomeGroup varchar(255), 
	WB2 varchar(2),
	NationalAccountsBase varchar(255),
	NationalAccountReference varchar(255),
	SNAValuation varchar(255),
	Lending varchar(255),
	OtherGroups varchar(255),
	NationalAccountsSystem varchar(255),
	AlternativeConversion varchar(255),
	PPPSurvey varchar(255),
	BalanceOfPayments varchar(255),
	ExternalDebtReporting varchar(255),
	TradeSystem varchar(255),
	GovernmentAccounting varchar(255),
	IMFDissemination varchar(255),
	LastCensus varchar(255),
	LastHouseholdSurvey varchar(255),
	MostRecentIncomeExpenditure varchar(255),
	VitalRegistrationComplete varchar(255),
	LatestAgricultural varchar(255),
	LatestIndustrial varchar(255),
	LatestTrade varchar(255),
	LatestWaterWithdrawal varchar(255)
);

load data local infile '/media/sf_shared/gender-data/Gender_StatsCountry.csv'
into table gender_country
fields terminated by ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;