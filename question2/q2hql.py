print("DROP TABLE IF EXISTS Q2;")
print("CREATE TABLE Q2")
print("SELECT YEAR, DATUM")
print("FROM (")
print("SELECT MAP(")
for year in range(2000, 2016):
    print ("'" + str(year) + "to" + str(year + 1) + "', ((y" + str(year) + " - y" + str(year+1) + ") / y" + str(year + 1) + " * 100,", end='')

print(") as temp
print("FROM gender_from_2000 where country_code ='USA' and indicator_code = 'SE.TER.CMPL.FE.ZS'")
