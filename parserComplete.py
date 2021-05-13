import json
import requests
import mysql.connector

# Connect to mysql server
mysql_config = {
    'user': 'fastbyte',
    'password': 'aldee220202',
    'host': '127.0.0.1',
    'database': 'test',
    'raise_on_warnings': True
}

con = mysql.connector.connect(**mysql_config)

# Create 4 cursors for each table
cura = con.cursor()
curb = con.cursor()
curc = con.cursor()
curd = con.cursor()

'''
--------- Overall India_data -----------

DATA taken from: https://api.rootnet.in/covid19-in/stats/latest

table:
    - total cases
    - recoveries
    - total deaths

----------------------------------------
'''
try:
    cura.execute('DROP TABLE IF EXISTS india_data')
except:
    pass

cura.execute(
    'CREATE  TABLE India_Data (totalCases BIGINT, recoveries BIGINT, deaths BIGINT)')

r = requests.get("https://api.rootnet.in/covid19-in/stats/latest").text
j = json.loads(r)


country = (j.get('data')).get('summary')
totalCases = int(country.get('total'))
totlaRecoveries = int(country.get('discharged'))
totalDeaths = int(country.get('deaths'))

sql = "insert into {table} values (%s, %s, %s)"
cura.execute(sql.format(table="India_Data"),
             (totalCases, totlaRecoveries, totalDeaths))

'''
--------- regional_data ---------

DATA taken from: https://api.rootnet.in/covid19-in/stats/latest

-> statename, totalcases, totalrecovered, deaths so far
-----------------------------------
'''
try:
    curb.execute('DROP TABLE Regional_data;')
except:
    pass

curb.execute(
    'CREATE TABLE Regional_data (statename varchar(50), totalCases BIGINT, recoveries BIGINT, deaths BIGINT);')

regional = (j.get('data')).get('regional')

for i in regional:
    state = str(i.get('loc'))
    totalcases = int(i.get('totalConfirmed'))
    recovered = int(i.get('discharged'))
    deaths = int(i.get('deaths'))
    sql = "insert into {table} values (%s, %s, %s, %s)"
    curb.execute(sql.format(table="Regional_data"),
                 (state, totalcases, recovered, deaths))


'''
---------- GLOBAL DATA ----------

gets live data from https://mahabub81.github.io/covid-19-api/api/v1/world-summary.json

TABLE INFO:
    - active
    - confirmed
    - deaths
    - recovered

---------------------------------
'''
r = requests.get(
    'https://mahabub81.github.io/covid-19-api/api/v1/world-summary.json').text
j = json.loads(r)

totalConfirmed = j.get('confirmed')
totalDeaths = j.get('deaths')
totalRecovered = j.get('recovered')
totalActive = j.get('active')

try:
    curc.execute("DROP TABLE IF EXISTS global_data")
except:
    pass

curc.execute(
    "CREATE TABLE global_data (active BIGINT, confirmed BIGINT, deaths BIGINT, recovered BIGINT)")

sql = "insert into {table} values (%s, %s, %s, %s)"
curb.execute(sql.format(table="global_data"),
             (totalActive, totalConfirmed, totalDeaths, totalRecovered))


'''
------------- COUNTRY DATA -----------------

Link:
https://mahabub81.github.io/covid-19-api/api/v1/countries.json

table:
    - name
    - active
    - confirmed
    - deaths
    - recovered

'''
r = requests.get(
    'https://mahabub81.github.io/covid-19-api/api/v1/countries.json').text

j = json.loads(r)

try:
    curd.execute("DROP TABLE IF EXISTS country_data")
except:
    pass
curd.execute(
    "CREATE TABLE country_data (name varchar(100), active BIGINT, confirmed BIGINT, deaths BIGINT, recovered BIGINT);")
sql = "insert into {table} values (%s, %s, %s, %s, %s)"

for country in j:
    name = country.get('country_region')
    data = country.get('latest')
    confirmed = data.get('confirmed')
    deaths = data.get('deaths')
    recovered = data.get('recovered')
    active = data.get('active')

    curd.execute(sql.format(table="country_data"),
                 (name, active, confirmed, deaths, recovered))


# Commit the changes to make save them
con.commit()
con.close()
