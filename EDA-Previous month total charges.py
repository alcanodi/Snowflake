import pandas as pd
import configparser
import snowflake.connector
from ydata_profiling import ProfileReport

config = configparser.ConfigParser()
config.read(r"settings_95592.ini") 

try:
    conn = snowflake.connector.connect(
        account=config["snowflake"]["account"],
        user=config["snowflake"]["user"],
        password=config["snowflake"]["password"],
        database=config["snowflake"]["database"],
        warehouse=config["snowflake"]["warehouse"],
        schema=config["snowflake"]["schema"]
    )
    print('Connected to SF')
except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")

# Extract data from source in SF
cursor = conn.cursor()
cursor.execute(
      '''
/*
Getting the total charges by claimid
*/
with 
charges as(
select
    claimid
    ,sum(case when type ='CHARGE' then amount end) as "Total charges"

from athenahealth.financials.activityfact

group by claimid
),

/*
Getting the payment information with the needed granularity, taking into account the defined range of dates and 
the existence of any payments
*/
payments as (
select 
    af.claimid
    ,year(af.postdate) as "Postdate payment year"
    ,month(af.postdate) as "Postdate payment month"
    ,af.patientid
    ,d.departmentname
    ,c.claimservicedate
    ,pa.ircgroup
    ,pa.insurancepackagename

    ,sum(case when af.transfertype = '1' and af.type = 'PAYMENT' then af.amount end) as "Primary payment"
    ,sum(case when af.transfertype = '2' and af.type = 'PAYMENT' then af.amount end) as "Secondary payment"
    ,sum(case when af.transfertype = 'p' and af.type = 'PAYMENT' then af.amount end) as "Patient payment"  
        
from athenahealth.financials.activityfact as af
left join athenahealth.athenaone.department as d on af.departmentid = d.departmentid
left join athenahealth.athenaone.claim as c on af.claimid = c.claimid
left join athenahealth.athenaone.patientinsurance as p on c.claimprimarypatientinsid = p.patientinsuranceid
left join athenahealth.athenaone.payer as pa on p.insurancepackageid = pa.insurancepackageid

where af.postdate >= '2024-03-01' and af.postdate<='2024-03-31'

group by 
    af.claimid
    ,year(af.postdate) 
    ,month(af.postdate) 
    ,af.patientid
    ,d.departmentname
    ,c.claimservicedate
    ,pa.ircgroup
    ,pa.insurancepackagename

having sum(case when af.type = 'PAYMENT' then af.amount end) <> 0
)

/*
Joining to get the total charges as a new column in the payment granularity query
*/

select
    p.*
    ,c."Total charges"
from payments as p
left join charges as c on p.claimid = c.claimid
      '''
      )

try:
# Get data
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description] 

    # Create df
    df = pd.DataFrame(data, columns=column_names)

    print('df extracted from SF')

except Exception as e:
        print(f"Error: {str(e)}")

# Close connection with SF

#df.to_csv('columnas_incident_tracker.csv',sep=',', index=False, encoding='utf-8')


df['CLAIMSERVICEDATE'] = pd.to_datetime(df['CLAIMSERVICEDATE'])
df['Primary payment'] = df['Primary payment'].astype(float)
df['Secondary payment'] = df['Secondary payment'].astype(float)
df['Patient payment'] = df['Patient payment'].astype(float)
df['Total charges'] = df['Total charges'].astype(float)
profile = ProfileReport(df, title="Profiling Report for Previous month total charges Analysis")
profile.to_file("EDA-Previous month total charges.html")


conn.close()