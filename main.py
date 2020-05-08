import requests
# from urllib2 import Request , urlopen, HTTPError
import json
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

#packages needed for authentication
import httplib2 as lib2
from oauth2client import client 
#packages needed for connecting with google API
from googleapiclient.discovery import build as google_build


print("test 1")

"""To access Google Analytics API, you need to produce your client_id, client_secret, and refresh_token. Refresh_token will be used to generate new client_secret that will expired every 1 hour"""

API_SERVICE_NAME = 'analyticsreporting'
API_VERSION = 'v4'
CLIENT_SECRETS_FILE = "/runner/sources/google_ads/client_secret.json"
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
client_id = "my_client_id"
client_secret = "my_client_secret"
token_uri = 'https://oauth2.googleapis.com/token'
token_expiry = datetime.now() - timedelta(days=1)
user_agent = 'my-user-agent/1.0'
refresh_token = 'my_refresh_token'

""" You need to create a connection to the destination database in which you will stream your data to """
engine = create_engine('mysql://root:@127.0.0.1/wibias')
con2 = engine.connect()
destination = 'wibias'


def getAccessToken(client_id, client_secret, token):
    params = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": token
    }

    authorization_url = "https://www.googleapis.com/oauth2/v4/token"

    r = requests.post(authorization_url, data=params)
    r.raise_for_status()
    if r.ok:
        print(r.json())
        return r.json()['access_token']
    else:
        return None


def new_access_code():
    access_token = getAccessToken(client_id, client_secret, refresh_token)
#     print(access_token)
    #credentials = google.oauth2.credentials.Credentials(access_token)
    return access_token


def pull_data(last_date):  
    access_token = new_access_code()
    credentials = client.GoogleCredentials(access_token=access_token, refresh_token=refresh_token, 
                                       client_id=client_id, client_secret=CLIENT_SECRETS_FILE, 
                                       token_uri="https://www.googleapis.com/oauth2/v4/token", token_expiry=token_expiry, 
                                       user_agent=user_agent)
    http = lib2.Http()

    #Authorize client
    authorized = credentials.authorize(http)

    #Let's build the client
    api_client = google_build(serviceName=API_SERVICE_NAME, version=API_VERSION, http=authorized)
    
    
    #Specify which data you want to pull from Google Analytics
    
    sample_request = {
      'viewId': '83705367',
      'dateRanges': {
          'startDate': last_date,
          'endDate': datetime.strftime(datetime.now() - timedelta(days=1),'%Y-%m-%d')
      },
      'dimensions': [{'name': 'ga:adContent'},
                    {'name':'ga:date'},
                     {'name':'ga:campaign'}],
      'metrics': [{'expression' : 'ga:users'},
                 {'expression' : 'ga:newUsers'},
                 {'expression' : 'ga:sessions'},
                 {'expression' : 'ga:bounceRate'},
                 {'expression' : 'ga:pageviewsPerSession'},
                  {'expression' : 'ga:avgSessionDuration'},
                  {'expression' : 'ga:goal1ConversionRate'},
                 {'expression' : 'ga:goal1Completions'},
                 {'expression' : 'ga:goal1Value'}],
        'filtersExpression':'ga:adContent=@_;ga:adContent!@GDN;ga:campaign!=id',
        'orderBys' : {'fieldName' : 'ga:date',
                     'sortOrder' : 'ASCENDING'},
        'pageSize' : 100000,
        'includeEmptyRows' : True
    }
    response = api_client.reports().batchGet(
        body={
        'reportRequests': sample_request
      }).execute()
    return response
    

def prase_response(report):
    """Parses and prints the Analytics Reporting API V4 response data"""
    #Initialize results, in list format because two dataframes might return
    result_list = []

    #Initialize empty data container for the two dateranges (if there are two that is)
    data_csv = []
    data_csv2 = []

    #Initialize header rows
    header_row = []

    #Get column headers, metric headers, and dimension headers.
    columnHeader = report.get('columnHeader', {})
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    dimensionHeaders = columnHeader.get('dimensions', [])

    #Combine all of those headers into the header_row, which is in a list format
    for dheader in dimensionHeaders:
        header_row.append(dheader)
    for mheader in metricHeaders:
        header_row.append(mheader['name'])

    #Get data from each of the rows, and append them into a list
    rows = report.get('data', {}).get('rows', [])
    for row in rows:
        row_temp = []
        dimensions = row.get('dimensions', [])
        metrics = row.get('metrics', [])
        for d in dimensions:
            row_temp.append(d)
        for m in metrics[0]['values']:
            row_temp.append(m)
        data_csv.append(row_temp)

        #In case of a second date range, do the same thing for the second request
        if len(metrics) == 2:
            row_temp2 = []
            for d in dimensions:
                row_temp2.append(d)
            for m in metrics[1]['values']:
                row_temp2.append(m)
            data_csv2.append(row_temp2)

    #Putting those list formats into pandas dataframe, and append them into the final result
    result_df = pd.DataFrame(data_csv, columns=header_row)
    result_list.append(result_df)
    if data_csv2 != []:
        result_list.append(pd.DataFrame(data_csv2, columns=header_row))

    return result_list


def update_data(raw_data,state):
    #define the query that will be exceuted depending on the situation (whether the data needs to be updated or simply inserted)
    select_query = """
    select * from {destination}.smt_daily_ga_ads da
    where da.ga_date in %(ga_date)s
    """.format(destination=destination)
    
    #smt_daily_ga_ad = con2.execute(text(select_query), dict(ga_date=raw_data['']))
    update_query = """
    UPDATE {destination}.smt_daily_ga_ads da
    SET
    ga_users = :ga_users,
    ga_newUsers = :ga_newUsers,
    ga_bounceRate = :ga_bounceRate,
    ga_pageviewsPerSession = :ga_pageviewsPerSession,
    ga_avgSessionDuration = :ga_avgSessionDuration,
    leads_conversion_rate = :leads_conversion_rate,
    leads_completions = :leads_completions,
    leads_velue = :leads_velue 
    WHERE
    da.ga_adContent = :ga_adContent
    and da.ga_campaign = :ga_campaign
    and da.ga_date = :ga_date
    """.format(destination=destination)
    
   
    insert_query = """
    INSERT INTO {destination}.smt_daily_ga_ads
    (ga_adContent, ga_date, ga_campaign, ga_users, ga_newUsers, ga_bounceRate, ga_pageviewsPerSession,
     ga_avgSessionDuration, leads_conversion_rate, leads_completions, leads_velue)
    VALUES
    (:ga_adContent, :ga_date, :ga_campaign, :ga_users, :ga_newUsers, :ga_bounceRate, :ga_pageviewsPerSession, 
        :ga_avgSessionDuration, :leads_conversion_rate, :leads_completions, :leads_velue) 
    """.format(destination=destination)

    
    smt_daily_ga_ad = pd.read_sql(select_query ,con2, params = dict(ga_date=raw_data['ga_date'].tolist())) #extract rows from database where the date is the same as that of raw_data
    
    
    tanggal = raw_data['ga_date'].unique() #list down the date extracted
    print(tanggal)

    if state is None:
        raw_data.to_sql('smt_daily_ga_ads',engine.connect(),if_exists='append',index_label='id')
        print('submitted')
    else:
        for tanggal in tanggal: #if raw_data contain more than 1 date, check 'em one by one
            raw_data_tanggal = raw_data[raw_data['ga_date']==tanggal]
            for i in (raw_data_tanggal.index.tolist()): 
                raw_data_index = raw_data_tanggal.loc[i] #checking raw_data row by row
                print(raw_data_index[['ga_adContent','ga_campaign']])
                if raw_data_index['ga_adContent'] in smt_daily_ga_ad['ga_adContent'].values: #if the adContent from raw_data already present in database, then check for its campaign name
                    c = smt_daily_ga_ad.index[smt_daily_ga_ad['ga_adContent'] == raw_data_index['ga_adContent']].tolist() #find the index at which the adContent from raw_data present in the database 
                    print(c)
                    #print('c')
                    for j in c: #check each campaign one by one
                        if raw_data_index['ga_campaign'] == smt_daily_ga_ad['ga_campaign'][j]: #if the same campaign already present, update it
                            #print('a')
                            #print(raw_data_index)
                            con2.execute(text(update_query),dict(raw_data_index))
                            print('data updated')
                            break
                    else:
                        con2.execute(text(insert_query), dict(raw_data_index)) #if not, insert as a new row
                        print("data inserted")
                else:
                    con2.execute(text(insert_query), dict(raw_data_index)) #if the adcontent is not found in the databse, insert as a new row
                    print("data inserted")
    return


def to_pd(response):
    response_data = response.get('reports', [])[0]
    raw_data = prase_response(response_data)[0]
    raw_data['ga:date']=pd.to_datetime(raw_data['ga:date'])
    raw_data.rename(index=str, columns = {'ga:adContent' : 'ga_adContent','ga:date' : 'ga_date','ga:campaign' : 'ga_campaign',
    'ga:users' : 'ga_users', 'ga:newUsers' : 'ga_newUsers', 'ga:sessions' : 'ga_sessions', 'ga:bounceRate':'ga_bounceRate','ga:pageviewsPerSession' : 'ga_pageviewsPerSession',
    'ga:avgSessionDuration' : 'ga_avgSessionDuration',
    'ga:goal1ConversionRate' : 'leads_conversion_rate','ga:goal1Completions':'leads_completions','ga:goal1Value':'leads_velue'}, inplace = True)
    #raw_data.index = np.arange(last_id, last_id+len(raw_data))
    last_date = max(raw_data['ga_date'])
    #print(max(raw_data.index))
    #raw_data.to_sql("smt_daily_ga_ads", con=engine.connect(), if_exists='append', index_label='id')
    print("finish")
    last_date = datetime.strftime(last_date,'%Y-%m-%d')
    update_state(int(raw_data.index[-1]), (last_date))
    return raw_data


def update_state(state, last_date):
    data = {#"last_id": state,
            "last_date": last_date}
    file = json.dumps(data)
    con2.execute(
        text("""
            INSERT INTO {destination}.smt_job_states (name, state)
            VALUES (:name, :state)
            ON DUPLICATE KEY
            UPDATE state = VALUES(state)
            """.format(destination=destination)),
        dict(name='data_ga:ads', state=file)
    )
    print("state updated")
    print(state)
    return


def main():
    #check the last data that have been streamed into the database
    query = """
            SELECT state
            FROM smt_job_states
            WHERE name = 'data_ga:ads'
            """
    state = con2.execute(text(query)).fetchone()#[0]
    if state is None:
        #if no data found, pull any data from 11 July 2019 (when the online ads began)
        last_date=datetime.strftime(datetime(2019,7,11),'%Y-%m-%d')
        #last_id = 0
    else:
        #else, pull the data starting from the last date it has been pulled
        state = json.loads(state[0])
        last_date = state['last_date']
        #last_id = state['last_id']
    response = pull_data(last_date)
    raw_data = to_pd(response)#, last_id)
    update_data(raw_data,state)
    #update_state()
    return


if __name__ == '__main__':
    while True:
        print("hello start")
        main()
        time.sleep(3610)
