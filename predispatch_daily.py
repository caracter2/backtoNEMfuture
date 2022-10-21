import pandas as pd
import numpy as np
import datetime
import requests
from bs4 import BeautifulSoup
import os
from zipfile import ZipFile
import io
from tqdm import tqdm
import plotly.express as px
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import functools
import streamlit as st

@functools.lru_cache(maxsize=None, typed=False)
def get_nemweb_file(url, table_name='', filter_column_n = None, filter_value = None, as_of=None):
    # sourcery skip: use-fstring-for-concatenation
    '''
    By default the function will filter the resultant file by the third column (assumed to contain the table_name).
    If specified, however, you can filter any column by specifying the column by position (using filter_column_n)
    and value (using filter_value)
    '''
    
    assert url[-3:] =='zip', 'Expect a zip file in url.'

    # Capitalise table name
    table_name = table_name.upper()

    # Download file and open zip
    file = requests.get(url, verify = False)
    z = ZipFile(io.BytesIO(file.content))

    # Set list of files as a dataframe:
    files_df = pd.DataFrame(data = z.namelist(), columns = ['filename'])

    # if as_of is defined we create timestamps for each file and pick the nearest one
    if as_of is not None:
        as_of = pd.to_datetime(as_of)

        try:
            files_df['timestamp'] = pd.to_datetime(files_df.filename.apply(lambda x: x.rsplit('_',1)[-1].rsplit('.')[0]))
        except Exception:
            files_df['timestamp'] = pd.to_datetime(files_df.filename.apply(lambda x: x.rsplit('_',2)[-2]))

        files_df.set_index('timestamp',inplace=True)
        files_df = pd.DataFrame(files_df.iloc[files_df.index.get_loc(as_of, method='ffill')]).T

    # Create a list of dummy columns names (numbers 1 through 300) to place as column headers for multi-table and multi-headers csvs
    dummy_cols = ['col_'+ str(x).zfill(3) for x in np.arange(1,300)]    

    # Initialise list of resultant files
    all_files = []

    # Open file
    for filename in files_df.filename:
        df = (pd.read_csv(z.open(filename),
                          compression = 'zip' if filename[-3:]== 'zip' else 'infer',
                          names = dummy_cols,  engine='python')
             )
        # Slice to only include the table we want
        if not filter_column_n is None and not filter_value is None:
            column_str = 'col_' + str(filter_column_n).zfill(3)
            df = (df
                  .query(f'{column_str} == "{filter_value}"')
                 )
        else:
            df = (df
                  .query(f'col_003 == "{table_name}"')
                 )

        # Set actual headers to first row
        df.columns = df.iloc[0]

        # Remove redundant header row and empty/dummy columns
        df = (df[1:]
                .dropna(axis=1, how='all')
               )

        # Append to list of all files
        all_files.append(df)

    # Concatenate all files
    data = pd.concat(all_files)

    # Change values to numeric where possible
    data = data.apply(pd.to_numeric, errors='ignore')

    return data

def get_files_list_nemweb_directory(url, verify=False):
    reqs = requests.get(url, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    # dates are found between line breaks
    dates = []
    for br in soup.findAll('br'):
        next_s = br.nextSibling
        dates.append(next_s)
    
    # links are found more easily via 'a' elements
    links = []
    for link in soup.find_all('a'):
        link_string = link.get('href')
        if not link_string is None:
            link_string = 'https://nemweb.com.au' + link_string
        links.append(link_string)
    
    # List of dates has an extra item on top (linking to parent directory) and an extra empty line at the bottom
    # so we slice the list using [1:-1]
    # List of links has one link to remove at the start (that linking to the parent directory) so we slice
    # the list using [1:]
    files = pd.DataFrame({'dates':dates[1:-1], 'links':links[1:]})
    files.links = files.links.str.lower()
            
    # After creating our dataframe we extract the date in datetime format
    files['date'] = pd.to_datetime(files.dates.str.rsplit(' ', n=2, expand=True)[0])
    
    # Then query to only keep the zip files with data and drop duplicates
    files = (files
             .query("links.str.contains('.zip')", engine = 'python')
             .sort_values(by = ['date','links'])
             .drop_duplicates(subset='date',keep='first')
             .sort_values(by = ['date','links'], ascending= [False,False])
            )
    return files

def get_public_prices_list():
    df = get_files_list_nemweb_directory('http://nemweb.com.au/reports/CURRENT/Public_Prices/')
    df['start'] = df.links.str.split('_',).apply(lambda x: pd.to_datetime(x[-2]))
    df['end'] = pd.to_datetime(df.links.str.split('_').apply(lambda x: x[-1]).str.replace('.zip','',regex=False))
    return df

def get_tradingis_reports_list():
    df = get_files_list_nemweb_directory('http://nemweb.com.au/reports/CURRENT/TradingIS_Reports/')
    df['start'] = df.links.str.split('_',).apply(lambda x: pd.to_datetime(x[-2])) - pd.Timedelta('5min')
    df['end'] = df.start + pd.Timedelta('5min')
    return df

def get_predispatch_reports_list():
    df = get_files_list_nemweb_directory('http://nemweb.com.au/reports/CURRENT/Predispatch_Reports/')
    df['start'] = (df.links
                   .str.replace('_LEGACY','')
                   .str.split('_',).apply(lambda x: pd.to_datetime(x[-2])) - pd.Timedelta('5min')
                  )
    df['end'] = df.start + pd.Timedelta('5min')
    return df

def get_earliest_current_pd_date():
    current_pd_files = (get_files_list_nemweb_directory('http://nemweb.com.au/reports/CURRENT/Predispatch_Reports/')
                        .sort_values(by = ['date'])
                       )
    earliest_pd_file = pd.to_datetime(current_pd_files.iloc[0,:].links.split('_')[-3])
    return earliest_pd_file
    

def get_required_pd_files_list(start, end):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    all_links_df = pd.DataFrame(columns = ['url','source'])
    if start < get_earliest_current_pd_date():
        years_and_dates = pd.DataFrame({'dates':pd.date_range(start,end, freq = '1d')})
        years_and_dates['year'] = years_and_dates.dates.dt.year.astype(str)
        years_and_dates['month'] = years_and_dates.dates.dt.month.astype(str).str.zfill(2)
        years_and_dates = years_and_dates.drop_duplicates(subset=['year','month'])
        link_prefix = r'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM'
        years_and_dates['url_links'] = years_and_dates.apply(lambda df:f'{link_prefix}/{df.year}/MMSDM_{df.year}_{df.month}/MMSDM_Historical_Data_SQLLoader/PREDISP_ALL_DATA/PUBLIC_DVD_PREDISPATCHPRICE_{df.year}{df.month}010000.zip',axis=1 )
        archive_links = list(years_and_dates.url_links)
        archive_links_df = pd.DataFrame({'url':archive_links})
        archive_links_df['source'] = 'archive'
        all_links_df = pd.concat([all_links_df,archive_links_df])
        
    if end > get_earliest_current_pd_date():
        adjusted_start = start - pd.Timedelta('1h')
        files_list = (get_predispatch_reports_list()
                      .query('start >= @adjusted_start')
                      .query('end <= @end')
                      .sort_values(by = ['start'])
                     )
        recent_links = list(files_list.links)
        recent_links_df = pd.DataFrame({'url':recent_links})
        recent_links_df['source'] = 'current'
        all_links_df = pd.concat([all_links_df,recent_links_df])
        
    
    all_links_df =(all_links_df.sort_values(by = ['source'])
                   .reset_index(drop=True)
                  )
        
    return all_links_df

def crunch_current_predispatch_file(url):
    data = (get_nemweb_file(url,filter_column_n = 2, filter_value = 'PDREGION')
        .dropna(axis=1)
        .filter(['PREDISPATCHSEQNO','PERIODID','REGIONID','RRP'])
        .rename(columns = {'PREDISPATCHSEQNO':'from_datetime','PERIODID':'interval_30'})
       )
    data.REGIONID = data.REGIONID.str.replace('1','')
    data.interval_30 = pd.to_datetime(data.interval_30, yearfirst = True)
    data.from_datetime = pd.to_datetime(data.from_datetime, yearfirst=True)
    return data

def crunch_archive_predispatch_file(url):
    data = (get_nemweb_file(url,filter_column_n = 3, filter_value = 'REGION_PRICES')
        .dropna(axis=1)
        .filter(['LASTCHANGED','DATETIME','REGIONID','RRP'])
        .rename(columns = {'DATETIME':'interval_30','LASTCHANGED':'from_datetime'})
       )
    data.interval_30 = pd.to_datetime(data.interval_30, yearfirst = True)
    data.from_datetime = pd.to_datetime(data.from_datetime, yearfirst=True).dt.floor('30min')
    data.REGIONID = data.REGIONID.str.replace('1','')
    return data


def get_predispatch_price_NEMWEB(start = datetime.date.today(),
                                 end = datetime.date.today() + datetime.timedelta(days=1)):
    
    
    pd_data = None
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    #TODO FIND ACTUAL START LIMIT
    assert start >= pd.to_datetime('1 jul 2009'), 'predispatch price data only exists from 1 Jul 2009 onwards.'
    
    list_of_files = get_required_pd_files_list(start, end)
    num_files = len(list_of_files)
    i = 0

    files_data = []
    with st.spinner('getting forecast data...'):
        pd_progress_bar = st.progress(0)
        # archive files
        for url in tqdm(list_of_files.query('source == "archive"').url.values):
            i+=1
            pd_progress_bar.progress(i/num_files)
            data = (crunch_archive_predispatch_file(url)
                    .query('interval_30>= @start')
                    .query('interval_30<= @end')
                    .query('from_datetime>= @start')
                    .query('from_datetime<= @end')
                )
            files_data.append(data)

        # current files
        for url in tqdm(list_of_files.query('source == "current"').url.values):
            i+=1
            pd_progress_bar.progress(i/num_files)
            data = (crunch_current_predispatch_file(url)
                    .query('interval_30>= @start')
                    .query('interval_30<= @end')
                    .query('from_datetime>= @start')
                    .query('from_datetime<= @end')
                )
            
            files_data.append(data)

        all_data = (pd.concat(files_data)
                    .drop_duplicates()
                    .sort_values(by = ['from_datetime','interval_30','REGIONID'])
                    .reset_index(drop=True)
                    .rename(columns = {'REGIONID':'region','RRP':'forecast_30min'})
                )
        all_data.from_datetime = pd.to_datetime(all_data.from_datetime)
        all_data.interval_30 = pd.to_datetime(all_data.interval_30)
        pd_progress_bar.empty()
    return all_data

def crunch_archive_dispatch_price_file(url):
    data = (get_nemweb_file(url,filter_column_n = 3, filter_value = 'PRICE')
        .dropna(axis=1)
        .filter(['SETTLEMENTDATE','REGIONID','RRP'])
       )
    return data

def get_dispatch_price_archive_files(start, end):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    dates = (pd.DataFrame({'dates':pd.date_range(start,end,freq='1d')}))
    dates = (dates
             .assign(month = dates.dates.dt.month.astype(str).str.zfill(2))
             .assign(year = dates.dates.dt.year.astype(str))
             .drop_duplicates(subset=['month','year'])
            )
    prefix = r'https://nemweb.com.au/data_archive/Wholesale_Electricity/MMSDM/'
    dates['links'] = dates.apply(lambda df: prefix +f'{df.year}/MMSDM_{df.year}_{df.month}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DISPATCHPRICE_{df.year}{df.month}010000.zip',axis=1)
    
    return dates


def get_trading_price_NEMWEB(start = datetime.date.today(),
                             end = datetime.date.today() + datetime.timedelta(days=1)):
    price_data=None
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    assert start >= pd.to_datetime('1 jul 2009'), 'trading price data only exists from 1 Jul 2009 onwards.'
    start_str = start.strftime('%Y/%m/%d %H:%M:%S')
    end_str = end.strftime('%Y/%m/%d %H:%M:%S')
    archive_success = False


    with st.spinner('getting settled prices data...'):
        settled_prices_progress_bar = st.progress(0.0)
        try:
            files_data =[]
            archive_tp_list = get_dispatch_price_archive_files(start, end)                
            num_of_files = len(archive_tp_list)
            print(f'{num_of_files} to download')
            i=0
            for url in tqdm(archive_tp_list.links.values):
                i+=1
                settled_prices_progress_bar.progress(i/num_of_files)
                price_data = crunch_archive_dispatch_price_file(url)
                price_data.SETTLEMENTDATE = pd.to_datetime(price_data.SETTLEMENTDATE)
                price_data.RRP = price_data.RRP.astype(float)
                files_data.append(price_data)
            price_data = pd.concat(files_data)

            if (len(price_data)> 0 and
                price_data.SETTLEMENTDATE.max() >= end and
                price_data.SETTLEMENTDATE.min() <= start+pd.Timedelta('5min')):
                settled_prices_progress_bar.progress(1.0)
                archive_success = True
        except:
            print('Incomplete or failed attempt to get data from archive.')
            archive_success = False

        settled_prices_progress_bar.empty()
        
        public_prices_success = False
        
        if not archive_success:
            try:
                print('Sourcing data from public prices...')
                if not price_data is None:
                    if len(price_data)>0:
                        adjusted_start = price_data.SETTLEMENTDATE.max() - pd.Timedelta('1d')
                    else:
                        adjusted_start = start
                else:
                    adjusted_start = start

                later_end = end + pd.Timedelta('1d')
                public_prices_list = (get_public_prices_list()
                                    .query('start >= @adjusted_start')
                                    .query('end <= @later_end')
                                    )
                

                files_data = []
                num_of_files = len(public_prices_list)
                i=0
                for url in tqdm(public_prices_list.links.values):
                    i+=1
                    settled_prices_progress_bar.progress(i/num_of_files)
                    data = (get_nemweb_file(url,filter_column_n = 2, filter_value = 'DREGION')
                            .dropna(axis=1)
                            .filter(['SETTLEMENTDATE','REGIONID','RRP'])
                            .drop_duplicates()
                            .query('SETTLEMENTDATE != "SETTLEMENTDATE"')
                        )

                    files_data.append(data)

                daily_report_data = pd.concat(files_data)

                if not price_data is None:
                    price_data = pd.concat([price_data,daily_report_data])
                    price_data.SETTLEMENTDATE = pd.to_datetime(price_data.SETTLEMENTDATE)
                    price_data.RRP = price_data.RRP.astype(float)

                    if (len(price_data)> 0 and
                        price_data.SETTLEMENTDATE.max() >= end and
                        price_data.SETTLEMENTDATE.min() <= start+pd.Timedelta('5min')):

                        public_prices_success = True
            except:
                print('Incomplete or failed attempt to get data daily public prices.')
                public_prices_success = False
                
        if not archive_success and not public_prices_success:
            print('Sourcing most recent data...')
            if not price_data is None:
                if len(price_data)>0:
                    adjusted_start = price_data.SETTLEMENTDATE.max() - pd.Timedelta('1d')
                else:
                    adjusted_start = start
            else:
                adjusted_start = start
                
            later_end = end
            recent_prices_list = (get_tradingis_reports_list()
                                .query('start >= @adjusted_start')
                                .query('end <= @later_end')
                                )
            
            files_data = []
            num_of_files = len(recent_prices_list)
            i=0
            for url in tqdm(recent_prices_list.links.values):
                i+=1
                settled_prices_progress_bar.progress(i/num_of_files)
                data = (get_nemweb_file(url,table_name = 'PRICE')
                        .dropna(axis=1)
                        .filter(['SETTLEMENTDATE','REGIONID','RRP'])
                        .drop_duplicates()
                        .query('SETTLEMENTDATE != "SETTLEMENTDATE"')
                    )
                
                files_data.append(data)

            settled_prices_progress_bar.empty()    

            recent_prices_data = pd.concat(files_data)
            
            if not price_data is None:
                price_data = pd.concat([price_data,recent_prices_data])
            else:
                price_data = recent_prices_data
    
    price_data.SETTLEMENTDATE = pd.to_datetime(price_data.SETTLEMENTDATE)
    price_data.RRP = price_data.RRP.astype(float)
    price_data.REGIONID = price_data.REGIONID.str.replace('1','')
    price_data = (price_data
                  .filter(['SETTLEMENTDATE','REGIONID','RRP'])
                  .sort_values(by = ['SETTLEMENTDATE','REGIONID'])
                  .query('SETTLEMENTDATE > @start')
                  .query('SETTLEMENTDATE <= @end')
                  .drop_duplicates()
                  .reset_index(drop=True)
                  .rename(columns = {'REGIONID':'region','RRP':'settled_5min','SETTLEMENTDATE':'interval_5'})
                 )
    price_data = (price_data
                 .assign(interval_30 = price_data.interval_5.dt.ceil('30min'))
                )
    price_data['settled_30min'] = price_data.groupby(by=['interval_30','region'])['settled_5min'].transform('mean')
    price_data.columns.name = ''
    return price_data



def create_forecast_vs_actuals_chart(actuals,
                                     predispatch,
                                     state = 'NSW'):
    resampled_predispatch = (predispatch.copy())
    resampled_predispatch['interval_5'] = resampled_predispatch.apply(lambda df: pd.date_range(df.interval_30-pd.Timedelta('30min'),
                                                                       df.interval_30, freq = '5min'), axis=1)
    resampled_predispatch = resampled_predispatch.explode('interval_5')


    actuals_df = actuals.query('region == @state')

    unique_froms = list(resampled_predispatch.from_datetime.unique())
    expanded_actuals = (actuals_df)
    expanded_actuals['from_datetime'] = expanded_actuals.apply(lambda df:unique_froms, axis=1)
    expanded_actuals = expanded_actuals.explode('from_datetime')

    df = (resampled_predispatch
          .query('region == @state')
          .query('interval_30> from_datetime')
          .merge(right= expanded_actuals, on = ['from_datetime','region','interval_5','interval_30'], how = 'outer')
          .sort_values(by = ['interval_5','interval_30','from_datetime'])
          .reset_index(drop=True)
         )

    last_settled=(df.filter(['from_datetime','interval_5','settled_5min'])
                  .dropna()
                  .interval_5.max()
                 )

    df['from_datetime_str'] = df.from_datetime.dt.strftime('%Y-%m-%d %H:%M')
    df['keep'] = np.where(((df.settled_5min.isnull()) & (df.settled_30min.isnull())),'True','False')
    df['keep'] = np.where((df.keep == "True") & np.logical_not( df.forecast_30min.isnull()),'False','True')
    df['keep'] = np.where((df.keep == "True") | (df.interval_5>=last_settled),'True','False')
    df = (df
          .query('keep=="True"')
         )


    fig = px.line(df, x = 'interval_5', y = ['forecast_30min','settled_5min','settled_30min'],
                  color_discrete_map = {'forecast_30min':'red','settled_5min':'grey','settled_30min':'black'},
                  animation_frame = 'from_datetime_str',
                  title = f'{state} Predispatch prices vs settled'
                 )
    return fig

    

