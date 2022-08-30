
import pandas as pd
import numpy as np
import datetime
import io
import plotly.express as px
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from zipfile import ZipFile
import functools



def sources_menu(table_name:str = 'PREDISPATCHREGIONSUM'):
    '''Returns a dataframe with links and source info for required table from all NemWeb sources.
    table_name: (str) One of "PREDISPATCHINTERCONNECTORRES", "PREDISPATCHPRICE" or "PREDISPATCHREGIONSUM". Default is "PREDISPATCHREGIONSUM" '''
    
    # Setup dictonary to look up according tables for each source
    table_name_dict={'P5MIN_REGIONSOLUTION':{'data_archive':'P5MIN_REGIONSOLUTION_ALL',
                                             'archive':'P5_Reports',
                                             'current':'P5_Reports',
                                             'filter_name':'REGIONSOLUTION',
                                             'time_frame':'p5'},
                     
                     
                     'P5MIN_INTERCONNECTORSOLN':{'data_archive':'P5MIN_INTERCONNECTORSOLN_ALL',
                                             'archive':'P5_Reports',
                                             'current':'P5_Reports',
                                             'filter_name':'INTERCONNECTORSOLN',
                                             'time_frame':'p5'},
                     
                     
                     'DISPATCHPRICE':{'data_archive':'P5MIN_INTERCONNECTORSOLN_ALL',
                                             'archive':'Public_Prices',
                                             'current':'Public_Prices',
                                             'filter_name':'INTERCONNECTORSOLN',
                                             'time_frame':'p5'},
        
        
                    'PREDISPATCHREGIONSUM':{'data_archive':'PREDISPATCHREGIONSUM',
                                            'archive':'PredispatchIS_Reports',
                                            'current':'PredispatchIS_Reports',
                                            'filter_name':'REGION_SOLUTION',
                                            'time_frame':'predispatch'},
                     
                     
                    'PREDISPATCHINTERCONNECTORRES':{'data_archive':'PREDISPATCHINTERCONNECTORRES',
                                                    'archive':'PredispatchIS_Reports',
                                                    'current':'PredispatchIS_Reports',
                                                    'filter_name':'INTERCONNECTOR_SOLN',
                                                    'time_frame':'predispatch'},
                     
                    'PREDISPATCHPRICE':{'data_archive':'PREDISPATCHPRICE',
                                        'archive':'PredispatchIS_Reports',
                                        'current':'PredispatchIS_Reports',
                                        'filter_name':'REGION_PRICES',
                                        'time_frame':'predispatch'},
                     
                    'PREDISPATCHPRICESENSITIVITIES':{'data_archive':None,
                                                      'archive':'Predispatch_Sensitivities',
                                                      'current':'Predispatch_Sensitivities',
                                                      'filter_name':'PRICESENSITIVITIES',
                                                      'time_frame':'predispatch'},
                     
                    'TRADINGPRICE':{'data_archive':'TRADINGPRICE',
                                     'archive':'TradingIS_Reports',
                                     'current':'TradingIS_Reports',
                                     'filter_name':'PRICE',
                                     'time_frame':'actuals'},
                     
                    'STPASA_REGIONSOLUTION':{'data_archive':'STPASA_REGIONSOLUTION',
                                             'archive':'Short_Term_PASA_Reports',
                                             'current':'Short_Term_PASA_Reports',
                                             'filter_name':'REGIONSOLUTION',
                                             'time_frame':'stpasa'},
                     
                    'STPASA_INTERCONNECTORSOLN':{'data_archive':'STPASA_INTERCONNECTORSOLN',
                                                 'archive':'Short_Term_PASA_Reports',
                                                 'current':'Short_Term_PASA_Reports',
                                                 'filter_name':'INTERCONNECTORSOLN',
                                                 'time_frame':'stpasa'}
                    }
    
    df_data_archive_info = data_archive_info(table_name = table_name_dict[table_name]['data_archive'],
                                             time_frame = table_name_dict[table_name]['time_frame'] )

    df_archive_info = archive_info(table_name_dict[table_name]['archive'])
    
    # only keep if ahead of data_archive (and if data_archive exists)
    if not df_data_archive_info is None:
        df_archive_info = df_archive_info[df_archive_info.dates> df_data_archive_info.dates.max()]
    
    
    df_current_info = current_info(table_name_dict[table_name]['current'])
    
    # only keep if ahead of archive
    if len(df_archive_info)>0:
        df_current_info = df_current_info[df_current_info.dates> df_archive_info.dates.max()]
    else:
        df_current_info = df_current_info[df_current_info.dates> df_data_archive_info.dates.max()]
        
        
    sources = (pd.concat([df_data_archive_info,df_archive_info,df_current_info])
              .reset_index(drop=True))
    sources['table'] = table_name
    sources['filter_name'] = table_name_dict[table_name]['filter_name']
    
    #todo implement sources menu for other table types
    sources.dates = pd.to_datetime(sources.dates)
    sources = (sources
               .sort_values(by =['preference','dates'])
               .drop_duplicates(subset='dates',keep='first')
              )
    
    return sources
    

def data_archive_info(table_name:str = None, time_frame = 'predispatch'):
    '''Returns a dataframe with links and source info for required table from data_archive NemWeb sources.
    table_name: (str) One of "PREDISPATCHINTERCONNECTORRES", "PREDISPATCHPRICE" or "PREDISPATCHREGIONSUM". Default is "PREDISPATCHREGIONSUM" '''
    if table_name is None:
        return None
    data_archive = pd.DataFrame(data = pd.date_range(earliest_data_archive_date(), latest_data_archive_date()), columns = ['dates'])
    data_archive.dates = data_archive.dates + pd.Timedelta(hours=4)
    data_archive['year'] = data_archive.dates.dt.year.astype(str)
    data_archive['month'] = data_archive.dates.dt.month.astype(str).str.zfill(2)
    url_prefix = r'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/'

    if time_frame == 'predispatch':
        folder_str = 'PREDISP_ALL_DATA'
        
    elif time_frame == 'p5':
        folder_str = 'P5MIN_ALL_DATA'
        
    else:
        folder_str = 'DATA'
        
    data_archive['url'] = data_archive.apply(lambda df: (url_prefix +
                                                    f'{df.year}/MMSDM_{df.year}_{df.month}/MMSDM_Historical_Data_SQLLoader/{folder_str}/PUBLIC_DVD_{table_name}_{df.year}{df.month}010000.zip'),
                                         axis = 1)

    data_archive['source'] = 'data_archive'
    data_archive['preference'] = 0
    data_archive = data_archive[['dates','source','preference','url']]
    return data_archive

def archive_info(table_name:str = 'PredispatchIS_Reports'):
    '''Returns a dataframe with links and source info for required table from archive NemWeb sources.
    table_name: (str) One of "Adjusted_Prices_Reports", "Bidmove_Complete", "Bidmove_Summary", "Billing", "CDEII", "Daily_Reports", "DAILYOCD", "Dispatch_IRSR",
    "Dispatch_Negative_Residue", "Dispatch_Reports", "Dispatch_SCADA", "DispatchIS_FCAS_Fix", "DispatchIS_Reports", "Dispatchprices_PRE_AP", "GSH", "HistDemand",
    "Market_Notice", "MCCDispatch", "Medium_Term_PASA_Reports", "MTPASA_DUIDAvailability", "MTPASA_RegionAvailability", "Network", "Next_Day_Actual_Gen", 
    "Next_Day_Dispatch", "Next_Day_Intermittent_DS", "NEXT_DAY_MCCDISPATCH", "Next_Day_Offer_Energy", "Next_Day_Offer_FCAS", "Next_Day_PreDispatch", 
    "Next_Day_PreDispatchD", "Next_Day_Trading", "Operational_Demand", "P5_Reports", "PDPASA", "Predispatch_IRSR", "Predispatch_Reports",
    "Predispatch_Sensitivities", "PredispatchIS_Reports", "Public_Prices", "ROOFTOP_PV", "Settlements", "SEVENDAYOUTLOOK_FULL", "SEVENDAYOUTLOOK_PEAK",
    "Short_Term_PASA_Reports", "Trading_Cumulative_Price", "Trading_IRSR", "TradingIS_Reports", "WDR_CAPACITY_NO_SCADA", "Weekly_Bulletin", "Yesterdays_Bids_Reports",
    "Yesterdays_MNSPBids_Reports".
    
    Default is "PredispatchIS_Reports" '''
    verify = True
    pd_archive_url = f'http://nemweb.com.au/Reports/Archive/{table_name}/'
    reqs = requests.get(pd_archive_url, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    all_links = []
    for link in soup.find_all('a'):
        link_string = link.get('href')
        if not link_string is None and '.zip' in link_string:
            link_string = 'https://nemweb.com.au' + link_string
            all_links.append(link_string)

    archive = pd.DataFrame(data = all_links, columns = ['url'])
    try:
        archive['start_date'] = pd.to_datetime(archive.url.apply(lambda x: x.rsplit('_',2)[-2]))
    except:
        archive['start_date'] = pd.to_datetime(archive.url.apply(lambda x: x.rsplit('_',1)[-1].rsplit('.')[0]))
        
    
    archive['end_date'] = pd.to_datetime(archive.url.apply(lambda x: x.rsplit('_',1)[-1].rsplit('.')[0]))
    archive['dates'] = archive.apply(lambda df: pd.date_range(df.start_date, df.end_date, freq='1d'), axis = 1)
    archive = archive.explode(column = 'dates')
    archive['source'] = 'archive'
    archive['preference'] = 1
    archive = archive[['dates','source','preference','url']]
    return archive


def current_info(table_name:str ='PredispatchIS_Reports'):
    '''Returns a dataframe with links and source info for required table from current reports NemWeb sources.
    table_name: (str) One of "Adjusted_Prices_Reports", "Alt_Limits", "Ancillary_Services_Payments", "Auction_Units_Reports", "Bidmove_Complete", "Bidmove_Summary",
    "Billing", "Causer_Pays", "Causer_Pays_Elements", "Causer_Pays_Rslcpf", "Causer_Pays_Scada", "CDEII", "CSC_CSP_ConstraintList", "CSC_CSP_Settlements",
    "Daily_Reports", "DAILYOCD", "Directions_Reconciliation", "Dispatch_IRSR", "DISPATCH_NEGATIVE_RESIDUE", "Dispatch_Reports", "Dispatch_SCADA", "DispatchIS_Reports",
    "Dispatchprices_PRE_AP", "DWGM", "Gas_Supply_Guarantee", "GBB", "GSH", "HighImpactOutages", "HistDemand", "IBEI", "Marginal_Loss_Factors", "Market_Notice",
    "MCCDispatch", "Medium_Term_PASA_Reports", "Mktsusp_Pricing", "MMSDataModelReport", "MTPASA_DUIDAvailability", "MTPASA_RegionAvailability", "Network",
    "Next_Day_Actual_Gen", "NEXT_DAY_AVAIL_SUBMISS_CLUSTER", "NEXT_DAY_AVAIL_SUBMISS_DAY", "Next_Day_Dispatch", "Next_Day_Intermittent_DS", "NEXT_DAY_MCCDISPATCH",
    "Next_Day_Offer_Energy", "Next_Day_Offer_FCAS", "Next_Day_PreDispatch", "Next_Day_PreDispatchD", "Next_Day_Trading", "Operational_Demand", "P5_Reports", "PasaSnap",
    "PD7Day", "PDPASA", "Predispatch_IRSR", "Predispatch_Reports", "Predispatch_Sensitivities", "PredispatchIS_Reports", "Public_Prices", "PublishedModelDataAccess",
    "Regional_Summary_Report", "Reserve_Contract_Recovery", "ROOFTOP_PV/ACTUAL","ROOFTOP_PV/FORECAST", "Settlements", "SEVENDAYOUTLOOK_FULL", "SEVENDAYOUTLOOK_PEAK", "Short_Term_PASA_Reports",
    "SRA_Bids", "SRA_NSR_RECONCILIATION", "SRA_Offers", "SRA_Results", "STTM", "SupplyDemand", "Trading_Cumulative_Price", "Trading_IRSR", "TradingIS_Reports",
    "VicGas", "Vwa_Fcas_Prices", "WDR_CAPACITY_NO_SCADA", "Weekly_Bulletin", "Weekly_Constraint_Reports", "Yesterdays_Bids_Reports", "Yesterdays_MNSPBids_Reports"
    
    Default is "PredispatchIS_Reports"
    
    '''
    verify = True
    pd_archive_url = f'http://nemweb.com.au/Reports/Current/{table_name}/'
    reqs = requests.get(pd_archive_url, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')

    all_links = []
    for link in soup.find_all('a'):
        link_string = link.get('href')
        if not link_string is None and '.zip' in link_string:
            link_string = 'https://nemweb.com.au' + link_string
            all_links.append(link_string)

    current = pd.DataFrame(data = all_links, columns = ['url'])
    try:
        current['dates'] = pd.to_datetime(current.url.apply(lambda x: x.rsplit('_',1)[-1].rsplit('.')[0]))
    except:
        current['dates'] = pd.to_datetime(current.url.apply(lambda x: x.rsplit('_',2)[-2]))

    current['source'] = 'current'
    current['preference'] = 2
    current = current[['dates','source','preference','url']]
    return current



def latest_data_archive_date(verify = False):
    '''returns the latest date available in the NemWeb Data Archive.
    verify variable establishes whether to check SSL cert. Default is false, can also be set to "Q:\certs\ca-bundle-git.crt" to use self-signed certs in Q drive.
    '''
    archive_url = 'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/'
    latest_year = 0
    latest_link = ''
    reqs = requests.get(archive_url, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    for link in soup.find_all('a'):
        link_string =link.get('href')
        if not link_string is None:
            link_string = 'https://nemweb.com.au' + link_string
            if 'MMSDM' in link_string and 'MTPASA' not in link_string:
                if int(link_string[-5:-1]) > latest_year:
                    latest_year = int(link_string[-5:-1]) 
                    latest_link = link_string
    reqs = requests.get(archive_url, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    
    reqs = requests.get(latest_link, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    latest_month = 1
    for link in soup.find_all('a'):
        link_string =link.get('href')
        if not link_string is None:
            link_string = 'https://nemweb.com.au' + link_string
            if str(latest_year) in link_string and '.zip' not in link_string:
                if int(link_string[-3:-1]) > latest_month:
                    latest_month = int(link_string[-3:-1])
                    
    latest_date = pd.to_datetime(pd.Period(f'{latest_year}-{latest_month}-01',freq='M').end_time.date()+pd.Timedelta(days=1))-pd.Timedelta(seconds=1)
    latest_date = pd.to_datetime(latest_date)

    return latest_date


def earliest_data_archive_date(verify = True):
    '''returns the latest date available in the NemWeb Data Archive.
    verify variable establishes whether to check SSL cert. Default is false, can also be set to "Q:\certs\ca-bundle-git.crt" to use self-signed certs in Q drive.
    '''
    archive_url = 'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/'
    latest_year = 5000
    latest_link = ''
    reqs = requests.get(archive_url, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    for link in soup.find_all('a'):
        link_string =link.get('href')
        if not link_string is None:
            link_string = 'https://nemweb.com.au' + link_string
            if 'MMSDM' in link_string and 'MTPASA' not in link_string:
                if int(link_string[-5:-1]) < latest_year:
                    latest_year = int(link_string[-5:-1]) 
                    latest_link = link_string
    reqs = requests.get(archive_url, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    
    reqs = requests.get(latest_link, verify = verify)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    
    latest_month = 12
    for link in soup.find_all('a'):
        link_string =link.get('href')
        if not link_string is None:
            link_string = 'https://nemweb.com.au' + link_string
            if str(latest_year) in link_string and '.zip' not in link_string:
                if int(link_string[-3:-1]) < latest_month:
                    latest_month = int(link_string[-3:-1])
                    
    earliest_date = pd.to_datetime(f'{latest_year}-{latest_month}-01')

    return earliest_date





# Price tables
def nemweb_trading_prices(as_of=None, start = None, end = None):
    return nemweb_table(table_name = 'TRADINGPRICE', as_of = as_of, start=start, end=end, transform_func = transform_trading_prices_data)

# Predispatch tables
def nemweb_pd_prices(as_of=None, start=None, end=None):
    return nemweb_table(table_name ='PREDISPATCHPRICE', as_of=as_of, start=start, end=end, transform_func = transform_pd_price_data)

# 5min tables
#TODO
# def nemweb_5min_region_solution(as_of=None, start=None, end=None):
#     return nemweb_table(table_name ='P5MIN_REGIONSOLUTION', as_of=as_of, start=start, end=end, transform_func = transform_p5_region_data)





# predispatch_price
def transform_pd_price_data(raw_df):
    pd_price_data = raw_df.copy()
    # Remove '1' suffix from REGIONID
    pd_price_data.REGIONID = pd_price_data.REGIONID.str.replace('1','')

    pd_price_data = (pd_price_data
                   # remove interventions
                   .query('INTERVENTION < 1')
                   # Filter to only desired columns
                  .filter(['PREDISPATCHSEQNO','REGIONID', 'RRP', 'EEP', 'LASTCHANGED',
                           'DATETIME', 'RAISE6SECRRP', 'RAISE60SECRRP', 'RAISE5MINRRP',
                           'RAISEREGRRP', 'LOWER6SECRRP', 'LOWER60SECRRP', 'LOWER5MINRRP',
                           'LOWERREGRRP'])
                  )
    
    # Set relevant columns to datetimes
    pd_price_data.LASTCHANGED = pd.to_datetime(pd_price_data.LASTCHANGED)
    pd_price_data.DATETIME = pd.to_datetime(pd_price_data.DATETIME)
   
    # Melt data so that data from all regions exists per row
    pd_price_data = (pd_price_data
                  .melt(id_vars = ['PREDISPATCHSEQNO','LASTCHANGED', 'DATETIME', 'REGIONID'], value_name = 'value',var_name = 'attribute')
                  )
    
    # concatenate with itself wth regionid set to combined to get all
    pd_price_data = (pd.concat([pd_price_data, pd_price_data.assign(REGIONID='AVG')])
                  .groupby(by = ['PREDISPATCHSEQNO','LASTCHANGED', 'DATETIME', 'REGIONID','attribute'])
                   .mean()
                   .reset_index()
                  )

    # Create new attribute name incorporating region
    pd_price_data.attribute = pd_price_data.attribute +'_'+ pd_price_data.REGIONID

    # Pivot table back so it's a long table with data for all regions and combined regions per row
    pd_price_data = (pd_price_data
                  .pivot_table(index=['PREDISPATCHSEQNO','LASTCHANGED', 'DATETIME'], columns = 'attribute', values = 'value')
                   .reset_index()
                  )
    

    return pd_price_data

# settled_prices
def transform_trading_prices_data(raw_df, freq ='30min', average_FCAS=False):
    trading_price_data = raw_df.copy()
    # Remove '1' suffix from REGIONID
    trading_price_data.REGIONID = trading_price_data.REGIONID.str.replace('1','')
    
    trading_price_data = (trading_price_data
                          # Filter to only desired columns
                          .filter(['SETTLEMENTDATE', 'REGIONID', 'RRP', 'RAISE6SECRRP', 'RAISE60SECRRP',
                           'RAISE5MINRRP', 'RAISEREGRRP', 'LOWER6SECRRP', 'LOWER60SECRRP', 'LOWER5MINRRP', 'LOWERREGRRP'])
                          # rename SETTLEMENTDATE to DATETIME
                          .rename(columns = {'SETTLEMENTDATE':'DATETIME'})
                  )
    
    # Set relevant columns to datetimes
    trading_price_data.DATETIME = pd.to_datetime(trading_price_data.DATETIME)
    
    # if frequency is 30minutes we average the data into intervals of 30 minutes (like prior to Oct 2021)
    if freq == '30min':
        trading_price_data.DATETIME = trading_price_data.DATETIME.dt.ceil('30min')
        trading_price_data = (trading_price_data.groupby(by=['DATETIME', 'REGIONID'])
                              .mean()
                              .reset_index()
                             )
        
   
    # Melt data so that data from all regions exists per row
    trading_price_data = (trading_price_data
                  .melt(id_vars = ['DATETIME', 'REGIONID'], value_name = 'value',var_name = 'attribute')
                  )
    
    # filter another set of data to only include mainland FCAS prices
    mainland_states = ['SA','VIC','QLD','NSW']
    mainland_data_only = (trading_price_data.copy()
                          # only keep mainland states
                          .query("REGIONID == @mainland_states")
                          # remove energy
                          .query('attribute !="RRP"')
                          .assign(REGIONID='MAINLAND_AVG')
                         )
    
    # concatenate with mainland data and calculate mean
    trading_price_data = (pd.concat([trading_price_data, mainland_data_only])
                          .groupby(by = ['DATETIME', 'REGIONID','attribute'])
                          .mean()
                          .reset_index()
                         )
    
        
    # Create new attribute name incorporating region ID
    trading_price_data.attribute = trading_price_data.attribute.str.replace('RRP','_SETTLEDRRP')
    trading_price_data.attribute = np.where(trading_price_data.attribute.str[0]=='_','ENERGY'+ trading_price_data.attribute,trading_price_data.attribute)
    
    # if average FCAS then we remove the individual fcas prices for each state and just keep the whole NEM average
    if average_FCAS:
        trading_price_data = (trading_price_data
                             .query('(attribute.str.contains("ENERGY")) or (REGIONID.str.contains("AVG"))', engine = 'python')
                             )
        
    trading_price_data.attribute = trading_price_data.attribute +'_'+ trading_price_data.REGIONID
    
    # Pivot table back so it's a long table with data for all regions and combined regions per row
    trading_price_data = (trading_price_data
                               .drop(columns = ['REGIONID'])
                               .pivot_table(index=['DATETIME'], columns = 'attribute', values = 'value')
                               .reset_index()
                              )
    

    return trading_price_data





@functools.lru_cache(maxsize=None, typed=False)
def get_nemweb_table(url, table_name, as_of=None):
    
    assert url[-3:] =='zip', 'Expect a zip file in url.'
    
    # Capitalise table name
    table_name = table_name.upper()
    
    # Download file and open zip
    file = requests.get(url, verify = True)    
    z = ZipFile(io.BytesIO(file.content))
    
    # Set list of files as a dataframe:
    files_df = pd.DataFrame(data = z.namelist(), columns = ['filename'])
    
    # if as_of is defined we create timestamps for each file and pick the nearest one
    if not as_of is None:
        as_of = pd.to_datetime(as_of)
        
        try:
            files_df['timestamp'] = pd.to_datetime(files_df.filename.apply(lambda x: x.rsplit('_',1)[-1].rsplit('.')[0]))
        except:
            files_df['timestamp'] = pd.to_datetime(files_df.filename.apply(lambda x: x.rsplit('_',2)[-2]))
    
        files_df.set_index('timestamp',inplace=True)
        files_df = pd.DataFrame(files_df.iloc[files_df.index.get_loc(as_of, method='ffill')]).T
    
    # Create a list of dummy columns names (numbers 1 through 300) to place as column headers for multi-table and multi-headers csvs
    dummy_cols = ['col_'+ str(x).zfill(3) for x in np.arange(1,300)]    
    
    # Initialise list of resultant files
    all_files = []
    
    # Open each file
    #for filename in tqdm(files_df.filename): #old
    for filename in files_df.filename:
        print(filename)
        df = (pd.read_csv(z.open(filename),
                          compression = 'zip' if filename[-3:]== 'zip' else 'infer',
                          names = dummy_cols,  engine='python')
              # Slice to only include the table we want
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




def nemweb_table(table_name:str='PREDISPATCHREGIONSUM', as_of=None, start=None, end=None, transform_func = None, date_slice_column = 'LASTCHANGED'):
    
    sources = sources_menu(table_name)
               
    if not as_of is None:
        as_of = pd.to_datetime(as_of)
        sources.set_index('dates',inplace=True)
        sources = (pd.DataFrame(sources.iloc[sources.index.get_loc(as_of, method='ffill')])
                   .T
                  .reset_index()
                  .rename(columns = {'index':'dates'}))
    
    elif not start is None and not end is None:
        start_datetime = pd.to_datetime(start) # - pd.Timedelta(days=1)
        end_datetime = pd.to_datetime(end) # + pd.Timedelta(days=1)
        sources = (sources
                  .query(f'dates >= "{start_datetime.strftime("%Y-%m-%d %X")}"')
                   .query(f'dates <= "{end_datetime.strftime("%Y-%m-%d %X")}"')
                  )
    else:
        # use latest file
        sources = sources.tail(1)
        
    
    dfs = []
    for i, row in tqdm(sources.iterrows()):
        url = row.url
        table_filter = row.filter_name
        df = get_nemweb_table(url, table_filter, as_of = as_of)
        dfs.append(df)
    dfs = pd.concat(dfs)
    dfs[date_slice_column] = pd.to_datetime(dfs[date_slice_column])
    
    if not as_of is None:    
        matching = (dfs.copy()
                   .set_index(date_slice_column)
                   )

        # Find LASTCHANGED that is equal or just earlier to as_of date
        matching = matching.loc[matching.index.unique()[matching.index.unique().get_loc(as_of, method='ffill')]].reset_index()
        matching_value = matching[date_slice_column][0]
        
        # Filter by that LASTCHANGED value
        dfs = dfs[dfs[date_slice_column] == matching_value]
            
    elif not start is None and not end is None:
        dfs = (dfs
                  .query(f'{date_slice_column} >= "{start_datetime.strftime("%Y-%m-%d %X")}"')
                   .query(f'{date_slice_column} <= "{end_datetime.strftime("%Y-%m-%d %X")}"')
                  )
    else:
        # use latest 
        dfs = dfs[dfs[date_slice_column] == dfs[date_slice_column].max()]
            
            
    # transform data (if function provided)
    if not transform_func is None:
        dfs = transform_func(dfs)
        
    return dfs





def get_pd_data_for_date(date = datetime.date.today()):
    return nemweb_pd_prices(start = date, end = date+datetime.timedelta(days = 1))




def get_trading_prices_for_date(date = pd.to_datetime(datetime.date.today())):
    start = pd.to_datetime(pd.to_datetime(date).date()) - datetime.timedelta(minutes = 30)
    end = pd.to_datetime(pd.to_datetime(date).date()) + datetime.timedelta(days = 2)
    return nemweb_trading_prices(start = start, end = end)



def transform_pd_data(pd_df):
    transformed_df = (pd_df
                      .drop(columns=['PREDISPATCHSEQNO'])
                     )
    transformed_df.columns.name = None
    transformed_df = (transformed_df
                      .melt(id_vars = ['LASTCHANGED','DATETIME'],value_name = 'price',var_name = 'attribute')
                     )
    transformed_df[['attribute','state']] = transformed_df.attribute.str.split('_',expand = True)
    transformed_df.attribute = transformed_df.attribute.replace('RRP','Energy')
    transformed_df.attribute = transformed_df.attribute.str.replace('RRP','')
    transformed_df = (transformed_df
                      .assign(type = np.where(transformed_df.attribute=='Energy','Energy','FCAS'))
                      .query('attribute !="EEP"')
                      .rename(columns = {'attribute':'market'})
                     )
    transformed_df['source_datetime'] = transformed_df.LASTCHANGED.dt.floor('5min').dt.strftime('%Y%m%d %H:%M')
    transformed_df['kind'] = 'predispatch'
    return transformed_df




def transform_settled_prices(settled_df, keep_all=True):   
    trading_prices_transformed = (settled_df
                                 .copy()
                                )
    trading_prices_transformed.columns.name = None
    trading_prices_transformed = (trading_prices_transformed
                                 .melt(id_vars = ['DATETIME'],value_name = 'price',var_name = 'attribute')

                                )
    trading_prices_transformed.attribute = trading_prices_transformed.attribute.str.replace('_SETTLEDRRP','')
    trading_prices_transformed.attribute = trading_prices_transformed.attribute.str.replace('MAINLAND_AVG','MAINLANDAVG')
    trading_prices_transformed[['attribute','state']] = trading_prices_transformed.attribute.str.split('_',expand = True)

    trading_prices_transformed.attribute = trading_prices_transformed.attribute.replace('ENERGY','Energy')


    trading_prices_transformed = (trading_prices_transformed
                                  .assign(LASTCHANGED = trading_prices_transformed.DATETIME)
                                 .assign(type = np.where(trading_prices_transformed.attribute=='Energy','Energy','FCAS'))
                                 .query('attribute !="EEP"')
                                 .rename(columns = {'attribute':'market'})
                                )

    trading_prices_transformed['ALL_DATETIMES'] = trading_prices_transformed.market.apply(lambda x: trading_prices_transformed.DATETIME.unique())
    trading_prices_transformed = (trading_prices_transformed.explode('ALL_DATETIMES'))
    trading_prices_transformed = trading_prices_transformed.query('ALL_DATETIMES>=LASTCHANGED') if not keep_all else trading_prices_transformed
    trading_prices_transformed = (trading_prices_transformed
                                  .drop(columns=['LASTCHANGED'])
                                  .rename(columns = {'ALL_DATETIMES':'LASTCHANGED'})
                                  .sort_values(by = ['LASTCHANGED','DATETIME','state','market'])
                                 )

    trading_prices_transformed['source_datetime'] = trading_prices_transformed.LASTCHANGED.dt.floor('5min').dt.strftime('%Y%m%d %H:%M')
    trading_prices_transformed['kind'] = 'settled'
    # trading_prices_transformed.query('state == "NSW"').query('market == "Energy"').head(49)

    trading_prices_transformed = trading_prices_transformed.filter(['LASTCHANGED','DATETIME',
                                                                    'market','price',
                                                                    'state','type',
                                                                    'source_datetime','kind'])
    return trading_prices_transformed




def get_all_data_for_date(date = pd.to_datetime(datetime.date.today()), keep_all=True):
    trading_prices = get_trading_prices_for_date(date)
    pd_data = get_pd_data_for_date(date)
    
    trading_prices = transform_settled_prices(trading_prices, keep_all = keep_all)
    pd_data = transform_pd_data(pd_data)
    combined_data = (pd.concat([pd_data,trading_prices])
                     .sort_values(by = ['kind','LASTCHANGED','source_datetime','DATETIME','state','market'],
                                  ascending = [False,True,True,True,True,True])
                     .reset_index(drop=True)
                    )
    
    return combined_data
    
    




def create_date_fig(date = pd.to_datetime(datetime.date.today()),state = 'NSW',market = 'Energy', keep_all=True):
    combined_data = get_all_data_for_date(date = date, keep_all = keep_all)
    combined_data = (combined_data
                     .query(f'state == "{state}"')
                     .query(f'market == "{market}"')
                    )
    
    fig = px.line(combined_data, x = 'DATETIME', y = 'price', facet_col = 'state', facet_row = 'market',
                 animation_frame='source_datetime', color = 'kind', color_discrete_map={'settled':'black'})
    fig.update_yaxes(matches=None)
    fig.for_each_annotation(lambda a: a.update(text = a.text.split('=')[1]))
    return fig



