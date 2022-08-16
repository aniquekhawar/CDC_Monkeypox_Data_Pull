import pandas as pd
from itertools import product

def update_timeseries(historical_timeseries, latest_timeseries):
    concat_df = pd.concat([historical_timeseries, latest_timeseries])
    latest_max_date = latest_timeseries['AsOf'].max()
    concat_df['AsOf'] = pd.to_datetime(concat_df['AsOf'])
    cases_to_date = latest_timeseries['Cases'].sum()

    # create data to impute (i.e., when states don't report on a specific day)
    unique_locations = concat_df['Location'].unique().tolist()
    imputed_date_range = pd.date_range('2022-8-4', latest_max_date)
    state_date_pairings = [unique_locations, imputed_date_range]
    state_date_pairings = [i for i in product(*state_date_pairings)]
    data_to_append = pd.DataFrame(state_date_pairings, columns = ['Location', 'AsOf'])

    concat_df2 = pd.concat([concat_df, data_to_append])
    concat_df2.drop_duplicates(subset = ['Location', 'AsOf'], keep = 'first', inplace = True)
    concat_df2.sort_values(by = ['Location', 'AsOf'], inplace = True)
    concat_df2['Cases'] = concat_df2.groupby(['Location'])['Cases'].ffill()
    print('Missing Cumulative Case Counts:', concat_df['Cases'].isna().sum())
    concat_df2['Cases'] = concat_df2.groupby(['Location'])['Cases'].bfill()
    
    # clean up variable names
    concat_df2.rename(columns = {'Location': 'State', 'Cases': 'Cumulative Cases'}, inplace = True)
    concat_df2['Daily New Cases'] = concat_df2.groupby('State')['Cumulative Cases'].diff(1)
    concat_df2['Daily New Cases'].fillna(0, inplace = True)
    concat_df2['Total US Cases to Date(excluding Non-Residents)'] = cases_to_date
    return concat_df2

# Load in historical state timeseries
state_ts = pd.read_csv('monkeypox_state_timeseries_latest.csv', usecols = ['State', 'Cumulative Cases', 'AsOf'], parse_dates = ['AsOf'])
historical_latest_date = state_ts['AsOf'].max()
state_ts.rename(columns = {'State': 'Location', 'Cumulative Cases': 'Cases'}, inplace = True)

url = 'https://www.cdc.gov/wcms/vizdata/poxvirus/monkeypox/data/USmap_counts.csv'
new_state_data = pd.read_csv(url, usecols = ['Location', 'Cases', 'AsOf'])
new_state_data = new_state_data[['Location', 'AsOf', 'Cases']].copy()
new_state_data = new_state_data[~new_state_data['Location'].isin(['Total', 'Non-US Resident'])] # filter out total and Non-US Resident

# extract the latest date from the CSV
new_state_data['AsOf'] = new_state_data['AsOf'].str.replace('Data as of ', '', regex = True)
new_state_data['AsOf'] = (pd.to_datetime(new_state_data['AsOf'])).dt.strftime('%Y-%m-%d')

state_latest_data_date = pd.to_datetime(new_state_data['AsOf'].max())

if state_latest_data_date > historical_latest_date:
    concat_df = update_timeseries(state_ts, new_state_data)
    concat_df.to_csv('data/monkeypox_state_timeseries_latest.csv', index = False)
else:
    print('No data to update.')
