import pandas as pd

def order_and_sort(df):

    # Create a data frame with columns in the desired order.
    # Desired Keys
    dkeys   = []
    dkeys.append('datetime')
    dkeys.append('frequency')
    dkeys.append('mode')

    dkeys.append('call_0')
    dkeys.append('srpt_0')
    dkeys.append('grid_0')
    dkeys.append('lat_0')
    dkeys.append('lon_0')
    dkeys.append('grid_src_0')
    dkeys.append('pfx_0')
    dkeys.append('ctry_0')

    dkeys.append('call_1')
    dkeys.append('srpt_1')
    dkeys.append('grid_1')
    dkeys.append('lat_1')
    dkeys.append('lon_1')
    dkeys.append('grid_src_1')
    dkeys.append('pfx_1')
    dkeys.append('ctry_1')

    dkeys.append('source')
    dkeys.append('single_op')
    dkeys.append('log_file')

    keys = []
    for dk in dkeys:
        if dk in df.columns:
            keys.append(dk)
    df  = df[keys]

    df.sort_values('datetime',inplace=True)
    df.index    = list(range(len(df)))
    return df

def combine_spots(spot_dfs):
    print('Concatenating all dataframes...') 
    df  = pd.DataFrame()
    for inx,spot_df in enumerate(spot_dfs):
        df  = df.append(spot_df,ignore_index=True)
    df  = order_and_sort(df)
    return df

def clean_call(call):
    if not pd.isnull(call):
        call = call.replace('/','-').upper()
    return call
