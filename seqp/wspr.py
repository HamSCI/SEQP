import datetime
from collections import OrderedDict
import numpy as np
import pandas as pd

def wspr_csv_to_df(csv_file):
    """
    Reads a WSPR CSV generated by trim_wspr_web_csv() into a
    pandas dataframe.
    """
    df = pd.read_csv(csv_file,parse_dates=[1])
    return df

def trim_wspr_web_csv(filename="wsprspots-2017-08.csv.gz",
    sTime=datetime.datetime(2017,8,21,14),
    eTime=datetime.datetime(2017,8,21,22)):
    """
    Reads in a datafile downloaed from http://wsprnet.org/drupal/downloads, parses the dates,
    trims it to the datetimes specified, and outputs a trimmed, compressed, CSV.
    """

    names   = ['spot_id','timestamp','reporter','reporter_grid','snr','freq','call_sign','grid','power','drift','distance','azimuth','band','version','code']

    prsr    = lambda x: datetime.datetime.fromtimestamp(float(x))
    df      = pd.read_csv(filename,header=None,names=names,parse_dates=[1],date_parser=prsr)
    df.set_index('spot_id',inplace=True)

    sTime_str   = sTime.strftime('%Y%m%d.%H%M')
    eTime_str   = eTime.strftime('%Y%m%d.%H%M')
    file_out    = '{}-{}_wsprspots.csv.bz2'.format(sTime_str,eTime_str)

    tf      = np.logical_and(df.timestamp >= sTime, df.timestamp < eTime)
    df      = df[tf]

    df.to_csv(file_out,compression='bz2')

    print('Wrote: {}'.format(file_out))

def get_df(csv_file):
    """
    Reads a WSPR CSV generated by trim_wspr_web_csv() into a
    pandas dataframe that has been cleaned and formatted for science
    analysis.
    """
    print('Getting WSPR dataframe...')
    df  = wspr_csv_to_df(csv_file)
    df['mode']                  = 'WSPR'
    df['grid_src_0']            = 'wspr'
    df['grid_src_1']            = 'wspr'

    keys = OrderedDict()
    keys['timestamp']           = 'datetime'
    keys['freq']                = 'frequency'
    keys['mode']                = 'mode'
    keys['reporter']            = 'call_0'
    keys['reporter_grid']       = 'grid_0'
    keys['grid_src_0']          = 'grid_src_0'
    keys['snr']                 = 'srpt_0'
    keys['call_sign']           = 'call_1'
    keys['grid']                = 'grid_1'
    keys['grid_src_1']          = 'grid_src_1'
#    keys['power']               = 'power'

    kys = [x for x in keys.keys()]
    df  = df[kys]
    df  = df.rename(columns=keys)

    df['source']    = 'wspr'

    return df
