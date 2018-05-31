import os
import datetime
import zipfile
import urllib.request, urllib.error, urllib.parse          # Used to automatically download data files from the web.

from collections import OrderedDict
import numpy as np
import pandas as pd
import tqdm

import mysql.connector

#from pyporktools import qrz
from qrz import QRZ #sudo pip3 install pyQRZ
qrz = QRZ(cfg='./qrz_settings.cfg')

from .data import clean_call
from . import geopack

Re = 6371

def rbn_csv_to_df(csv_file):
    """
    Reads a RBN CSV into a dataframe.
    """
    df = pd.read_csv(csv_file,parse_dates=[0])
    return df

def get_df(csv_file,qth_locator=None):
    """
    Reads a RBN CSV into a pandas dataframe that has been
    cleaned and formatted for science analysis.
    """
    print('Getting RBN dataframe...')
    df = rbn_csv_to_df(csv_file)

    df_0    = df.copy()
    del df['mode']

    df['grid_0']                = None
    df['grid_1']                = None
    df['grid_src_0']            = None
    df['grid_src_1']            = None

    keys = OrderedDict()
    keys['timestamp']           = 'datetime'
    keys['freq']                = 'frequency'
    keys['tx_mode']             = 'mode'
    keys['rx_call']             = 'call_0'
    keys['grid_0']              = 'grid_0'
    keys['grid_src_0']          = 'grid_src_0'
    keys['db']                  = 'srpt_0'
    keys['tx_call']             = 'call_1'
    keys['grid_1']              = 'grid_1'
    keys['grid_src_1']          = 'grid_src_1'

    kys = [x for x in keys.keys()]
    df  = df[kys]
    df  = df.rename(columns=keys)

    for key in ['call_0','call_1']:
        df[key] = df[key].apply(clean_call)

    if qth_locator is not None:
        print('Geolocating RBN dataframe...')
        tqdm.tqdm.pandas(tqdm.tqdm,leave=True)
        for key in [0,1]:
            grid_k      = 'grid_{!s}'.format(key)
            grid_src_k  = 'grid_src_{!s}'.format(key)
            call_k      = 'call_{!s}'.format(key)

            print('{} --> {}'.format(call_k,grid_k))

            result              = df[call_k].progress_apply(qth_locator)
            grids, grid_srcs    = zip(*result)

            df[grid_k]          = grids
            df[grid_src_k]      = grid_srcs

    # Set Dataframe to MHz.
    df['frequency'] = df['frequency']/1000.

    df['source']    = 'rbn'

    return df

################################################################################
class MySqlEclipse(object):
    def __init__(self,user='hamsci',password='hamsci',host='localhost',database='seqp_analysis'):
        db          = mysql.connector.connect(user=user, password=password,host=host, database=database)
        crsr        = db.cursor()

#                      lat DECIMAL(10,6),
#                      lon DECIMAL(10,6),
        qry         = '''
                      CREATE TABLE IF NOT EXISTS location_cache (
                      callsign VARCHAR(20),
                      lat FLOAT,
                      lon FLOAT,
                      lookup_source VARCHAR(20),
                      lookup_datetime DATETIME
                      );
                      '''
        crsr.execute(qry)
        db.commit()

        self.db     = db

mysql_ecl = MySqlEclipse()

ram_cache   = {}

def geolocate(callsign):
    """
    Get the latitude and longitude of a callsign.
    First check the RAM cache, then the local MySQL cache, then go to QRZ.
    If the result is missing from any of the caches, add it.
    """

    # Check the RAM cache first...
    ram_result = ram_cache.get(callsign)
    if ram_result is not None:
        return ram_result

    # Now check MySQL...
    user        = 'hamsci'
    password    = 'hamsci'
    host        = 'localhost'
    database    = 'seqp_analysis'
    db          = mysql.connector.connect(user=user,password=password,host=host,database=database)
    
    qry     = ("SELECT lat,lon FROM location_cache "
               "WHERE callsign='{}';".format(callsign))
    crsr    = db.cursor()
    crsr.execute(qry)
    result  = crsr.fetchone()
    crsr.close()

    if result is None:
        # Not in RAM or MySQL, so try QRZ.com...
        try:
            result  = qrz.callsign(callsign)
            lat     = float(result['lat'])
            lon     = float(result['lon'])
        except:
            lat     = np.nan
            lon     = np.nan

        if not np.isnan(lat):
            # Add information to mysql database
            add_mysql       = ("INSERT INTO location_cache "
                               "(callsign,lat,lon,lookup_source,lookup_datetime) "
                               "VALUES (%s, %s, %s, %s, %s);")

            lookup_source   = 'qrz'
            lookup_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            data_mysql      = (callsign,lat,lon,lookup_source,lookup_datetime)

            crsr    = db.cursor()
            crsr.execute(add_mysql,data_mysql)
            db.commit()
            crsr.close()
    else:
        lat = result[0]
        lon = result[1]
    db.close()

    result              = (lat,lon)
    ram_cache[callsign] = result
    return result

def read_rbn(sTime,eTime=None,data_dir='data/rbn',qrz_call=None,qrz_passwd=None):
    ymd_list    = [datetime.datetime(sTime.year,sTime.month,sTime.day)]
    eDay        =  datetime.datetime(eTime.year,eTime.month,eTime.day)
    while ymd_list[-1] < eDay:
        ymd_list.append(ymd_list[-1] + datetime.timedelta(days=1))

    for ymd_dt in ymd_list:
        ymd         = ymd_dt.strftime('%Y%m%d')
        data_file   = '{0}.zip'.format(ymd)
        data_path   = os.path.join(data_dir,data_file)  

        time_0      = datetime.datetime.now()
        print('Starting RBN processing on <%s> at %s.' % (data_file,str(time_0)))

        ################################################################################
        # Make sure the data file exists.  If not, download it and open it.
        if not os.path.exists(data_path):
             try:    # Create the output directory, but fail silently if it already exists
                 os.makedirs(data_dir) 
             except:
                 pass

             # File downloading code from: http://stackoverflow.com/questions/22676/how-do-i-download-a-file-over-http-using-python
             url = 'http://www.reversebeacon.net/raw_data/dl.php?f='+ymd

             u = urllib.request.urlopen(url)
             f = open(data_path, 'wb')
             meta = u.info()
             file_size = int(meta["Content-Length"])
             print("Downloading: %s Bytes: %s" % (data_path, file_size))
         
             file_size_dl = 0
             block_sz = 8192
             while True:
                 buffer = u.read(block_sz)
                 if not buffer:
                     break
         
                 file_size_dl += len(buffer)
                 f.write(buffer)
                 status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
                 status = status + chr(8)*(len(status)+1)
                 print(status, end=' ')
             f.close()
             status = 'Done downloading!  Now converting to Pandas dataframe...'
             print(status)

        std_sTime=datetime.datetime(sTime.year,sTime.month,sTime.day, sTime.hour)
        if eTime.minute == 0 and eTime.second == 0:
            hourly_eTime=datetime.datetime(eTime.year,eTime.month,eTime.day, eTime.hour)
        else:
            hourly_eTime=eTime+datetime.timedelta(hours=1)
            hourly_eTime=datetime.datetime(hourly_eTime.year,hourly_eTime.month,hourly_eTime.day, hourly_eTime.hour)

        std_eTime=std_sTime+datetime.timedelta(hours=1)

        hour_flag=0
        while std_eTime<=hourly_eTime:
                csv_filename = 'rbn_'+std_sTime.strftime('%Y%m%d%H%M-')+std_eTime.strftime('%Y%m%d%H%M.csv.bz2')

                csv_filepath = os.path.join(data_dir,csv_filename)
                print(csv_filepath)
                if not os.path.exists(csv_filepath):
                    # Load data into dataframe here. ###############################################
                    with zipfile.ZipFile(data_path,'r') as z:   #This block lets us directly read the compressed gz file into memory.
                        with z.open(ymd+'.csv') as fl:
                            df          = pd.read_csv(fl,parse_dates=[10])

                    # Create columns for storing geolocation data.
                    df['dx_lat'] = np.zeros(df.shape[0],dtype=np.float)*np.nan
                    df['dx_lon'] = np.zeros(df.shape[0],dtype=np.float)*np.nan
                    df['de_lat'] = np.zeros(df.shape[0],dtype=np.float)*np.nan
                    df['de_lon'] = np.zeros(df.shape[0],dtype=np.float)*np.nan

                    # Trim dataframe to just the entries in a 1 hour time period.
                    df = df[np.logical_and(df['date'] >= std_sTime,df['date'] < std_eTime)]

                    # Look up lat/lons in QRZ.com
                    errors  = 0
                    success = 0
                    for index,row in df.iterrows():
                        if index % 50   == 0:
                            print(index,datetime.datetime.now()-time_0,row['date'])
                        de_call = row['callsign']
                        dx_call = row['dx']
                        dts     = row['date'].strftime('%Y %b %d %H%M UT')

                        de      = geolocate(de_call)
                        dx      = geolocate(dx_call)
                        row['de_lat'] = de[0]
                        row['de_lon'] = de[1]
                        row['dx_lat'] = dx[0]
                        row['dx_lon'] = dx[1]
                        df.loc[index] = row

                        if np.isnan(de[0]) or np.isnan(dx[0]):
#                            print('{index:06d} LOOKUP ERROR - {dt} DX: {dx} DE: {de}'.format(index=index,dt=dts,dx=dx_call,de=de_call))
                            errors += 1
                        else:
#                            print('{index:06d} OK - {dt} DX: {dx} DE: {de}'.format(index=index,dt=dts,dx=dx_call,de=de_call))
                            success += 1

                    total   = success + errors
                    if total == 0:
                        print("No call signs geolocated.")
                    else:
                        pct     = success / float(total) * 100.
                        print('{0:d} of {1:d} ({2:.1f} %) call signs geolocated via qrz.com.'.format(success,total,pct))
                    print('Writing: {}'.format(csv_filepath))
                    df.to_csv(csv_filepath,index=False,compression='bz2')
                else:
                    print('Reading: {}'.format(csv_filepath))
                    df = pd.read_csv(csv_filepath,parse_dates=['date'],compression='bz2')

                if hour_flag==0:
                    df_comp=df
                    hour_flag=hour_flag+1
                #When specified start/end times cross over the hour mark
                else:
                    df_comp=pd.concat([df_comp, df])

                std_sTime=std_eTime
                std_eTime=std_sTime+datetime.timedelta(hours=1)
        
        # Trim dataframe to just the entries we need.
        df = df_comp[np.logical_and(df_comp['date'] >= sTime,df_comp['date'] < eTime)]

        # Calculate Total Great Circle Path Distance
        lat1, lon1          = df['de_lat'],df['de_lon']
        lat2, lon2          = df['dx_lat'],df['dx_lon']
        R_gc                = Re*geopack.greatCircleDist(lat1,lon1,lat2,lon2)
        df.loc[:,'R_gc']    = R_gc

        # Calculate Band
        df.loc[:,'band']        = np.array((np.floor(df['freq']/1000.)),dtype=np.int)

        return df
