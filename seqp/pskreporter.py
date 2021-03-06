from collections import OrderedDict
import pandas as pd
import numpy as np

import tqdm

from .data import clean_call

def get_df(csv_file,qth_locator=None):
    """
    Reads a PSKReporter CSV generated by generate_seqp_csv() into a
    pandas dataframe that has been cleaned and formatted for science
    analysis.
    """
    print('Getting PSKReporter dataframe...')
    df  = pskr_csv_to_df(csv_file)
    df['grid_src_0']            = 'pskr'
    df['grid_src_1']            = 'pskr'

    keys = OrderedDict()
#    keys['sequenceNumber']      = 'sequenceNumber'
#    keys['senderInfoId']        = 'senderInfoId'
#    keys['receiverInfoId']      = 'receiverInfoId'
#    keys['source']              = 'source'
#    keys['senderStatus']        = 'senderStatus'
#    keys['iMD']                 = 'iMD'
#    keys['ipOriginId']          = 'ipOriginId'
#    keys['senderMobileLocator'] = 'senderMobileLocator'
#    keys['code0']               = 'code0'
#    keys['code1']               = 'code1'
#    keys['band']                = 'band'
    keys['flowStartSeconds']    = 'datetime'
    keys['frequency']           = 'frequency'
    keys['mode']                = 'mode'
    keys['receiver_call']       = 'call_0'
    keys['receiver_grid']       = 'grid_0'
    keys['grid_src_0']          = 'grid_src_0'
    keys['sNR']                 = 'srpt_0'
    keys['sender_call']         = 'call_1'
    keys['sender_grid']         = 'grid_1'
    keys['grid_src_1']          = 'grid_src_1'

    kys = [x for x in keys.keys()]
    df  = df[kys]
    df  = df.rename(columns=keys)

    for key in ['call_0','call_1']:
        df[key] = df[key].apply(clean_call)

    if qth_locator is not None:
        # The four character locators are provided by the sender (which the receiver then reports). 
        # The six character ones are typically reported by the receiver for themselves based on what 
        # they entered into wsjt-x. I do lookups to a couple of places to try and improve the precision,
        # but only replace reported locators if the reported locator is a prefix of what I get from 
        # external sources. These do not include QRZ as their license does not permit my use. 
        #  - Philip Gladstone, 30 April 2018

        tqdm.tqdm.pandas(tqdm.tqdm,leave=True)

        print('Geolocating PSKReporter dataframe...')
        for key in [0,1]:
            grid_k      = 'grid_{!s}'.format(key)
            grid_src_k  = 'grid_src_{!s}'.format(key)
            call_k      = 'call_{!s}'.format(key)

            print('{} --> {}'.format(call_k,grid_k))

            result              = df[call_k].progress_apply(qth_locator)
            grids, grid_srcs    = zip(*result)

            grids               = np.array(grids)           
            grid_srcs           = np.array(grid_srcs)

            tf  = np.logical_not(np.logical_or( pd.isnull(grid_srcs),
                                                grid_srcs=='qrz'    ))

            df.loc[tf,grid_k]       = grids[tf]
            df.loc[tf,grid_src_k]   = grid_srcs[tf]

    # Set Dataframe to MHz.
    df['frequency'] = df['frequency']/1.e6

    df['source']    = 'pskreporter'

    return df

def pskr_csv_to_df(csv_file):
    """
    Reads a PSKReporter CSV generated by generate_seqp_csv() into a
    pandas dataframe.
    """
    df = pd.read_csv(csv_file,parse_dates=[9])
    return df
