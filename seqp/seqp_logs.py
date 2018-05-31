import os,glob
import datetime
import dateutil
import logging
from collections import OrderedDict

import mysql.connector

import numpy as np
import pandas as pd
import tqdm

# QRZ Username/Password must be stored in qrz_settings.cfg
from qrz import QRZ
qrz = QRZ(cfg='./qrz_settings.cfg')

#from hamtools import qrz

from .data import clean_call
from . import gen_lib
prep_output = gen_lib.prep_output
from . import locator
grid_valid  = locator.grid_valid

class LogFilter(object):
    def __init__(self,level):
        """
        Convenience class to send different log levels to
        different output files.
        """
        self.__level = level

    def filter(self,logRecord):
        return logRecord.levelno == self.__level

def seqp_logs_to_df(log_dir='data/seqp/submitted_logs',output_dir=None):
    """
    Load SEQP Logs into a Data Frame.
    """
    # START-OF-LOG: 2.0
    # ARRL-SECTION: KS
    # CALLSIGN: NJ0P
    # CLUB: Pilot Knob Amateur Radio Club
    # CONTEST: ECLIPSE-QSO
    # CATEGORY-OPERATOR: SINGLE-OP
    # CATEGORY-MODE:    CW
    # CATEGORY-POWER: 5W QRP
    # CATEGORY-STATION: PORTABLE
    # CATEGORY-TRANSMITTER: ONE
    # SOAPBOX: Operated outside, in a public park in Reserve, Kansas EM29FX
    # SOAPBOX: Operated during totality.  Ground conductivity 22 mhos estimated
    # SOAPBOX: Elecraft KX2, battery, packtenna random wire with 9:1 balun up in tree
    # CLAIMED-SCORE: 0
    # OPERATORS: NJ0P
    # NAME: Rick Reichert
    # ADDRESS: 620 S Hickory Trl
    # ADDRESS: Lansing, KS 66043
    # ADDRESS: USA
    # CREATED-BY: N1MM Logger+
    # QSO: 14038 CW 2017-08-21 1555 NJ0P          599  K3JT          569  EM99XO
    # QSO: 14035 CW 2017-08-21 1558 NJ0P          599  W6RW          599  DM22QR
    # QSO: 14031 CW 2017-08-21 1645 NJ0P          599  AA3B          599  FN20EI
    # QSO: 14026 CW 2017-08-21 1702 NJ0P          599  K3WW          579  FN20IJ
    # QSO: 14033 CW 2017-08-21 1719 NJ0P          599  W4AU          559  FM19DD
    # QSO: 14029 CW 2017-08-21 1803 NJ0P          579  W7IY          559  FM18GP
    # QSO: 14019 CW 2017-08-21 1812 NJ0P          599  K1EO          599  FN44NS
    # END-OF-LOG:
    ### Create log files for call signs properly parsed and not.
    log         = logging.getLogger()

    if output_dir is not None:
        handler_1   = logging.FileHandler(os.path.join(output_dir,'seqp_info.log'),mode='w')
        handler_1.setLevel(logging.INFO)
        handler_1.addFilter(LogFilter(logging.INFO))
        log.addHandler(handler_1)

        handler_2   = logging.FileHandler(os.path.join(output_dir,'seqp_error.log'),mode='w')
        handler_2.setLevel(logging.ERROR)
        handler_2.addFilter(LogFilter(logging.ERROR))
        log.addHandler(handler_2)

    log.setLevel(logging.INFO)

    ### Find Log Files ###
    # Need to descend a level for calls with /suffixes.
    dirs    = [os.path.join(log_dir,x) for x in next(os.walk(log_dir))[1]]
    dirs    = [log_dir] + dirs

    files   = []
    for dr in dirs:
        tmp     = glob.glob(os.path.join(dr,'*.log'))
        files   = files + tmp

    # Write all QSOs to a text file as a sanity check.
    if output_dir is not None:
        raw_out_path    = os.path.join(output_dir,'qso_raw.txt')
        with open(raw_out_path,'w') as fl:
            pass

    df_lst  = []
    for fle in files:
        print('Processing {!s}...'.format(fle))
        for encoding in ['utf-8','ISO-8859-1']:
            try:
                with open(fle,'U',encoding=encoding) as fl:
                    raw = fl.read().splitlines()

                rpl_strs = [u'\xa0',u'\u00FF',"\\'a0",'\\']
                for rpl_str in rpl_strs:
                    raw = [x.replace(rpl_str, u' ') for x in raw]

                raw = [x.replace(u'\xad', u'-') for x in raw]

                encoding_error = False
                break
            except UnicodeDecodeError:
                encoding_error = True
                continue

            if encoding_error:
                logging.error('UnicodeDecodeError ({!s}): {!s}'.format(encoding,fle))

        try: 
            callsign            = None
            single_op           = True
            for line in raw:
                spl = line.split(':')

                if spl[0].upper() == 'QSO':
                    # Write all QSOs to a text file as a sanity check.
                    if output_dir is not None:
                        with open(raw_out_path,'a') as fl:
                            fl.write(line+'\n')

                if spl[0].upper() == 'CATEGORY-OPERATOR':
                    if 'MULTI' in spl[1]:
                        single_op = False

                if spl[0].upper() == 'OPERATORS':
                    operators = len(spl[1].split())
                    if operators > 1:
                        single_op = False

                # Parse Soapbox
                soapbox = {}
                if spl[0].upper() == 'SOAPBOX':
                    tmp = spl[1].split(',')
                    for val in tmp:
                        try:
                            sb_line = val.split('=')
                            soapbox[sb_line[0].upper().strip()] = sb_line[1].strip()
                        except:
                            pass

                # Parse QSO Line
                if spl[0].upper() == 'QSO':
                    qso                 = spl[1].split()
                    qso_dct             = {}
                    qso_dct['freq']     = freq_check(qso[0])
                    qso_dct['mode']     = qso[1].upper()

                    date_str            = qso[2:4]
                    try:
                        qso_dct['datetime'] = dateutil.parser.parse(' '.join(date_str))
                    except:
                        qso_dct['datetime'] = np.datetime64('NaT')
                        logging.error('QSO DateParse Error: {!s} ({!s})'.format(fle,date_str))

                    for val in qso[4:]:
                        val = db_check(val)

                        key_list = []
                        key_list.append(('call_0','call'))
                        key_list.append(('rst_0', 'sig_rpt'))
                        key_list.append(('grid_0','grid'))
                        key_list.append(('call_1','call'))
                        key_list.append(('rst_1', 'sig_rpt'))
                        key_list.append(('grid_1','grid'))

                        next_val    = False
                        for key,fcheck in key_list:
                            if not key in qso_dct and not next_val:
                                if field_check(val) == fcheck:
                                    qso_dct[key]    = val.upper()
                                    next_val = True
                                else:
                                    if key == 'grid_sent':
                                        qso_dct[key]    = soapbox.get('GRID')
                                    else:
                                        qso_dct[key]    = None

                        qso_dct['power']                = soapbox.get('POWER')
                        qso_dct['single_op']            = single_op
                        qso_dct['log_file']             = os.path.basename(fle)

#                    qso_dct['call_sent']      = qso[4].upper()
#                    qso_dct['rst_sent']     = qso[5]
#                    qso_dct['grid_sent']    = qso[6].upper()
#                    qso_dct['call_rx']      = qso[7].upper()
#                    qso_dct['rst_rx']       = qso[8]
#                    qso_dct['grid_rx']      = qso[9].upper()
                    df_lst.append(qso_dct)
            logging.info('Processed: {!s}'.format(fle))
        except Exception as ex:
            logging.error('Parsing error: {!s}'.format(fle))

    df  = pd.DataFrame(df_lst)
    for key in ['call_0','call_1']:
        df[key] = df[key].apply(clean_call)
    return df

# SEQP Geolocation Code ########################################################

def field_check(field):
    """
    Guess the parameter type in a Cabrillo file:
        'sig_rpt'
        'call'
        'grid'
    """
    ptype   = None

    try:
        val     = float(field)
        ptype   = 'sig_rpt'
    except:
        pass

    if len(field) >= 4:
        if  (field[0].isalpha() and field[1].isalpha() 
                and field[2].isdigit() and field[3].isdigit()):
            ptype = 'grid'

    if ptype is None and len(field) >= 3:
        ptype   = 'call'

    return ptype

def freq_check(val):
    band = {}
    band[160]   =   1800.
    band[80]    =   3500.
    band[60]    =   5330.5
    band[40]    =   7000.
    band[30]    =  10100.
    band[20]    =  14000.
    band[17]    =  18068.
    band[15]    =  21000.
    band[12]    =  24890.
    band[10]    =  28000.
    band[6]     =  50000.
    band[2]     = 144000.
    lval    = val.lower()
    if lval[-1] == 'm':
        val = int(lval.rstrip('m'))
        val = band.get(val)
    elif float(val) < 1000.:
        val = float(val)*1000.
    else:
        val = float(val)
    return val


def db_check(val):
    lval    = val.lower()
    if lval[-2:] == 'db':
        try:
            val = str(float(lval.rstrip('db')))
        except:
            pass
    return val

class GetGrid(object):
    """
    Find the most common 6-char grid in a qth_dct for a
    given source.
    """
    def __init__(self,call,source,qth_dct):
        self.grid       = None
        self.grid_count = None
        self.call       = call
        self.source     = source

        try:
            call_dct        = qth_dct[call.upper()]
        except:
            return

        self.call_dct   = call_dct
        self.call       = call.upper()
        self.source     = source


        # Test if source exists.
        if source not in call_dct:
            return
        
        # Get list of valid grids.
        valid_grids     = []
        grid_lens       = []
        for grid,frq in call_dct[source]:
            if grid_valid(grid):
                valid_grids.append( (grid,frq) )
                grid_lens.append(len(grid))

        # Make sure we have valid grids to work with.
        if len(valid_grids) == 0:
            return

        for char_len in [4,6]:
            if char_len in grid_lens:
                self.grid,self.grid_count = valid_grids[grid_lens.index(char_len)]

        return

    def __str__(self):
        return '{!s} {!s} {!s} {!s}'.format(self.call,self.grid,self.source,self.grid_count)

    def __bool__(self):
        return bool(self.grid)

class SeqpQTH(object):
    def __init__(self,log_dir=None,output_dir=None,df=None,qrz_lookup=True):
        """
        Analyze SEQP submitted logs and database to determine best
        gridsquare for each QSO.
        """
        if df is None:
            df  = seqp_logs_to_df(log_dir,output_dir)

        self.log_df     = df
        self.qrz_lookup = qrz_lookup
        self.__create_qth_dict(df)
        self.__update_from_sql()
        self.__qth_count()
        self.__create_qth_df()
        self.__cache_dict   = {}

    def __call__(self,call):
        result  = self.__cache_dict.get(call)
        if result is not None:
            return result

        gg_obj  = self.find_qth(call)

        grid    = gg_obj.grid
        grd_src = gg_obj.source

        if grid is None and self.qrz_lookup:
            try:
                qz_obj  = qrz.callsign(call)
                grid    = qz_obj['grid']
                grd_src = 'qrz'
            except:
                pass

        if grid is None:
            grd_src = None

        result  = (grid,grd_src)
        self.__cache_dict[call] = result
        return result

    def __create_qth_dict(self,df):
        """
        Put the call and grid from each QSO into a dictionary.
        """
        qth_dct = {}
        print('Building QTH Dictionary...')
        for inx,row in tqdm.tqdm(df.iterrows(),total=len(df)):
            sfxs     = [('sent',0),('rx',1)]
            for sfx in sfxs:
                try:
                    call    = row['call_{!s}'.format(sfx[1])].upper()
                    grid    = row['grid_{!s}'.format(sfx[1])].upper()

                    if call not in qth_dct:
                        qth_dct[call] = {}

                    if 'seqp_'+sfx[0] not in qth_dct[call]:
                        qth_dct[call]['seqp_'+sfx[0]] = []

                    qth_dct[call]['seqp_'+sfx[0]].append(grid)
                except:
                    pass

        self.qth_dict = qth_dct

    def __update_from_sql(self,user='hamsci', password='hamsci', host='localhost', database='hamsci_rsrch'):
        """
        Update qth_dict with values from submitted to hamsci.org.
        """
        cnx     = mysql.connector.connect(user=user, password=password,host=host, database=database)
        crsr    = cnx.cursor()
        query   = ("SELECT callsign,per_gs FROM seqp_submissions")
        crsr.execute(query)

        sfx     = 'seqp_submitted'
        for callsign, per_gs in crsr:
            try:
                call    = callsign.upper()
                grid    = per_gs.upper()

                if call not in self.qth_dict:
                    self.qth_dict[call] = {}

                if sfx not in self.qth_dict[call]:
                    self.qth_dict[call][sfx] = []

                self.qth_dict[call][sfx].append(grid)
            except:
                pass
        crsr.close()
        cnx.close()

    def __qth_count(self):
        """
        Count the frequency of grids used in an array.
        This method modifies self.qth_dict.
        """
        qth_cnt = {}
        print('Counting QTHs in dictionary...')
        for call,dct_0 in tqdm.tqdm(self.qth_dict.items()):
            if call not in qth_cnt: qth_cnt[call] = {}
            for srx, grids in dct_0.items():
                if srx not in qth_cnt[call]:
                    qth_cnt[call][srx] = {}
                this_dct    = qth_cnt[call][srx]

                grids_unq   = list(set(grids))
                grids_unq.sort()

                for grid in grids_unq:
                    cnt             = np.count_nonzero(np.array(grids) == grid)
                    this_dct[grid]  = cnt 

                td_sorted = (sorted(this_dct.items(), key=lambda x:x[1]))[::-1]
                qth_cnt[call][srx] = td_sorted

        self.qth_dict   = qth_cnt

    def __create_qth_df(self):
        """
        Creates a QTH dataframe from the QTH dictionary.
        """
        inx_lst     = []
        qth_df_lst  = []
        calls       = list(self.qth_dict.keys())
        calls.sort()
        for call in calls: 
            grid_obj    = self.find_qth(call)

            inx_lst.append(call)
            tmp             = {}
            tmp['grid']     = grid_obj.grid
            tmp['count']    = grid_obj.grid_count
            tmp['source']   = grid_obj.source

            qth_df_lst.append(tmp)

        qth_df              = pd.DataFrame(qth_df_lst,index=inx_lst)
        qth_df['grid_len']  = qth_df.grid.apply(lambda x: 0 if x is None else len(x))
        self.qth_df         = qth_df
        return qth_df

    def find_qth(self,call):
        """
        Determine the most appropriate QTH in the self.qth_dict for
        for a particular call.
        """
        submitted   = GetGrid(call,'seqp_submitted',self.qth_dict)
        sent        = GetGrid(call,'seqp_sent',self.qth_dict)
        rx          = GetGrid(call,'seqp_rx',self.qth_dict)

        ret = None

        if submitted and sent and rx:
            if submitted == sent:
                ret = submitted
            elif sent == rx:
                ret = sent
            else:
                ret = submitted

        elif submitted: 
            ret = submitted

        elif sent:
            ret = sent

        else:
            ret = rx

        return ret

    def print_stats(self):
        df  = self.qth_df
        # Count GS Lengths
        sources = list(df.source.unique())
        vals    = df.grid_len.unique()

        stat_lst    = []
        for source in sources:
            tmp = {}
            for val in vals:
                key         = '{!s}-Char'.format(val)
                tf          = np.logical_and(df.grid_len==val,df.source==source)
                cnt         = np.count_nonzero(tf)
                tmp[key]    = cnt

                key = 'All-Char'
                cnt = np.count_nonzero(df.source==source)
                tmp[key]    = cnt
            stat_lst.append(tmp)

        source  = 'all_sources'
        sources.append(source)
        tmp = {}
        for val in vals:
            key         = '{!s}-Char'.format(val)
            cnt         = np.count_nonzero(df.grid_len==val)
            tmp[key]    = cnt

        # All char lengths
        key = 'All-Char'
        cnt = np.count_nonzero(df.grid_len)
        tmp[key]    = cnt

        stat_lst.append(tmp)

        stat_df = pd.DataFrame(stat_lst,index=sources)
        print(stat_df)
        return stat_df

    def to_ascii(self,filename='qths.txt'):
        """Write all results out to a text file."""
        with open(filename,'w') as fl_qth:
            for key,val in self.qth_dict.items():
                    fl_qth.write('{!s}: {!s}\n'.format(key, val))

def get_df(log_input,output_dir=None):
    """
    Get the SEQP log entries in a dataframe for scientific processing.
        log_input: Either the path to directory containing SEQP log files,
            OR a SeqpQTH object.
    """
    print('Getting SEQP Logs dataframe...')

    if hasattr(log_input,'log_df'):
        qth = log_input
        df  = qth.log_df
    else:
        # Load QSOs from log files.
        df  = seqp_logs_to_df(log_input,output_dir)

        # Determine best QTH for each call based on submitted, sent, and received
        # information.
        qth = SeqpQTH(df)

    # Apply QTH information to log dataframe and compare with originally logged results.
    df              = df.copy()
    df              = df.rename(columns={'grid_0':'log_grid_0','grid_1':'log_grid_1'})

    df['grid_0']    = None
    df['grid_1']    = None
    grid_0          = []
    grid_src_0      = []
    grid_1          = []
    grid_src_1      = []

    print('Recomputing locations based on submitted, sent, and received reports...')
    for inx,row in tqdm.tqdm(df.iterrows(),total=len(df)):
        qo  = qth.find_qth(row['call_0'])
        grid_0.append(qo.grid)
        grid_src_0.append(qo.source)

        qo  = qth.find_qth(row['call_1'])
        grid_1.append(qo.grid)
        grid_src_1.append(qo.source)
    
    df['grid_0']        = grid_0
    df['grid_src_0']    = grid_src_0
    df['grid_1']        = grid_1
    df['grid_src_1']    = grid_src_1

    # Check to see where the newly assigned grid square does not match
    # the reported one. Print out a table of this.
    if output_dir is not None:
        df_tmp  = df[np.logical_or(df['grid_0'] != df['log_grid_0'],
                                   df['grid_1'] != df['log_grid_1'])]
        keys    = []
        keys.append('call_0')
        keys.append('log_grid_0')
        keys.append('grid_0')
        keys.append('grid_src_0')
        keys.append('call_1')
        keys.append('log_grid_1')
        keys.append('grid_1')
        keys.append('grid_src_1')
        with open(os.path.join(output_dir,'bad_grid.txt'),'w') as fl:
            fl.write(df_tmp[keys].to_string())
    
    # Return data frame with selected columns.
    keys    = OrderedDict()
    keys['datetime']        = 'datetime'
    keys['freq']            = 'frequency'
    keys['mode']            = 'mode'
    keys['call_0']          = 'call_0'
    keys['grid_0']          = 'grid_0'
    keys['grid_src_0']      = 'grid_src_0'
    keys['rst_0']           = 'srpt_0'
    keys['call_1']          = 'call_1'
    keys['grid_1']          = 'grid_1'
    keys['grid_src_1']      = 'grid_src_1'
    keys['rst_1']           = 'srpt_1'
    keys['single_op']       = 'single_op'
    keys['log_file']        = 'log_file'
    
    kys = [x for x in keys.keys()]
    df  = df[kys]
    df  = df.rename(columns=keys)

    # Set frequency to MHz.
    df['frequency'] = df['frequency']/1000.

    # ID Source
    df['source']            = 'seqp_logs'
    df  = df.drop_duplicates()
    return df
