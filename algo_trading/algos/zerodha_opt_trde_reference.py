# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 10:35:14 2021
This file has the class approach to reduce the global variable count in file trde_fun
@author: Prvnkmr
"""
# import django_standalone
import pandas as pd
from pathlib import Path
import numpy as np
from datetime import datetime,date,timedelta
import time
import os
from django.utils import timezone
from functools import wraps
from algo_trading.settings import DEBUG
import orjson
from flatten_json import flatten
import logging

if DEBUG:
    from kalai.api import TickStore, fetch_ticker_data_from_redis
    from kalai.zerodha_utils import ZerodhaUtility
    from kalai.models import my_logger,AlgoInfo,StopLoss,ProcessedTickStore,AlgoOut
    log_file_path = 'logs/'
    data_file_path = 'data/'
    # redis_client = redis.StrictRedis(
    #     host="localhost",
    #     port=6379,
    #     db=0,
    #     decode_responses=True,
    # )
else:
    from .api import  TickStore, fetch_ticker_data_from_redis
    from .zerodha_utils import ZerodhaUtility
    from .models import my_logger,AlgoInfo,StopLoss,ProcessedTickStore,AlgoOut
    log_file_path ='kalai/logs/'
    data_file_path = 'kalai/data/'
    # redis_client = redis.StrictRedis(
    #     host="redis",
    #     port=6379,
    #     db=0,
    #     decode_responses=True,
    # )

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(funcName)s line-(%(lineno)d)  %(message)s\n\r')
logger = logging.getLogger("TaskScheduler")
general_logger = my_logger('general_logger', log_file_path+'general.log')
trade_logger = my_logger('second_logger', log_file_path+'trade.log')
strike_logger = my_logger('third_logger', log_file_path+'strike_logger.log')
# opt_tkn_update_logger = my_logger('opt_tkn_update_logger', log_file_path+'opt_tkn_update_logger.log')
inst_analysis_logger = my_logger('inst_analysis_logger', log_file_path+'inst_analysis_logger.log')
execution_time_logger = my_logger('execution_time_logger', log_file_path+'execution_time_logger.log')

def timeit(func):
    """
    Decorator for measuring function's running time.
    """
    @wraps(func)
    def measure_time(*args, **kw):
        start_time = time.time()
        result = func(*args, **kw)
        execution_time = time.time() - start_time
        if DEBUG:
            execution_time_logger.info('%s ' % (f"Method '{func.__name__}' took {execution_time} seconds to execute"))
        return result

    return measure_time

def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)
    wrapper.has_run = False
    return wrapper
# @timeit
class TradeAlgo():

    def __init__(self, disco_bro=None):
        self.client = disco_bro
        self.special_session = False  # change here to activate special session
        self.day_light_saving = True
        self.nfo_risk_hold = False
        self.input_file = data_file_path+'token_ref.xlsx'
        self.output_file = data_file_path+'cum_table.xlsx'
        self.master_list = data_file_path+'Master_inst_token.xlsx'
        self.stock_config = pd.read_excel(self.input_file, sheet_name='nfo_config',engine='openpyxl')
        self.nse_holiday_info = pd.read_excel(self.input_file, sheet_name='nse_holiday_info',engine='openpyxl')
        self.nse_holiday_info['Date'] = pd.to_datetime(self.nse_holiday_info['Date'])
        self.stock_data_info = pd.read_excel(self.input_file, sheet_name='nfo_list', converters={'Scan_window': np.int32},engine='openpyxl')
        # self.loss_table_info = pd.read_excel(self.input_file, sheet_name='nse_stoploss',engine='openpyxl')
        self.loss_table_info = pd.read_excel(self.input_file, sheet_name='stoploss_tbl', engine='openpyxl',header=[0, 1])
        self.der_loss_table_info = pd.read_excel(self.input_file, sheet_name='derloss_tbl', engine='openpyxl',header=[0, 1])
        self.update_config_info()
        self.percent_of_capital_utilization = self.stock_config.iloc[7, 1]
        self.cap_config = self.stock_config.iloc[1:5, 4:]
        self.cap_config.columns = self.cap_config.iloc[0]
        self.cap_config = self.cap_config.drop(self.cap_config.index[0]).reset_index(drop=True)
        self.debounce_counter_threshold = self.stock_config.iloc[8, 1]
        self.order_pending_counter_threshold = self.stock_config.iloc[12, 1]  # create a counter once orders is placed
        self.margin_per_stock = self.stock_config.iloc[9, 1]
        self.live_balance_lower_limit = self.stock_config.iloc[10, 1]
        try:
            self.master_tkn_list_update_time = pd.read_json(orjson.loads(flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='zerodha_master_token_list'))['zerodha_master_token_list']))
        except:
            self.master_tkn_list_update_time = pd.DataFrame([{'updated_time':datetime.today()-timedelta(days = 2)}])
        self.buy_ttl_value = self.stock_config.iloc[14, 1]
        self.sell_ttl_value = self.stock_config.iloc[15, 1]
        self.strike_choice_CE = self.stock_config.iloc[16, 1]
        self.strike_choice_PE = self.stock_config.iloc[17, 1]
        self.hedge_threshold = self.stock_config.iloc[18, 1]
        self.month_cutoff = 0
        self.week_cutoff = 3
        self.capital_allowed = self.stock_config.iloc[19, 1]
        self.stoploss_threshold = self.stock_config.iloc[20, 1]
        self.hold_frame = self.current_holdings(self.client.holdings())  # to get holdings list
        self.pos_day_frame, self.pos_net_frame = self.client.pos_data()  # to get position list
        self.prev_position = pd.DataFrame([])
        self.buy_stock_cap = pd.DataFrame([])
        self.sell_stock_table = pd.DataFrame([])
        self.cum_table, self.inst_list_int, self.init_ref_list = self.master_tkn_list(self.input_file, self.master_list, self.output_file)
        self.open_positions = self.open_position_update(self.pos_day_frame, self.pos_net_frame)
        self.order_status = self.order_status_update()  # to get order list
        # self.CE_signal = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'CE_signal'])  # store previous value
        # self.PE_signal = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'PE_signal'])  # store previous value
        # self.CE_jump = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'CE_jump'])  # store previous value
        # self.PE_jump = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'PE_jump'])  # store previous value
        self.one_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros((len(self.inst_list_int),2)))],columns=['instrument_token', 'buy_counter_CE','buy_counter_PE'])  # counter for judging buy
        self.minus_one_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros((len(self.inst_list_int),2)))],columns=['instrument_token', 'buy_counter_CE','buy_counter_PE'])  # counter for judging buy
        self.minus_two_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros((len(self.inst_list_int),2)))],columns=['instrument_token', 'buy_counter_CE','buy_counter_PE'])  # counter for judging buy
        self.minus_three_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'buy_counter'])  # counter for judging buy
        self.minus_five_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'buy_counter'])  # counter for judging buy
        self.stop_loss_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'stop_loss_counter'])  # counter for orders
        self.hedge_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'hedge_counter'])  # counter for hedging
        self.jump_counter = pd.DataFrame(np.c_[(self.inst_list_int, np.zeros(len(self.inst_list_int)))],columns=['instrument_token', 'jump_counter'])  # counter for Jump function
        self.updated_list = self.token_list_update()
        self.insert_instrument_token(self.updated_list['instrument_token'].values.tolist())
        self.final_ref_tokens = list(set(self.updated_list['instrument_token'].dropna()).intersection(self.cum_table['Ref_stock_tkn'].values))
        self.avail_cash, self.live_balance = self.client.chk_live_bal()
        self.all_ref_tkns = self.cum_table['Ref_stock_tkn'].unique()
        self.read_algo_info_table()
        self.fwd_15_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.fwd_10_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.fwd_30_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.fwd_1_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.fwd_3_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.fwd_60_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.fwd_5_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.ref_min_max_all = pd.DataFrame([],columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.half_day_cdl_all = pd.DataFrame([],columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.day_cdl_all = pd.DataFrame([], columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.prev_day_cdl_all = pd.DataFrame([],columns=['open', 'low', 'high', 'close', 'instrument_token', 'date_time'])
        self.data_ready = False
        self.read_db_candle()
        # self.scan_window = 3600
        self.fetch_window = 10000  # day
        self.tick_data_raw = ProcessedTickStore.get_with_time(seconds=self.fetch_window)
        # self.tick_data_raw = cx.read_sql(self.db_conn,"SELECT data FROM kalai_processedtickstore WHERE timestamp >= NOW() - INTERVAL '1 DAYS'")
        if len(self.tick_data_raw) > 0:
            self.tick_data = pd.DataFrame(orjson.loads(item["data"]) for item in self.tick_data_raw)
            # self.tick_data = pd.json_normalize([orjson.loads(item) for item in (list(map(lambda x: x["data"], self.tick_data_raw)))])
            # self.tick_data['date_time'] = pd.to_datetime(self.tick_data['exchange_timestamp'], format='ISO8601',errors='coerce')#ass
            self.tick_data['date_time'] =pd.to_datetime(self.tick_data['current_time'], format='ISO8601',errors='coerce')
            now = timezone.make_naive(timezone.now())
            analyse_start = pd.to_datetime(now.replace(hour=8, minute=59, second=0, microsecond=0))
            self.tick_data = self.tick_data.sort_values(by='date_time', ascending=True)
            self.tick_data = self.tick_data[(self.tick_data['date_time'] > analyse_start) & (self.tick_data['date_time'] <= now)]
            # self.tick_data_raw = []
        else:
            self.tick_data = pd.DataFrame([])
        if self.tick_data.empty and not DEBUG:
            # self.tick_data_raw = TickStore.get_with_time(seconds=20)
            self.tick_data_raw =fetch_ticker_data_from_redis(broker_name="zerodha")
            if len(self.tick_data_raw)>0:
                self.tick_data = pd.json_normalize(self.tick_data_raw)
            else:
                self.tick_data = pd.DataFrame([])
        elif self.tick_data.empty and DEBUG:
            # self.tick_data_raw = TickStore.get_with_time(seconds=self.scan_window)
            self.tick_data = pd.DataFrame([])
        if not self.tick_data.empty:
            # self.tick_data = pd.json_normalize(self.delta_tick_data_raw)
            # self.tick_data = pd.DataFrame.from_records([orjson.loads(item) for item in (list(map(lambda x: x["data"], self.tick_data_raw)))])
            # self.tick_data['date_time'] = pd.to_datetime(self.tick_data['exchange_timestamp'], format='ISO8601',errors='coerce')#ass
            self.tick_data['date_time'] = pd.to_datetime(self.tick_data['current_time'], format='ISO8601',errors='coerce')
            now = timezone.make_naive(timezone.now())
            analyse_start = pd.to_datetime(now.replace(hour=8, minute=59, second=0, microsecond=0))
            self.tick_data = self.tick_data.sort_values(by='date_time', ascending=True)
            self.tick_data = self.tick_data[(self.tick_data['date_time'] > analyse_start) & (self.tick_data['date_time'] <= now)]
            if DEBUG:
                now = timezone.make_naive(timezone.now())
                analyse_start = pd.to_datetime(now.replace(hour=8, minute=59, second=0, microsecond=0))
                analyse_end = pd.to_datetime(now.replace(hour=16, minute=00, second=10, microsecond=28))
                self.tick_data = self.tick_data.sort_values(by='date_time', ascending=True)
                self.tick_data = self.tick_data[(self.tick_data['date_time'] > analyse_start) & (self.tick_data['date_time'] <= now)]
                # self.tick_data = pd.DataFrame([],columns=['open','low','high','close','instrument_token','date_time'])
                # self.session_ref_data = pd.DataFrame([],columns=['open','low','high','close','instrument_token','date_time'])
                # self.recent_olhc = pd.DataFrame([],columns=['open','low','high','close','instrument_token','date_time'])

            for self.tkn in self.final_ref_tokens:
                if self.tkn in self.cum_table['Ref_stock_tkn'].values and len(self.tick_data) > 0:
                    self.session_ref_data = self.tick_data[self.tick_data['instrument_token'] == self.tkn]
                    self.session_ref_data.reset_index(drop=True, inplace=True)
                    self.session_ref_data = self.session_ref_data.set_index('date_time')
                    self.recent_olhc = self.tick_to_olhc(self.session_ref_data, '1s', 'start_day', 'left', 'left')
                    self.fwd_15_all = pd.concat([self.fwd_15_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='15min',bod=True)], ignore_index=True)
                    self.fwd_10_all = pd.concat([self.fwd_10_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='10min',bod=True)], ignore_index=True)
                    self.fwd_30_all = pd.concat([self.fwd_30_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='30min',bod=True)], ignore_index=True)
                    self.fwd_1_all = pd.concat([self.fwd_1_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='1min',bod=True)], ignore_index=True)
                    self.fwd_3_all = pd.concat([self.fwd_3_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='3min',bod=True)], ignore_index=True)
                    self.fwd_60_all = pd.concat([self.fwd_60_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='60min',bod=True)], ignore_index=True)
                    self.fwd_5_all = pd.concat([self.fwd_5_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='5min',bod=True)], ignore_index=True)
                    self.ref_min_max_all = self.fwd_10_all
                    self.half_day_cdl_all = pd.concat([self.half_day_cdl_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='0.5D',bod=True)], ignore_index=True)
                    self.day_cdl_all = pd.concat([self.day_cdl_all, self.group_by_rolling_window(self.recent_olhc,pd.DataFrame([]), window_size='1D',bod=True)], ignore_index=True)
                    self.data_ready = True # add this to cum table....for every ref token
                    self.recent_olhc = pd.DataFrame([])
                # else:
                #     self.data_ready = False
        else:
            self.tick_data = pd.DataFrame([])
            self.delta_tick_data = pd.DataFrame([])
        self.init_done = True
        self.sl_update()
        general_logger.info('initialization finished')

    def update_config_info(self):
        '''add the data read from excel file only'''

        self.nse_holiday_info_json_old = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='nse_holiday_info'))
        self.nse_holiday_info = self.nse_holiday_info.drop_duplicates(subset='Date',keep='last').reset_index(drop=True)
        if self.nse_holiday_info_json_old == {} and not self.nse_holiday_info.empty:
            self.diff_nse_holiday_info = True
        elif not self.nse_holiday_info.empty and self.nse_holiday_info_json_old['nse_holiday_info'] != '"[]"':
            self.nse_holiday_info_old = pd.read_json(orjson.loads(self.nse_holiday_info_json_old['nse_holiday_info']),orient='split')
            if not self.nse_holiday_info_old.equals(self.nse_holiday_info):
                self.diff_nse_holiday_info = True
            else:
                self.diff_nse_holiday_info = False
        elif self.nse_holiday_info.empty and self.nse_holiday_info_json_old['nse_holiday_info'] != '"[]"':
            self.diff_nse_holiday_info = True
        elif not self.nse_holiday_info.empty and self.nse_holiday_info_json_old['nse_holiday_info'] == '"[]"':
            self.diff_nse_holiday_info = True
        else:
            self.diff_nse_holiday_info = False
        if self.diff_nse_holiday_info:
            logger.info('holiday table updated')
            if not DEBUG:
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='nse_holiday_info',
                    tabledata=orjson.dumps(self.nse_holiday_info.to_json(orient='split'),default=self.json_datetime_converter).decode('utf-8')
                )
                self.nse_holiday_info_json = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='nse_holiday_info'))
                self.nse_holiday_info = pd.read_json(orjson.loads(self.nse_holiday_info_json_old['nse_holiday_info']),orient='split')

        self.stock_data_info_json_old = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='stock_data_info'))
        self.stock_data_info = self.stock_data_info.drop_duplicates(subset='Symbol', keep='last').reset_index(drop=True)
        if self.stock_data_info_json_old == {} and not self.stock_data_info.empty:
            self.diff_stock_data_info = True
        elif not self.stock_data_info.empty and self.stock_data_info_json_old['stock_data_info'] != '"[]"':
            self.stock_data_info_old = pd.read_json(orjson.loads(self.stock_data_info_json_old['stock_data_info']),orient='split')
            if not self.stock_data_info_old.equals(self.stock_data_info):
                self.diff_stock_data_info = True
            else:
                self.diff_stock_data_info = False
        elif self.stock_data_info.empty and self.stock_data_info_json_old['stock_data_info'] != '"[]"':
            self.diff_stock_data_info = True
        elif not self.stock_data_info.empty and self.stock_data_info_json_old['stock_data_info'] == '"[]"':
            self.diff_stock_data_info = True
        else:
            self.diff_stock_data_info = False
        if self.diff_stock_data_info:
            logger.info('stock data table updated')
            if not DEBUG:
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='stock_data_info',
                    tabledata=orjson.dumps(self.stock_data_info.to_json(orient='split'),
                                           default=self.json_datetime_converter).decode('utf-8')
                )
                self.stock_data_info_json = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='stock_data_info'))
                self.stock_data_info = pd.read_json(orjson.loads(self.stock_data_info_json['stock_data_info']),orient='split')

        self.loss_table_info_json_old = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='loss_table_info'))
        # self.loss_table_info = self.loss_table_info.drop_duplicates(subset='price', keep='last').reset_index(drop=True)
        if self.loss_table_info_json_old == {} and not self.loss_table_info.empty:
            self.diff_loss_table_info = True
        elif not self.loss_table_info.empty and self.loss_table_info_json_old['loss_table_info'] != '"[]"':
            self.loss_table_info_old = pd.read_json(orjson.loads(self.loss_table_info_json_old['loss_table_info']),orient="split")
            if not self.loss_table_info_old.equals(self.loss_table_info):
                self.diff_loss_table_info = True
            else:
                self.diff_loss_table_info = False
        elif self.loss_table_info.empty and self.loss_table_info_json_old['loss_table_info'] != '"[]"':
            self.diff_loss_table_info = True
        elif not self.loss_table_info.empty and self.loss_table_info_json_old['loss_table_info'] == '"[]"':
            self.diff_loss_table_info = True
        else:
            self.diff_loss_table_info = False
        if self.diff_loss_table_info:
            logger.info('loss table updated')
            if not DEBUG:
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='loss_table_info',
                    tabledata=orjson.dumps(self.loss_table_info.to_json(orient='split'),
                                           default=self.json_datetime_converter).decode('utf-8')
                )
                self.loss_table_info_json = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='loss_table_info'))
                self.loss_table_info = pd.read_json(orjson.loads(self.loss_table_info_json['loss_table_info']),orient="split")

        self.der_loss_table_info_json_old = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='der_loss_table_info'))
        # self.der_loss_table_info = self.der_loss_table_info.drop_duplicates(subset='price', keep='last').reset_index(drop=True)
        if self.der_loss_table_info_json_old == {} and not self.der_loss_table_info.empty:
            self.diff_der_loss_table_info = True
        elif not self.der_loss_table_info.empty and self.der_loss_table_info_json_old['der_loss_table_info'] != '"[]"':
            self.der_loss_table_info_old = pd.read_json(orjson.loads(self.der_loss_table_info_json_old['der_loss_table_info']),orient="split")
            if not self.der_loss_table_info_old.equals(self.der_loss_table_info):
                self.diff_der_loss_table_info = True
            else:
                self.diff_der_loss_table_info = False
        elif self.der_loss_table_info.empty and self.loss_table_info_json_old['der_loss_table_info'] != '"[]"':
            self.diff_der_loss_table_info = True
        elif not self.der_loss_table_info.empty and self.der_loss_table_info_json_old['der_loss_table_info'] == '"[]"':
            self.diff_der_loss_table_info = True
        else:
            self.diff_der_loss_table_info = False
        if self.diff_der_loss_table_info:
            logger.info('der loss table updated')
            if not DEBUG:
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='der_loss_table_info',
                    tabledata=orjson.dumps(self.der_loss_table_info.to_json(orient='split'),
                                           default=self.json_datetime_converter).decode('utf-8')
                )
                self.der_loss_table_info_json = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='der_loss_table_info'))
                self.der_loss_table_info = pd.read_json(orjson.loads(self.der_loss_table_info_json['der_loss_table_info']),orient="split")



    def sl_update(self):
        if self.pre_trade_sl_update_time():
            self.pre_trde()

    def special_session_chk(self,exchg):
        if (exchg=='MCX' or exchg=='CDS') and self.special_session :
            return not self.special_session
        return self.special_session

    def next_session_closed(self,exchg):
        tomorrow = np.datetime64('today', 'D') +  np.timedelta64(1, 'D')
        if exchg =='NFO' or exchg =='CDS':
            if datetime.today().weekday() == 4 or any(tomorrow == self.nse_holiday_info[(self.nse_holiday_info['Morning_session']=='Closed')]['Date'].values):
                return True
            else:
                return False
        elif exchg =='MCX':
            if datetime.today().weekday() == 4 or any(tomorrow == self.nse_holiday_info[(self.nse_holiday_info['Evening_session']=='Closed')]['Date'].values):
                return True
            else:
                return False
        else:
            return False

    def is_holiday(self,exchg):
        today = np.datetime64('today', 'D')
        if exchg =='NFO' or exchg =='CDS':
            if any(today == self.nse_holiday_info[(self.nse_holiday_info['Morning_session']=='Closed')]['Date'].values):
                return True
            else:
                return False
        elif exchg =='MCX':
            if any(today == self.nse_holiday_info[(self.nse_holiday_info['Evening_session']=='Closed')]['Date'].values):
                return True
            else:
                return False
        else:
            return False

    def clear_tables(self):
        self.cdl_vars = ['self.recent_olhc','self.fwd_15_all','self.fwd_10_all','self.fwd_30_all','self.fwd_1_all','self.fwd_3_all','self.fwd_60_all','self.fwd_5_all',
                         'self.ref_min_max_all','self.half_day_cdl_all','self.day_cdl_all']

        for x in self.cdl_vars:
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename=x[5:],
                    tabledata=orjson.dumps(pd.DataFrame([]).to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                )

    def struct_log(self,tag, data, broker):
        try:
            orjson.loads(data)
        except:
            data = {tag: string for string in data}
        
        AlgoOut.bulk_create(broker=broker, data=data)

    def current_holdings(self,hold):
        if hold != None:
            hold = pd.DataFrame.from_dict(hold)
        else:
            hold = pd.DataFrame([])
        return hold


    def write_db_candle(self):
        '''Writes candle stick data based on variables provided'''
        self.cdl_vars = ['self.fwd_15_all','self.fwd_10_all','self.fwd_30_all','self.fwd_60_all','self.day_cdl_all']
        # if self.session_end(exchg = 'MCX'):
        #     self.cdl_vars = ['self.fwd_15_all','self.fwd_10_all','self.fwd_30_all','self.fwd_60_all','self.day_cdl_all','self.prev_day_cdl_all']
        for x in self.cdl_vars:
            try:
                if not eval(x).empty and not DEBUG:
                    AlgoInfo.create_or_update(
                        broker=self.client.broker,
                        tablename=x[5:],
                        tabledata=orjson.dumps(eval(x).to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                        # tabledata = orjson.dumps(tt.to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                    )
            except:
                continue

    def off_session_cdl_write(self):
        '''Writes candle stick data based on variables provided---- not used now'''
        self.cdl_vars = ['self.fwd_15_all','self.fwd_10_all','self.fwd_30_all','self.fwd_60_all','self.day_cdl_all','self.prev_day_cdl_all']
        # if self.session_end(exchg = 'MCX'):
        #     self.cdl_vars = ['self.fwd_15_all','self.fwd_10_all','self.fwd_30_all','self.fwd_60_all','self.day_cdl_all','self.prev_day_cdl_all']
        for x in self.cdl_vars:
            try:
                if not eval(x).empty and not DEBUG:
                    AlgoInfo.create_or_update(
                        broker=self.client.broker,
                        tablename=x[5:],
                        tabledata=orjson.dumps(eval(x).to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                        # tabledata = orjson.dumps(tt.to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                    )
            except:
                continue

    def read_db_candle(self):
        '''Writes candle stick data based on variables provided'''
        self.cdl_vars = ['self.fwd_15_all','self.fwd_10_all','self.fwd_30_all','self.fwd_60_all','self.day_cdl_all','self.prev_day_cdl_all']

        for x in self.cdl_vars:
            try:
                cdl_data_json = flatten(AlgoInfo.get_table_data(
                    broker=self.client.broker,
                    tablename=x[5:]
                ))
                if not pd.read_json(orjson.loads(cdl_data_json[x[5:]])).empty:
                    exec('{}=pd.read_json(orjson.loads(cdl_data_json[x[5:]]))'.format(x))
            except:
                continue

    def read_algo_info_table(self):
        self.stop_loss_json = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='zerodha_stop_loss'))
        try:
            if len(self.stop_loss_json) == 0 :
                self.stop_loss_info = pd.DataFrame([],columns=['order_id', 'buy_price', 'instrument_token','date_time','tradingsymbol'])
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='zerodha_stop_loss',
                    tabledata=orjson.dumps(self.stop_loss_info.to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                    # Using dict format for consistency
                )

            elif self.stop_loss_json['zerodha_stop_loss'] == '"[]"' :
                self.stop_loss_info = pd.DataFrame([],columns=['order_id', 'buy_price', 'instrument_token','date_time','tradingsymbol'])

            else:
                self.stop_loss_info = pd.read_json(orjson.loads(self.stop_loss_json['zerodha_stop_loss']))
        except:
            self.stop_loss_info = pd.read_json(orjson.loads(self.stop_loss_json['zerodha_stop_loss']))
            general_logger.info('error happened in algoinfo read stop_loss_info')

        self.strike_entry_json = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='zerodha_strike_entry'))
        try:
            if len(self.strike_entry_json) == 0:
                self.strike_entry_info = pd.DataFrame([],columns=['ref_symbol', 'strike_value', 'instrument_token','date_time','tradingsymbol'])
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='zerodha_strike_entry',
                    tabledata=orjson.dumps(self.strike_entry_info.to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                    # Using dict format for consistency
                )

            elif self.strike_entry_json['zerodha_strike_entry'] == '"[]"' :
                self.strike_entry_info = pd.DataFrame([],columns=['ref_symbol', 'strike_value', 'instrument_token','date_time','tradingsymbol'])

            else:
                self.strike_entry_info = pd.read_json(orjson.loads(self.strike_entry_json['zerodha_strike_entry']))
        except:
            self.strike_entry_info = pd.read_json(orjson.loads(self.strike_entry_json['zerodha_strike_entry']))
            general_logger.info('error happened in algoinfo read strike_entry_info')

        self.strike_exit_json = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='zerodha_strike_exit'))
        try:
            if len(self.strike_exit_json) == 0:
                self.strike_exit_info = pd.DataFrame([],columns=['ref_symbol', 'strike_value', 'instrument_token', 'date_time', 'tradingsymbol'])
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='zerodha_strike_exit',
                    tabledata=orjson.dumps(self.strike_exit_info.to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                    # Using dict format for consistency
                )
            elif self.strike_exit_json['zerodha_strike_exit'] == '"[]"':
                self.strike_exit_info = pd.DataFrame([],columns=['ref_symbol', 'strike_value', 'instrument_token', 'date_time', 'tradingsymbol'])

            else:
                self.strike_exit_info = pd.read_json(orjson.loads(self.strike_exit_json['zerodha_strike_exit']))
        except:
            self.strike_exit_info = pd.read_json(orjson.loads(self.strike_exit_json['zerodha_strike_exit']))
            general_logger.info('error happened in algoinfo read strike_exit_info')
        general_logger.info('exited read algo info')

    def update_algo_info_table(self):
        try:
            self.strike_exit_json_old =  flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='zerodha_strike_exit'))
            self.strike_exit_info = self.strike_exit_info.drop_duplicates(subset='instrument_token', keep='last').reset_index(drop=True)
            if self.strike_exit_json_old =={} and not self.strike_exit_info.empty :
                self.diff_strike_exit = True
            elif not self.strike_exit_info.empty and self.strike_exit_json_old['zerodha_strike_exit'] != '"[]"':
                self.strike_exit_info_old = pd.read_json(orjson.loads(self.strike_exit_json_old['zerodha_strike_exit']))
                if len(list(set(self.strike_exit_info_old['strike_value'].values).symmetric_difference(self.strike_exit_info['strike_value'].values)))>0 :
                    self.diff_strike_exit = True
                else:
                    self.diff_strike_exit = False
            elif self.strike_exit_info.empty and  self.strike_exit_json_old['zerodha_strike_exit'] != '"[]"':
                self.diff_strike_exit = True
            elif not self.strike_exit_info.empty and  self.strike_exit_json_old['zerodha_strike_exit'] == '"[]"':
                self.diff_strike_exit = True
            else:
                self.diff_strike_exit = False
            if self.diff_strike_exit :
                logger.info('strike exit table updated')
                if not DEBUG:
                    AlgoInfo.create_or_update(
                        broker=self.client.broker,
                        tablename='zerodha_strike_exit',
                        tabledata=orjson.dumps(self.strike_exit_info.to_json(orient='records'), default=self.json_datetime_converter).decode('utf-8')
                        # Using dict format for consistency
                    )

            self.strike_entry_json_old = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='zerodha_strike_entry'))
            self.strike_entry_info = self.strike_entry_info.drop_duplicates(subset='instrument_token', keep='last').reset_index(drop=True)
            if self.strike_entry_json_old =={} and not self.strike_entry_info.empty :
                self.diff_strike_entry = True
            elif not self.strike_entry_info.empty and self.strike_entry_json_old['zerodha_strike_entry'] != '"[]"':
                self.strike_entry_info_old = pd.read_json(orjson.loads(self.strike_entry_json_old['zerodha_strike_entry']))
                # if len(list(set(self.strike_entry_info_old['instrument_token'].values).symmetric_difference(self.strike_entry_info['instrument_token'].values)))>0:
                if not set(self.strike_entry_info_old['strike_value'].values)==set(self.strike_entry_info['strike_value'].values) or (len(self.strike_entry_info_old) != len(self.strike_entry_info)) :
                    self.diff_strike_entry = True
                    logger.info('strike entry table case1')
                    # logger.info(self.strike_entry_info_old.compare(self.strike_entry_info))
                else:
                    self.diff_strike_entry = False
            elif self.strike_entry_info.empty and  self.strike_entry_json_old['zerodha_strike_entry'] != '"[]"':
                self.diff_strike_entry = True
                logger.info('strike entry table case2')
            elif not self.strike_entry_info.empty and self.strike_entry_json_old['zerodha_strike_entry'] == '"[]"':
                self.diff_strike_entry = True
                logger.info('strike entry table case3')
            else:
                self.diff_strike_entry = False
            if self.diff_strike_entry :
                logger.info('strike entry table updated')
                if not DEBUG:
                    AlgoInfo.create_or_update(
                        broker=self.client.broker,
                        tablename='zerodha_strike_entry',
                        tabledata=orjson.dumps(self.strike_entry_info.to_json(orient='records'), default=self.json_datetime_converter).decode('utf-8')
                        # Using dict format for consistency
                    )
            self.stop_loss_json_old = flatten(AlgoInfo.get_table_data(broker=self.client.broker, tablename='zerodha_stop_loss'))
            self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token', keep='last').reset_index(drop=True)
            if self.stop_loss_json_old =={} and not self.stop_loss_info.empty :
                self.diff_stop_loss = True
            elif  not self.stop_loss_info.empty and self.stop_loss_json_old['zerodha_stop_loss'] != '"[]"':
                self.stop_loss_info_old = pd.read_json(orjson.loads(self.stop_loss_json_old['zerodha_stop_loss']))
                if not set(self.stop_loss_info_old['buy_price'].values)==set(self.stop_loss_info['buy_price'].values) or (len(self.stop_loss_info_old) != len(self.stop_loss_info)):
                    self.diff_stop_loss = True
                    logger.info('stop loss table case1')
                    # logger.info(self.stop_loss_info_old.compare(self.stop_loss_info))
                else:
                    self.diff_stop_loss = False
            elif self.stop_loss_info.empty and  self.stop_loss_json_old['zerodha_stop_loss'] != '"[]"':
                self.diff_stop_loss = True
                logger.info('stop loss table case2')
            elif not self.stop_loss_info.empty and  self.stop_loss_json_old['zerodha_stop_loss'] == '"[]"':
                self.diff_stop_loss = True
                logger.info('stop loss table case3')
            else:
                self.diff_stop_loss =False
            if self.diff_stop_loss :
                logger.info('stop loss table updated')
                if not DEBUG:
                    AlgoInfo.create_or_update(
                        broker=self.client.broker,
                        tablename='zerodha_stop_loss',
                        tabledata=orjson.dumps(self.stop_loss_info.to_json(orient='records'), default=self.json_datetime_converter).decode('utf-8')
                        # Using dict format for consistency
                    )
        except Exception as e:
            general_logger.info(str(e))
            pass
        general_logger.info('exited update algo info')

    def order_status_update(self):
        orders = pd.DataFrame(self.client.orders())
        if not orders.empty:
            orders['Ref_stock'] = [str(self.cum_table[self.cum_table['tradingsymbol']==x]['Ref_stock'].values[0]) for x in orders['tradingsymbol'].values]
            orders['instrument_type'] = [str(self.cum_table[self.cum_table['tradingsymbol']==x]['instrument_type'].values[0]) for x in orders['tradingsymbol'].values]
        general_logger.info('order list updated from broker')
        return orders

    def open_position_update(self, day, net):
        '''it is used to store current open buy position'''

        open_position = pd.DataFrame([])
        # if debug_mode:
        #     net['quantity'] =[50,50]

        if net != None:
            net = pd.read_json(net)
            open_position = net[net['quantity'] > 0]

        # if not day.empty and net.empty:
        #     open_position = day[day['quantity'] > 0]
        # elif not net.empty and day.empty:
        #     open_position = net[net['quantity'] > 0]
        # elif not day.empty and not net.empty:
        #     open_position = pd.concat([day,net])
        #     open_position = open_position[open_position['quantity']>0]
        if not open_position.empty:
            open_position = open_position.drop_duplicates(subset=["instrument_token"])
            open_position['Ref_stock'] = [str(self.cum_table[self.cum_table['tradingsymbol'] == x]['Ref_stock'].values[0]) for x in open_position['tradingsymbol'].values]
            open_position['instrument_type'] = [str(self.cum_table[self.cum_table['tradingsymbol'] == x]['instrument_type'].values[0]) for x in open_position['tradingsymbol'].values]
            open_position.reset_index(drop=True, inplace=True)
        general_logger.info('open position updated from broker')
        # if not open_position.empty:
        #     self.prev_position = open_position
        # else:
        #     open_position = self.prev_position
        return open_position

    @staticmethod
    def json_datetime_converter(obj):
        """Convert `datetime` objects to ISO 8601 strings for JSON serialization."""
        if isinstance(obj, (datetime,date)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    # @timeit
    def master_tkn_list(self, input_file, master_list, output_file):
        ''' it is used to create cum_table used for other calculations'''

        # self.ref_table = pd.read_excel(input_file, sheet_name='ref_list',converters={'Scan_window':np.int32},engine='openpyxl')
        # self.aug_table = pd.concat([self.stock_data, self.ref_table], ignore_index=True)
        self.aug_table = self.stock_data_info
        self.aug_table.reset_index(drop=True, inplace=True)
        # if not debug_mode:
        #     try:
        #         os.remove(output_file)
        #     except:
        #         pass

        if not self.master_tkn_list_update_time.empty:

            self.run_tkn_update = pd.to_datetime(self.master_tkn_list_update_time.values[0]).date<date.today() and self.master_list_update_time()  # update every 20hrs
        elif DEBUG:
            # if not pd.isnull(self.stock_config.iloc[11, 1]):
            #     self.run_tkn_update = pd.Timestamp(self.master_tkn_list_update_time).date()<date.today()
            # else:
            self.run_tkn_update = False
        else:
            self.run_tkn_update = True

        if self.run_tkn_update:
            try:
                os.remove(master_list)
                os.remove(output_file)
            except:
                pass

        # if any((self.run_tkn_update, not Path(master_list).exists())):
        if self.run_tkn_update:
            self.nse_inst_data = self.client.instruments_list('NSE')
            AlgoInfo.create_or_update(
                broker=self.client.broker,
                tablename='nse_instrument_data',
                tabledata=orjson.dumps(self.nse_inst_data, default=self.json_datetime_converter).decode('utf-8')
                # Using dict format for consistency
            )
            self.nse_inst_data = pd.DataFrame(self.nse_inst_data)
            self.nfo_inst_data = self.client.instruments_list('NFO')
            AlgoInfo.create_or_update(
                broker=self.client.broker,
                tablename='nfo_instrument_data',
                tabledata=orjson.dumps(self.nfo_inst_data, default=self.json_datetime_converter).decode('utf-8')
                # Using dict format for consistency
            )
            self.nfo_inst_data = pd.DataFrame(self.nfo_inst_data)
            self.cds_inst_data = self.client.instruments_list('CDS')
            AlgoInfo.create_or_update(
                broker=self.client.broker,
                tablename='cds_instrument_data',
                tabledata=orjson.dumps(self.cds_inst_data, default=self.json_datetime_converter).decode('utf-8')
                # Using dict format for consistency
            )
            self.cds_inst_data = pd.DataFrame(self.cds_inst_data)
            self.mcx_inst_data = self.client.instruments_list('MCX')
            AlgoInfo.create_or_update(
                broker=self.client.broker,
                tablename='mcx_instrument_data',
                tabledata=orjson.dumps(self.mcx_inst_data, default=self.json_datetime_converter).decode('utf-8')
                # Using dict format for consistency
            )
            self.mcx_inst_data = pd.DataFrame(self.mcx_inst_data)
            self.bfo_inst_data = self.client.instruments_list('BFO')
            AlgoInfo.create_or_update(
                broker=self.client.broker,
                tablename='bfo_instrument_data',
                tabledata=orjson.dumps(self.bfo_inst_data, default=self.json_datetime_converter).decode('utf-8')
                # Using dict format for consistency
            )
            self.bfo_inst_data = pd.DataFrame(self.bfo_inst_data)
            self.bse_inst_data = self.client.instruments_list('BSE')
            AlgoInfo.create_or_update(
                broker=self.client.broker,
                tablename='bse_instrument_data',
                tabledata=orjson.dumps(self.bse_inst_data, default=self.json_datetime_converter).decode('utf-8')
                # Using dict format for consistency
            )
            self.bse_inst_data = pd.DataFrame(self.bse_inst_data)
            # if DEBUG:
            #     with pd.ExcelWriter(master_list,engine='openpyxl',mode = 'w') as writer:
            #         self.nse_inst_data.to_excel(writer, sheet_name='NSE', index=False)
            #         self.nfo_inst_data.to_excel(writer, sheet_name='NFO', index=False)
            #         self.cds_inst_data.to_excel(writer, sheet_name='CDS', index=False)
            #         self.mcx_inst_data.to_excel(writer, sheet_name='MCX', index=False)
            #         self.bfo_inst_data.to_excel(writer, sheet_name='BFO', index=False)
            #         self.bse_inst_data.to_excel(writer, sheet_name='BSE', index=False)

            if not DEBUG:
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename='zerodha_master_token_list',
                    tabledata=orjson.dumps(pd.DataFrame([{'updated_time':datetime.today()}]).to_json(orient='records'), default=self.json_datetime_converter).decode('utf-8')
                    # Using dict format for consistency
                )
                general_logger.info('master_token_list updated at ' + str(datetime.today()))
            # self.wb = load_workbook(input_file)
            # self.ws = self.wb['nfo_config']
            # self.ws.cell(row = 13, column = 2).value =str(datetime.today())
            # general_logger.info('master_token_list updated at '+str(datetime.today()))
            # self.wb.save(filename=input_file)
            # self.wb.close()
        else:
            general_logger.info('start read from existing data in db')
            # self.nse_inst_data = AlgoInfo.get_table_data(broker=self.client.broker,tablename='nse_instrument_data')
            self.nse_inst_data = pd.DataFrame(orjson.loads(flatten(AlgoInfo.get_table_data(broker=self.client.broker,tablename='nse_instrument_data'))['nse_instrument_data']))
            self.nfo_inst_data = pd.DataFrame(orjson.loads(flatten(AlgoInfo.get_table_data(broker=self.client.broker,tablename='nfo_instrument_data'))['nfo_instrument_data']))
            self.cds_inst_data = pd.DataFrame(orjson.loads(flatten(AlgoInfo.get_table_data(broker=self.client.broker,tablename='cds_instrument_data'))['cds_instrument_data']))
            self.mcx_inst_data = pd.DataFrame(orjson.loads(flatten(AlgoInfo.get_table_data(broker=self.client.broker,tablename='mcx_instrument_data'))['mcx_instrument_data']))
            self.bfo_inst_data = pd.DataFrame(orjson.loads(flatten(AlgoInfo.get_table_data(broker=self.client.broker,tablename='bfo_instrument_data'))['bfo_instrument_data']))
            self.bse_inst_data = pd.DataFrame(orjson.loads(flatten(AlgoInfo.get_table_data(broker=self.client.broker,tablename='bse_instrument_data'))['bse_instrument_data']))
            general_logger.info('master_token_list read from existing file')
            # if DEBUG:
            #     with pd.ExcelWriter(master_list,engine='openpyxl',mode = 'w') as writer:
            #         self.nse_inst_data.to_excel(writer, sheet_name='NSE', index=False)
            #         self.nfo_inst_data.to_excel(writer, sheet_name='NFO', index=False)
            #         self.cds_inst_data.to_excel(writer, sheet_name='CDS', index=False)
            #         self.mcx_inst_data.to_excel(writer, sheet_name='MCX', index=False)
            #         self.bfo_inst_data.to_excel(writer, sheet_name='BFO', index=False)
            #         self.bse_inst_data.to_excel(writer, sheet_name='BSE', index=False)
        self.nfo_cds_mcx = pd.concat([self.nfo_inst_data, self.cds_inst_data, self.mcx_inst_data,self.bfo_inst_data])
        self.nfo_cds_mcx.reset_index(drop=True, inplace=True)

        for nn in range(len(self.nfo_cds_mcx)):  # need to add mcx tokens analysis (it has no weekely derivatives but has 2 monthly futures combined)
            if str(self.nfo_cds_mcx['name'][nn]) == 'nan' or self.nfo_cds_mcx['name'][nn] not in self.nfo_cds_mcx['tradingsymbol'][nn]:
                self.nfo_cds_mcx.drop(nn, inplace=True)

        self.nfo_cds_mcx.reset_index(drop=True, inplace=True)
        self.nfo_cds_mcx['cap'] = ['NA'] * len(self.nfo_cds_mcx)
        self.nfo_cds_mcx['exp_date_list'] = (pd.to_datetime(self.nfo_cds_mcx['expiry'].values) - datetime.today()).days
        self.nfo_cds_mcx['exp_month'] = pd.to_datetime(self.nfo_cds_mcx['expiry'].values).month.values

        # for w in range(len(self.nfo_cds_mcx)):  # need to add mcx tokens analysis (it has no weekely derivatives but has 2 monthly futures combined)
        #
        #     self.match_str = self.nfo_cds_mcx['tradingsymbol'].values[w][len(self.nfo_cds_mcx['name'].values[w]) + 2:len(self.nfo_cds_mcx['name'].values[w]) + 5]
        #     self.match_str1 = self.nfo_cds_mcx['tradingsymbol'].values[w][len(self.nfo_cds_mcx['name'].values[w]) + 2:]
        #     if self.match_str.isdigit() and not self.match_str.isalpha() and '-OPT' in self.nfo_cds_mcx['segment'][w]:
        #         self.nfo_cds_mcx.at[w, 'cap'] = 'weekely_options'
        #     elif self.match_str.isalpha() and '-OPT' in self.nfo_cds_mcx['segment'][w]:
        #         self.nfo_cds_mcx.at[w, 'cap'] = 'monthly_options'
        #     elif self.match_str.isalpha() and self.match_str1.isalpha() and '-FUT' in self.nfo_cds_mcx['segment'][w]:
        #         self.nfo_cds_mcx.at[w, 'cap'] = 'monthly_futures'
        #     elif self.match_str.isdigit() and not self.match_str.isalpha() and '-FUT' in self.nfo_cds_mcx['segment'][w]:
        #         self.nfo_cds_mcx.at[w, 'cap'] = 'weekely_futures'

        # 1. Pre-calculate lengths and extract substrings for the whole column
        name_len = self.nfo_cds_mcx['name'].str.len() + 2
        match_str = self.nfo_cds_mcx.apply(lambda r: r['tradingsymbol'][name_len[r.name]: name_len[r.name] + 3], axis=1)
        match_str1 = self.nfo_cds_mcx.apply(lambda r: r['tradingsymbol'][name_len[r.name]:], axis=1)

        # 2. Define our conditions
        is_digit = match_str.str.isdigit()
        is_alpha = match_str.str.isalpha()
        is_alpha1 = match_str1.str.isalpha()
        is_opt = self.nfo_cds_mcx['segment'].str.contains('-OPT', regex=False)
        is_fut = self.nfo_cds_mcx['segment'].str.contains('-FUT', regex=False)

        conditions = [
            is_digit & ~is_alpha & is_opt,  # weekly options
            is_alpha & is_opt,  # monthly options
            is_alpha & is_alpha1 & is_fut,  # monthly futures
            is_digit & ~is_alpha & is_fut,  # weekly futures
        ]

        choices = [
            'weekely_options',
            'monthly_options',
            'monthly_futures',
            'weekely_futures'
        ]

        # 3. Apply all logic in one go
        self.nfo_cds_mcx['cap'] = np.select(conditions, choices, default=None)
        general_logger.info('aug table loop starts')
        # for v in range(len(self.aug_table)):
        #     # update code fast
        #     if self.aug_table['Exchange_type'][v] == 'cds':
        #         self.cds_fut_derivatives_all = self.nfo_cds_mcx.groupby(['name', 'instrument_type', 'cap']).get_group((self.aug_table['Symbol'][v], 'FUT', 'monthly_futures'))
        #         self.cds_fut_derivatives = self.cds_fut_derivatives_all[self.cds_fut_derivatives_all['exp_date_list'] > self.month_cutoff]
        #         cds_opt_list = self.nfo_cds_mcx.groupby(['name', 'segment', 'cap']).get_group((self.aug_table['Symbol'][v], 'CDS-OPT', 'monthly_options'))
        #         cds_curr_month = min(cds_opt_list[cds_opt_list['exp_month'] > self.month_cutoff]['exp_month'])
        #         # self.cds_ref_info = self.cds_fut_derivatives['tradingsymbol'][self.cds_fut_derivatives['exp_month']==cds_curr_month].values[0]
        #         if not self.cds_fut_derivatives['tradingsymbol'][self.cds_fut_derivatives['exp_month']==cds_curr_month].empty:
        #             self.cds_ref_info = self.cds_fut_derivatives['tradingsymbol'][self.cds_fut_derivatives['exp_month']==cds_curr_month].values[0]
        #         else:
        #             self.cds_ref_info = self.cds_fut_derivatives['tradingsymbol'][self.cds_fut_derivatives['exp_month'] == cds_curr_month + 1].values[0]
        #
        #         self.aug_table.at[v, 'Ref_stock'] = self.cds_ref_info
        #         self.aug_table.at[v, 'Nifty_index'] = self.cds_ref_info
        #         self.temp_cds_info = pd.DataFrame.from_dict(
        #             {'Symbol': self.cds_ref_info, 'Company Name': self.cds_ref_info, 'Industry': self.cds_ref_info,
        #              'Tradable_group': ['Yes'], 'Tradable_stock': ['No'], 'Exchange_type': [np.nan],
        #              'Max_capital_CE': [np.nan], 'Max_capital_PE': [np.nan],
        #              'Max_lots_per_order': self.aug_table.loc[v, 'Max_lots_per_order'], 'Instrument_type': 'Ref_stock',
        #              'Ref_stock': [self.cds_ref_info], 'Nifty_index': [self.cds_ref_info]})
        #         self.aug_table = pd.concat([self.aug_table, self.temp_cds_info])
        #
        #     if self.aug_table['Exchange_type'][v] == 'mcx':  # Can be enabled when above loop for mcx is fixed
        #         self.mcx_fut_derivatives_all = self.nfo_cds_mcx.groupby(['name', 'segment', 'cap']).get_group((self.aug_table['Symbol'][v], 'MCX-FUT', 'monthly_futures'))
        #         self.mcx_fut_derivatives = self.mcx_fut_derivatives_all[self.mcx_fut_derivatives_all['exp_date_list'] > self.month_cutoff]
        #         mcx_opt_list =  self.nfo_cds_mcx.groupby(['name', 'segment', 'cap']).get_group((self.aug_table['Symbol'][v], 'MCX-OPT', 'monthly_options'))
        #         mcx_curr_month = min(mcx_opt_list[mcx_opt_list['exp_month'] > self.month_cutoff]['exp_month'])
        #         if not self.mcx_fut_derivatives['tradingsymbol'][self.mcx_fut_derivatives['exp_month']==mcx_curr_month].empty:
        #             self.mcx_ref_info = self.mcx_fut_derivatives['tradingsymbol'][self.mcx_fut_derivatives['exp_month']==mcx_curr_month].values[0]
        #         else:
        #             self.mcx_ref_info = self.mcx_fut_derivatives['tradingsymbol'][self.mcx_fut_derivatives['exp_month'] == mcx_curr_month + 1].values[0]
        #         self.aug_table.at[v, 'Ref_stock'] = self.mcx_ref_info
        #         self.aug_table.at[v, 'Nifty_index'] = self.mcx_ref_info
        #         self.temp_mcx_info = pd.DataFrame.from_dict(
        #             {'Symbol': self.mcx_ref_info, 'Company Name': self.mcx_ref_info, 'Industry': self.mcx_ref_info,
        #              'Tradable_group': ['Yes'], 'Tradable_stock': ['No'], 'Exchange_type': [np.nan],
        #              'Max_capital_CE': [np.nan], 'Max_capital_PE': [np.nan],
        #              'Max_lots_per_order': self.aug_table.loc[v, 'Max_lots_per_order'], 'Instrument_type': 'Ref_stock',
        #              'Ref_stock': [self.mcx_ref_info], 'Nifty_index': [self.mcx_ref_info]})
        #         self.aug_table = pd.concat([self.aug_table, self.temp_mcx_info],ignore_index=True)
        #
        #     if self.aug_table['Exchange_type'][v] == 'bfo':
        #         self.bfo_fut_derivatives_all = self.nfo_cds_mcx.groupby(['name', 'instrument_type', 'cap']).get_group((self.aug_table['Symbol'][v], 'FUT', 'monthly_futures'))
        #         self.bfo_fut_derivatives = self.bfo_fut_derivatives_all[self.bfo_fut_derivatives_all['exp_date_list'] > self.month_cutoff]
        #         bfo_opt_list = self.nfo_cds_mcx.groupby(['name', 'segment', 'cap']).get_group((self.aug_table['Symbol'][v], 'BFO-OPT', 'monthly_options'))
        #         bfo_curr_month = min(bfo_opt_list[bfo_opt_list['exp_month'] > self.month_cutoff]['exp_month'])
        #         if not self.bfo_fut_derivatives['tradingsymbol'][self.bfo_fut_derivatives['exp_month']==bfo_curr_month].empty:
        #             self.bfo_ref_info = self.bfo_fut_derivatives['tradingsymbol'][self.bfo_fut_derivatives['exp_month']==bfo_curr_month].values[0]
        #         else:
        #             self.bfo_ref_info = self.bfo_fut_derivatives['tradingsymbol'][self.bfo_fut_derivatives['exp_month'] == bfo_curr_month + 1].values[0]
        #
        #         self.aug_table.at[v, 'Ref_stock'] = self.bfo_ref_info
        #         self.aug_table.at[v, 'Nifty_index'] = self.bfo_ref_info
        #         self.temp_mcx_info = pd.DataFrame.from_dict(
        #             {'Symbol': self.bfo_ref_info, 'Company Name': self.bfo_ref_info,
        #              'Industry': self.bfo_ref_info, 'Tradable_group': ['Yes'], 'Tradable_stock': ['No'],
        #              'Exchange_type': [np.nan], 'Max_capital_CE': [np.nan], 'Max_capital_PE': [np.nan],
        #              'Max_lots_per_order': self.aug_table.loc[v, 'Max_lots_per_order'], 'Instrument_type': 'Ref_stock',
        #              'Ref_stock': [self.bfo_ref_info], 'Nifty_index': [self.bfo_ref_info]})
        #         self.aug_table = pd.concat([self.aug_table, self.temp_mcx_info])
        #
        #     if self.aug_table['Exchange_type'][v] == 'nfo':
        #         self.nfo_fut_derivatives_all = self.nfo_cds_mcx.groupby(['name', 'instrument_type', 'cap']).get_group((self.aug_table['Symbol'][v], 'FUT', 'monthly_futures'))
        #         self.nfo_fut_derivatives = self.nfo_fut_derivatives_all[self.nfo_fut_derivatives_all['exp_date_list'] > self.month_cutoff]
        #         nfo_opt_list = self.nfo_cds_mcx.groupby(['name', 'segment', 'cap']).get_group((self.aug_table['Symbol'][v], 'NFO-OPT', 'monthly_options'))
        #         nfo_curr_month = min(nfo_opt_list[nfo_opt_list['exp_month'] > self.month_cutoff]['exp_month'])
        #         if not self.nfo_fut_derivatives['tradingsymbol'][self.nfo_fut_derivatives['exp_month']==nfo_curr_month].empty:
        #             self.nfo_ref_info = self.nfo_fut_derivatives['tradingsymbol'][self.nfo_fut_derivatives['exp_month']==nfo_curr_month].values[0]
        #         else:
        #             self.nfo_ref_info = self.nfo_fut_derivatives['tradingsymbol'][self.nfo_fut_derivatives['exp_month'] == nfo_curr_month + 1].values[0]
        #
        #         self.aug_table.at[v, 'Ref_stock'] = self.nfo_ref_info
        #         self.aug_table.at[v, 'Nifty_index'] = self.nfo_ref_info
        #         self.temp_mcx_info = pd.DataFrame.from_dict(
        #             {'Symbol': self.nfo_ref_info, 'Company Name': self.nfo_ref_info,
        #              'Industry': self.nfo_ref_info, 'Tradable_group': ['Yes'], 'Tradable_stock': ['No'],
        #              'Exchange_type': [np.nan], 'Max_capital_CE': [np.nan], 'Max_capital_PE': [np.nan],
        #              'Max_lots_per_order': self.aug_table.loc[v, 'Max_lots_per_order'], 'Instrument_type': 'Ref_stock',
        #              'Ref_stock': [self.nfo_ref_info], 'Nifty_index': [self.nfo_ref_info]})
        #         self.aug_table = pd.concat([self.aug_table, self.temp_mcx_info])
        # # self.aug_table = self.aug_table.drop_duplicates(subset=['Ref_stock'], keep='last')
        # self.aug_table.reset_index(drop=True, inplace=True)

        # Pre-compute groupby objects ONCE outside the loop
        fut_groups = self.nfo_cds_mcx.groupby(['name', 'instrument_type', 'cap'])
        opt_groups = self.nfo_cds_mcx.groupby(['name', 'cap'])

        new_rows = []  # collect rows instead of concat inside loop

        for symbol, max_lots in zip(self.aug_table['Ref_stock'], self.aug_table['Max_lots_per_order']):

            # Futures & options lookup
            cds_fut_all = fut_groups.get_group((symbol, 'FUT', 'monthly_futures'))
            cds_fut = cds_fut_all[cds_fut_all['exp_date_list'] > self.month_cutoff]
            cds_opt_list = opt_groups.get_group((symbol, 'monthly_options'))

            # Current month
            # cds_curr_month = cds_opt_list.loc[cds_opt_list['exp_month'] > self.month_cutoff, 'exp_month'].min()
            cds_curr_month = cds_opt_list.loc[(cds_opt_list['exp_month'] > self.month_cutoff) & (cds_opt_list['exp_date_list'] == cds_opt_list['exp_date_list'].min()), 'exp_month'].min()

            # Ref stock lookup
            mask_curr = cds_fut['exp_month'] == cds_curr_month
            if mask_curr.any():
                cds_ref_info = cds_fut.loc[mask_curr, 'tradingsymbol'].iat[0]
            else:
                cds_ref_info = cds_fut.loc[(cds_fut['exp_date_list'] == min(cds_fut['exp_date_list'])) & ((pd.to_datetime(cds_fut['expiry'].values) > datetime.today())), 'tradingsymbol'].iat[0]
            if any(cds_fut['exchange'] == 'MCX'):
                index_symbol = cds_ref_info
            else:
                index_symbol = self.aug_table['Nifty_index'][self.aug_table['Ref_stock'] == symbol]
            new_rows.append({
                'Symbol': cds_ref_info,
                'Company Name': cds_ref_info,
                'Industry': cds_ref_info,
                'Tradable_group': 'Yes',
                'Tradable_stock': 'No',
                'Exchange_type': np.nan,
                'Max_capital_CE': np.nan,
                'Max_capital_PE': np.nan,
                'Max_lots_per_order': max_lots,
                'Instrument_type': 'Ref_stock',
                'Ref_stock': cds_ref_info,
                'Nifty_index': index_symbol,
            })

        # Vectorized column assignment — no per-row .at[]
        symbols = [r['Symbol'] for r in new_rows]
        self.aug_table['Index_tkn'] = self.aug_table['Nifty_index'].apply(self.symbol_to_tkn)
        self.aug_table['Ref_stock'] = symbols #replacing existing stock info with futures stock
        # self.aug_table['Nifty_index'] = symbols
        # self.aug_table['Ref_stock_tkn'] = list(map(self.symbol_to_tkn, self.aug_table['Ref_stock']))
        self.aug_table['Ref_stock_tkn'] = self.aug_table['Ref_stock'].apply(self.symbol_to_tkn)
        self.aug_table['Index_tkn'] = np.where(self.aug_table['Index_tkn'] != -1,self.aug_table['Index_tkn'],self.aug_table['Ref_stock_tkn'])
        # self.aug_table['Index_tkn'] = list(map(self.symbol_to_tkn, self.aug_table['Nifty_index']))
        # Single concat at the end
        # self.aug_table = pd.concat([self.aug_table, pd.DataFrame(new_rows)], ignore_index=True)
        general_logger.info('cum table loop starts')
        if any((self.run_tkn_update, not Path(output_file).exists())):
            # Pre-create groupby objects once instead of repeatedly calling groupby
            nfo_groups = self.nfo_cds_mcx.groupby('name')
            nse_groups = self.nse_inst_data.groupby('tradingsymbol')
            bse_groups = self.bse_inst_data.groupby('tradingsymbol')
            cap_groups = self.cap_config.groupby('cap')

            # Collect all chunks first, then concat once at the end
            # cum_chunks = []
            #
            # for d in range(len(self.aug_table['Symbol'])):
            #     # sym_list = self.aug_table['Symbol'][0:len(self.stock_data)].unique()
            #     symbol = self.aug_table['Symbol'][d]
            #
            #     # Check NFO first
            #     if symbol in nfo_groups.groups:
            #         temp_table1 = nfo_groups.get_group(symbol).reset_index(drop=True)
            #
            #         # Vectorized lookup instead of iterating
            #         caps = temp_table1['cap'].values
            #         temp_table2_chunks = [cap_groups.get_group(cap) for cap in caps if cap in cap_groups.groups]
            #
            #         if temp_table2_chunks:
            #             temp_table2 = pd.concat(temp_table2_chunks, ignore_index=True)
            #             temp_table2 = temp_table2.iloc[:, 1:]  # removing column cap
            #             temp_table1 = pd.concat([temp_table1, temp_table2], axis=1)
            #
            #     # Check NSE
            #     elif symbol in nse_groups.groups:
            #         temp_table1 = nse_groups.get_group(symbol).reset_index(drop=True)
            #
            #         # Create temp_table2 more efficiently
            #         default_row = {
            #             'cap': 'NA',
            #             'minimum_lots_to_buy': -1,
            #             'maximum_lots_to_buy': -1,
            #             'lower_price_limit': -1,
            #             'upper_price_limit': -1,
            #             'preference': -1,
            #             'tradable': -1
            #         }
            #         temp_table2 = pd.DataFrame([default_row] * len(temp_table1))
            #         temp_table1 = pd.concat([temp_table1, temp_table2], axis=1)
            #
            #     # Check BSE
            #     elif symbol in bse_groups.groups:
            #         temp_table1 = bse_groups.get_group(symbol).reset_index(drop=True)
            #
            #         # Create temp_table2 more efficiently
            #         default_row = {
            #             'cap': 'NA',
            #             'minimum_lots_to_buy': -1,
            #             'maximum_lots_to_buy': -1,
            #             'lower_price_limit': -1,
            #             'upper_price_limit': -1,
            #             'preference': -1,
            #             'tradable': -1
            #         }
            #         temp_table2 = pd.DataFrame([default_row] * len(temp_table1))
            #         temp_table1 = pd.concat([temp_table1, temp_table2], axis=1)
            #     else:
            #         continue  # Skip if symbol not found
            #
            #     # Replicate aug_table row
            #     aug_repeated = pd.DataFrame(np.repeat(self.aug_table.iloc[d:d + 1].values, len(temp_table1), axis=0),columns=self.aug_table.columns)
            #     temp_table1 = pd.concat([temp_table1, aug_repeated], axis=1)
            #     cum_chunks.append(temp_table1)
            #     # self.cum_table = pd.concat([self.cum_table, temp_table1], ignore_index=True)
            #
            # # Single concat at the end instead of repeated concatenations
            # self.cum_table = pd.concat(cum_chunks, ignore_index=True) if cum_chunks else pd.DataFrame([])

            # ── constants ────────────────────────────────────────────────────────────────
            DEFAULT_COLS = ['cap', 'minimum_lots_to_buy', 'maximum_lots_to_buy',
                            'lower_price_limit', 'upper_price_limit', 'preference', 'tradable']
            DEFAULT_VALS = {'cap': 'NA', 'minimum_lots_to_buy': -1, 'maximum_lots_to_buy': -1,
                            'lower_price_limit': -1, 'upper_price_limit': -1,
                            'preference': -1, 'tradable': -1}

            def _default_block(n: int) -> pd.DataFrame:
                """Return an n-row default cap block without repeated dict creation."""
                return pd.DataFrame({k: pd.array([v] * n) for k, v in DEFAULT_VALS.items()})

            # ── pre-compute once ──────────────────────────────────────────────────────────
            aug_records = self.aug_table.to_dict('records')  # fast row access
            aug_arr = self.aug_table.values  # for np.repeat
            aug_cols = self.aug_table.columns

            cum_chunks: list[pd.DataFrame] = []

            for d, row in enumerate(aug_records):
                symbol = row['Symbol']

                # ── exchange lookup (NFO → NSE → BSE) ────────────────────────────────
                if symbol in nfo_groups.groups:
                    temp_table1 = nfo_groups.get_group(symbol).reset_index(drop=True)

                    # pull all cap blocks in one concat instead of a list-comp + concat
                    valid_caps = [cap for cap in temp_table1['cap'].values
                                  if cap in cap_groups.groups]
                    if valid_caps:
                        cap_block = pd.concat(
                            [cap_groups.get_group(c) for c in valid_caps],
                            ignore_index=True
                        ).iloc[:, 1:]  # drop 'cap' column
                        temp_table1 = pd.concat([temp_table1, cap_block], axis=1)

                elif symbol in nse_groups.groups:
                    temp_table1 = nse_groups.get_group(symbol).reset_index(drop=True)
                    temp_table1 = pd.concat(
                        [temp_table1, _default_block(len(temp_table1))], axis=1
                    )

                elif symbol in bse_groups.groups:
                    temp_table1 = bse_groups.get_group(symbol).reset_index(drop=True)
                    temp_table1 = pd.concat(
                        [temp_table1, _default_block(len(temp_table1))], axis=1
                    )

                else:
                    continue

                # ── replicate aug_table row ───────────────────────────────────────────
                n = len(temp_table1)
                aug_repeated = pd.DataFrame(
                    np.repeat(aug_arr[d:d + 1], n, axis=0),
                    columns=aug_cols
                )
                cum_chunks.append(pd.concat([temp_table1, aug_repeated], axis=1))

            # ── single concat ─────────────────────────────────────────────────────────────
            self.cum_table = pd.concat(cum_chunks, ignore_index=True) if cum_chunks else pd.DataFrame()

            self.cum_table['current_month'] = ['No'] * len(self.cum_table)
            self.cum_table['CE_jump'] = [0]* len(self.cum_table)
            self.cum_table['PE_jump'] = [0] * len(self.cum_table)
            self.cum_table['day_fall'] = [0]* len(self.cum_table)
            self.cum_table['day_rise'] = [0]* len(self.cum_table)
            self.cum_table['recent_sell_order_time'] = [timezone.make_naive(timezone.now()).replace(hour=9, minute=0, second=0, microsecond=0)]* len(self.cum_table)
            self.cum_table['recent_buy_order_time'] = [timezone.make_naive(timezone.now()).replace(hour=9, minute=0, second=0, microsecond=0)] * len(self.cum_table)
            self.cum_table['week_dist'] = pd.to_datetime(self.cum_table['expiry'].values).isocalendar().week.values - datetime.today().isocalendar().week
            self.cum_table['month_dist'] = pd.to_datetime(self.cum_table['expiry'].values).month.values - datetime.today().month
            self.ref_expiry_tbl = self.cum_table[self.cum_table['exp_date_list']>self.month_cutoff].groupby(['Ref_stock','cap'])['exp_date_list'].min().reset_index()
            self.ref_expiry_tbl = self.ref_expiry_tbl[self.ref_expiry_tbl['cap']=='monthly_options']

            # for vv in range(len(self.cum_table)):
            #     if (self.cum_table.loc[vv]['exp_date_list'] >= self.month_cutoff):
            #         if not self.ref_expiry_tbl[self.ref_expiry_tbl['Ref_stock'] == self.cum_table.loc[vv]['Ref_stock']].empty:
            #             if self.cum_table.loc[vv]['exp_date_list'] <= self.ref_expiry_tbl[self.ref_expiry_tbl['Ref_stock']== self.cum_table.loc[vv]['Ref_stock']]['exp_date_list'].values[0]:
            #                 self.cum_table.loc[vv, 'current_month'] = 'Yes'
            #             else:
            #                 self.cum_table.loc[vv, 'current_month'] = 'No'
            # ── pre-compute reusable masks & lookups ──────────────────────────────────────
            general_logger.info('expiry loop starts')
            # 1. rows that meet the date threshold
            mask_cutoff = self.cum_table['exp_date_list'] >= self.month_cutoff

            # 2. ref_expiry lookup: Ref_stock → exp_date_list  (one value per Ref_stock)
            ref_map = self.ref_expiry_tbl.set_index('Ref_stock')['exp_date_list']

            # 3. rows whose Ref_stock exists in ref_expiry_tbl
            mask_ref = self.cum_table['Ref_stock'].isin(ref_map.index)

            # ── combined mask for rows that need evaluation ───────────────────────────────
            active = mask_cutoff & mask_ref

            # ── vectorized exp_date comparison for active rows ────────────────────────────
            mapped_expiry = self.cum_table.loc[active, 'Ref_stock'].map(ref_map)

            self.cum_table.loc[active, 'current_month'] = np.where(
                self.cum_table.loc[active, 'exp_date_list'] <= mapped_expiry,
                'Yes', 'No'
            )
            general_logger.info('expiry loop finished')
            self.cum_table['ATM_ITM_OTM'] = ['NA'] * len(self.cum_table)
            self.cum_table['Buy_strike'] = ['NA'] * len(self.cum_table)
            self.cum_table['buy_signal_PE'] = [0] * len(self.cum_table)
            self.cum_table['buy_signal_CE'] = [0] * len(self.cum_table)
            self.cum_table['current_value'] = [0] * len(self.cum_table)
            # forex_data = self.cum_table[self.cum_table['exchange'] == 'CDS']
            # strike_filtered = self.cum_table[(self.cum_table['strike'] % self.cum_table['Delta_strike'] == 0) & (self.cum_table['exchange'] != 'CDS')]  # strikr filter enabled
            # strike_filtered = self.cum_table[self.cum_table['exchange'] != 'CDS']#strikr filter disabled
            # self.cum_table = pd.concat([strike_filtered, forex_data])

            # self.cum_list_cols = list(self.cum_table.columns)
            # self.col_order = self.cum_list_cols[1:] + [self.cum_list_cols[0]]
            # self.cum_table = self.cum_table[self.col_order]
            # self.cum_table = self.cum_table.drop(columns=['last_price', 'lower_price_limit', 'upper_price_limit', 'Unnamed: 0', 'Symbol', 'Company Name','Industry', 'Instrument_type'])
            self.cum_table = self.cum_table.drop_duplicates(subset=['instrument_token'])
            if DEBUG:
                with pd.ExcelWriter(output_file, mode='w', engine='openpyxl') as writer:
                    self.cum_table.to_excel(writer, sheet_name='cum_table', index=False)

        else:
            self.cum_table = pd.read_excel(output_file, engine="openpyxl", sheet_name='cum_table')
        self.cum_table = self.cum_table[self.cum_table['Ref_stock_tkn'] != -1]
        self.cum_table.reset_index(drop=True, inplace=True)
        self.inst_list_int = self.cum_table['instrument_token'].unique()
        # self.init_ref_list = pd.DataFrame(self.cum_table['Ref_stock_tkn'].unique(), columns=['instrument_token'])
        self.init_ref_list = pd.DataFrame(self.cum_table['Ref_stock_tkn'][(self.cum_table['Capital_share'] > 0)].unique(), columns=['instrument_token'])
        self.index_ref_list = pd.DataFrame(self.cum_table['Index_tkn'][(self.cum_table['Capital_share'] > 0)].unique(),columns=['instrument_token'])

        general_logger.info('cum table finished')
        return self.cum_table, self.inst_list_int, self.init_ref_list

    @timeit
    def is_weekend(self):
        '''this function is used to check weekend'''

        return datetime.today().weekday() > 4

    @timeit
    def stoploss_update(self, tkn_lst):
        '''  currently not used
        this function is used to update the stoploss of instrument token'''
        for g in range(len(tkn_lst)):
            try:
                prev_close_price = TickStore.get_recent_tickstore(instrument_tokens=[tkn_lst[g]])
                prev_stoploss = StopLoss.get_data(token_no=tkn_lst[g])
                if len(prev_close_price) > 0 and prev_stoploss != -1:
                    if prev_stoploss < prev_close_price['last_price']:
                        StopLoss.create(order_id=-1, buy_price=prev_close_price['last_price'],token_no=tkn_lst[g], tradingsymbol=self.cum_table['tradingsymbol'][self.cum_table['instrument_token'] == tkn_lst[g]].values[0])  # enclosed in try since price may not be available sometimes
                elif len(prev_close_price) > 0 and prev_stoploss == -1:
                    StopLoss.create(order_id=-1, buy_price=prev_close_price['last_price'],token_no=tkn_lst[g], tradingsymbol=self.cum_table['tradingsymbol'][self.cum_table['instrument_token'] == tkn_lst[g]].values[0])
            except:
                continue
        return

    # @timeit
    def exchg_time_buy_chk(self, exchg):
        ''''place orders between the timing'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if not self.session_end(exchg):
            hold_time = 13 <= now.minute <= 20 or 43 <= now.minute <= 50
        else:
            hold_time = False
        if exchg == 'CDS':
            end_time = now.replace(hour=17, minute=00, second=0, microsecond=0)
        elif exchg == 'MCX':
            if not self.session_end(exchg):
                hold_time = (58 <= now.minute <= 59 or 0 <= now.minute<= 5) or 28 <= now.minute <= 35
            else:
                hold_time = False
            start_time = now.replace(hour=9, minute=1, second=0, microsecond=0)
            if self.day_light_saving:
                end_time = now.replace(hour=23, minute=30, second=0, microsecond=0)  # modify for day light savings
            else:
                end_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
        else:
            end_time = now.replace(hour=15, minute=40, second=0, microsecond=0)
        if now < start_time :
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time :
            # raise Exception("Trading time is over")
            return False
        return True and not hold_time

    @timeit
    def hedge_time_chk(self, exchg):
        ''''perform hedging between the timing'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if exchg == 'CDS':
            end_time = now.replace(hour=17, minute=00, second=0, microsecond=0)
        elif exchg == 'MCX':

            end_time = now.replace(hour=23, minute=30, second=0, microsecond=0)
        else:
            end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    def half_time(self, exchg):
        ''''chec half time'''
        now = timezone.make_naive(timezone.now())
        if exchg == 'CDS':
            start_time = now.replace(hour=12, minute=15, second=0, microsecond=0)
            end_time = now.replace(hour=16, minute=30, second=50, microsecond=0)
        elif exchg == 'MCX':
            start_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
            end_time = now.replace(hour=23, minute=55, second=50, microsecond=0)
        else:
            start_time = now.replace(hour=13, minute=30, second=0, microsecond=0)
            end_time = now.replace(hour=15, minute=15, second=10, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    # @timeit
    def exchg_time_sell_chk(self, exchg):
        ''''place orders between the timing'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=9, minute=15, second=00, microsecond=0)
        if exchg == 'CDS':
            start_time = now.replace(hour=9, minute=3, second=0, microsecond=0)
            end_time = now.replace(hour=17, minute=00, second=0, microsecond=0)
        elif exchg == 'MCX':
            start_time = now.replace(hour=9, minute=3, second=0, microsecond=0)
            if self.day_light_saving:
                end_time = now.replace(hour=23, minute=30, second=0, microsecond=0)#modify for day light savings
            else:
                end_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
        else:
            end_time = now.replace(hour=15, minute=40, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    # @timeit
    def session_end(self,exchg):
        ''' used for daily PE square off'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=15, minute=20, second=20, microsecond=0)
        end_time = now.replace(hour=15, minute=35, second=0, microsecond=0)
        if exchg == 'MCX':
            if self.day_light_saving:
                start_time = now.replace(hour=23, minute=00, second=0, microsecond=0)
                end_time = now.replace(hour=23, minute=30, second=0, microsecond=0)
            else:
                start_time = now.replace(hour=23, minute=30, second=0, microsecond=0)
                end_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now.replace(hour=15, minute=00, second=0, microsecond=0)
            end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    def prev_cdl_save_time(self):
        now = timezone.make_naive(timezone.now())

        # Window 1: 15:29:00 to 15:30:00
        start_1 = now.replace(hour=15, minute=29, second=0, microsecond=0)
        end_1 = now.replace(hour=15, minute=30, second=0, microsecond=0)

        # Window 2: Dynamic based on Daylight Savings
        if self.day_light_saving:
            start_2 = now.replace(hour=23, minute=30, second=0, microsecond=0)
            end_2 = now.replace(hour=23, minute=31, second=0, microsecond=0)
        else:
            start_2 = now.replace(hour=23, minute=55, second=0, microsecond=0)
            end_2 = now.replace(hour=23, minute=56, second=0, microsecond=0)

        # Check if 'now' is inside Window 1 OR Window 2
        if (start_1 <= now <= end_1) or (start_2 <= now <= end_2):
            return True

        return False
    def pre_trade_sl_update_time(self):
        ''' used for stop loss update'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=12, minute=45, second=0, microsecond=0)
        end_time = now.replace(hour=12, minute=46, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    def sl_update_time(self,exchg):
        ''' used for stop loss update'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=12, minute=45, second=0, microsecond=0)
        end_time = now.replace(hour=12, minute=46, second=0, microsecond=0)
        if exchg == 'NFO':
            start_time = now.replace(hour=10, minute=15, second=0, microsecond=0)
            end_time = now.replace(hour=10, minute=16, second=0, microsecond=0)
        if exchg == 'MCX':
            if self.day_light_saving:
                start_time = now.replace(hour=23, minute=59, second=0, microsecond=0)
                end_time = now.replace(hour=23, minute=59, second=0, microsecond=0)
            else:
                start_time = now.replace(hour=23, minute=59, second=0, microsecond=0)
                end_time = now.replace(hour=23, minute=59, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now.replace(hour=17, minute=30, second=0, microsecond=0)
            end_time = now.replace(hour=17, minute=30, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        if self.session_end(exchg) and self.next_session_closed(exchg):
            return True
        return True

    def strike_update_time(self,exchg):
        ''' used for daily PE square off'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        end_time = now.replace(hour=9, minute=15, second=5, microsecond=0)
        if exchg == 'MCX':
            start_time = now.replace(hour=9, minute=29, second=40, microsecond=0)
            end_time = now.replace(hour=9, minute=30, second=1, microsecond=0)
        elif exchg == 'CDS':
            start_time = now.replace(hour=9, minute=00, second=3, microsecond=0)
            end_time = now.replace(hour=9, minute=00, second=8, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    def session_reset(self,exchg):
        ''' used for daily PE square off'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=15, minute=10, second=0, microsecond=0)
        end_time = now.replace(hour=15, minute=20, second=0, microsecond=0)
        if exchg == 'MCX':
            start_time = now.replace(hour=23, minute=10, second=0, microsecond=0)
            end_time = now.replace(hour=23, minute=20, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now.replace(hour=16, minute=40, second=0, microsecond=0)
            end_time = now.replace(hour=16, minute=50, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    def session_start(self,exchg):
        ''' used for session_start'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=9, minute=25, second=0, microsecond=0)
        end_time = now.replace(hour=9, minute=45, second=0, microsecond=0)
        if exchg == 'MCX':
            start_time = now.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now.replace(hour=9, minute=45, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now.replace(hour=9, minute=1, second=0, microsecond=0)
            end_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    # @timeit
    def expiry_sell_time(self,exchg):
        ''' used to check expiry day sell'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=15, minute=00, second=0, microsecond=0)
        end_time = now.replace(hour=15, minute=40, second=0, microsecond=0)
        if exchg == 'MCX':
            start_time = now.replace(hour=11, minute=00, second=0, microsecond=0)
            end_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
        elif exchg == 'CDS':
            start_time = now.replace(hour=15, minute=00, second=0, microsecond=0)
            end_time = now.replace(hour=17, minute=00, second=0, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    def post_trade_time(self,exchg):
        ''' not needed here'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=15, minute=40, second=0, microsecond=0)
        end_time = now.replace(hour=15, minute=40, second=10, microsecond=0)
        if exchg == 'MCX':
            start_time = now.replace(hour=23, minute=55, second=0, microsecond=0)
            end_time = now.replace(hour=23, minute=55, second=10, microsecond=0)
        elif exchg == 'CDS':
            start_time = now.replace(hour=17, minute=00, second=0, microsecond=0)
            end_time = now.replace(hour=17, minute=00, second=10, microsecond=0)
        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    def master_list_update_time(self):
        ''' used to master token list'''
        now = timezone.make_naive(timezone.now())
        start_time = now.replace(hour=8, minute=00, second=0, microsecond=0)
        end_time = now.replace(hour=9, minute=00, second=0, microsecond=0)

        if now < start_time:
            # ticker.finished("Trading dint start yet")
            # raise Exception("Trading Dint start yet")
            return False
        elif now > end_time:
            # raise Exception("Trading time is over")
            return False
        return True

    # @timeit
    def token_list_update(self):
        '''
        This function collects the tokens in holdings,positions,ref,index and monitored options contracts
        '''
        if not self.hold_frame.empty:
            self.init_holding_tokens = pd.DataFrame(self.hold_frame['instrument_token'].unique(),columns=['instrument_token'])
        else:
            self.init_holding_tokens = pd.DataFrame([], columns=['instrument_token'])
        if not self.open_positions.empty:
            self.pos_tkn_list = pd.DataFrame(self.open_positions['instrument_token'].unique(),columns=['instrument_token'])
        else:
            self.pos_tkn_list = pd.DataFrame([], columns=['instrument_token'])
        # split based on strike choice
        ce_list = self.cum_table['instrument_token'][(self.cum_table['Buy_strike'] == 'Yes') & (self.cum_table['instrument_type'] == 'CE')].values
        pe_list = self.cum_table['instrument_token'][(self.cum_table['Buy_strike'] == 'Yes') & (self.cum_table['instrument_type'] == 'PE')].values
        # self.current_list = pd.DataFrame(self.cum_table['instrument_token'][self.cum_table['ATM_ITM_OTM'] == self.strike_choice_CE], columns = ['instrument_token']) #self.strike_choice
        self.current_list = pd.DataFrame(np.r_[ce_list, pe_list], columns=['instrument_token'])  # self.strike_choice
        if self.current_list.empty:
            self.current_list = pd.DataFrame([], columns=['instrument_token'])
        self.updated_list = pd.concat([self.index_ref_list,self.init_ref_list, self.init_holding_tokens, self.pos_tkn_list, self.current_list],ignore_index=True).drop_duplicates(subset=["instrument_token"])  # append and get only unique tokens

        return self.updated_list


    def insert_instrument_token(self, token_list):
        '''this function inserts instrument token in db for subscribing'''
        table_data = AlgoInfo.get_table_data(self.client.broker, 'zerodha_inst_tokens')
        token_data = table_data.get('zerodha_inst_tokens', {})
        self.existing_tkn = orjson.loads(token_data)['tokenid']
        # self.existing_tkn = list(flatten(AlgoInfo.get_table_data(self.client.broker, 'zerodha_inst_tokens')).values())
        if len(token_list) > 0 and not DEBUG and (set(self.existing_tkn) != set(token_list)):
            AlgoInfo.create_or_update(
                broker=self.client.broker,
                tablename='zerodha_inst_tokens',
                tabledata=orjson.dumps({'tokenid':token_list}).decode("utf-8") # Using dict format for consistency
            )


    def olhc_trans(self, raw_df, delta_t, smooth_factor, origin, lable, closed):
        '''
        raw_df = data fetched from database
        smooth_factor = filter factor for ewm
        delta_t = resampling raw data
        assumed old data point is first
        currently in use
        '''

        if len(raw_df) > 1:
            raw_df = raw_df.sort_values(by='date_time', ascending=False)
            dup_idx = raw_df.index.duplicated(keep='first')
            raw_df = raw_df[~dup_idx]
            resampled_olhc = raw_df.resample(delta_t, origin=origin, label=lable, closed=closed).first()
            resampled_olhc = resampled_olhc.dropna()
            resampled_olhc = resampled_olhc.sort_values(by='date_time', ascending=False)
            resampled_olhc.reset_index(drop=False, inplace=True)
            if len(resampled_olhc) < 10:
                resampled_olhc = pd.concat([resampled_olhc, *[resampled_olhc.tail(1)] * 10])
            # smoothed_olhc = resampled_olhc.ewm(alpha=smooth_factor).mean(numeric_only=True)
            # smoothed_olhc[['date_time']] = resampled_olhc[['date_time']]
        else:
            # smoothed_olhc = pd.DataFrame([])
            resampled_olhc = pd.DataFrame([])

        return  resampled_olhc

    def tick_to_olhc(self, raw_df, delta_t, origin, lable, closed):
        '''
        raw_df = data fetched from database
        smooth_factor = filter factor for ewm
        delta_t = resampling raw data
        assumed old data point is first
        currently in use
        '''

        if len(raw_df) > 1:
            resampled_olhc = raw_df['last_price'].resample(delta_t, origin=origin, label=lable, closed=closed).ohlc()
            # print(raw_df['last_price'])
            resampled_olhc['instrument_token'] = raw_df['instrument_token'].resample(delta_t, origin=origin, label=lable, closed=closed).mean().astype("Int64")
            resampled_olhc = resampled_olhc.dropna()
            resampled_olhc = resampled_olhc.sort_values(by='date_time', ascending=False)
            resampled_olhc.reset_index(drop=False, inplace=True)
        else:
            resampled_olhc = pd.DataFrame([])

        return  resampled_olhc

    def group_by_rolling_window(self,df_all,df_recent, window_size,bod=False):
        """
        Create candlesticks based on new tick data received
        """
        if not df_recent.empty and not df_all.empty:
            df = pd.concat([df_recent[df_recent['date_time']>=df_all['date_time'].max()],df_all[df_all['date_time']>=df_all['date_time'].max()]],ignore_index=True)
            # general_logger.info('all_df start time is ' + str(df_all['date_time'].max()))
        elif not df_recent.empty:
            df = df_recent
        else:
            df = df_all
        if not df.empty:
            df=df.sort_values(by='date_time', ascending=False)
            now = timezone.make_naive(timezone.now())
            if  any(self.cum_table[self.cum_table['instrument_token']==df['instrument_token'].values[0]]['exchange'].values == 'MCX'):
                start_time_ref = pd.to_datetime(now.replace(hour=9, minute=00, second=00, microsecond=0))
            elif any(self.cum_table[self.cum_table['instrument_token']==df['instrument_token'].values[0]]['exchange'].values == 'CDS'):
                start_time_ref = pd.to_datetime(now.replace(hour=9, minute=00, second=00, microsecond=0))
            else:
                start_time_ref = pd.to_datetime(now.replace(hour=9, minute=15, second=00, microsecond=0))
            end_time_ref = pd.to_datetime(now.replace(hour=23, minute=55, second=00, microsecond=0))
            timestamps_idx = pd.date_range(start=start_time_ref, end=end_time_ref, freq=window_size, inclusive='left')
            start_time = df['date_time'].min()
            end_time = df['date_time'].max()
            # df.reset_index(drop=False, inplace=True)
            self.time_interval_data = pd.DataFrame(timestamps_idx,columns=['time_intervals'])
            self.time_interval_data['time_intervals'] = self.time_interval_data['time_intervals']
            if bod:
                self.time_interval_data['time_diff'] = (timestamps_idx - start_time).total_seconds()
            else:
                self.time_interval_data['time_diff'] = (timestamps_idx - now).total_seconds()
            nearest_idx = self.time_interval_data.index[self.time_interval_data['time_diff']>0].min()
            if np.isnan(nearest_idx):
                nearest_idx =0
            groups =[]
            if nearest_idx >0 :
                current_start = self.time_interval_data['time_intervals'].iloc[nearest_idx-1]
            else:
                current_start = self.time_interval_data['time_intervals'].iloc[nearest_idx]
            window_size_td = pd.to_timedelta(window_size)
            if window_size_td.total_seconds() != 60:
                while current_start <= end_time :
                    current_end = current_start + window_size_td
                    try:
                        mask = (df['date_time'] >= current_start) & (df['date_time'] <= current_end)
                        df_window = df[mask]
                        mask_1 = (df_all['date_time'] >= current_start) & (df_all['date_time'] <= current_end)
                        df_window_1 = df_all[mask_1]
                        df_window_1 = df_window_1.sort_values(by='date_time', ascending=False)
                        df_window_1.reset_index(drop=True, inplace=True)
                        # df_window = pd.concat([df_window,df_window_1],ignore_index=True)
                        df_window = df_window.sort_values(by='date_time', ascending=False)
                        df_window.reset_index(drop=True, inplace=True)
                        # general_logger.info('current start time is ' + str(df_window_1['date_time'].min()))
                        # groups.append(np.r_[np.array([
                            # df_window['open'].iloc[-1] if len(df_window) > 1 else df_window['open'].iloc[0],  # open
                        if df_window_1.empty:
                            groups.append(np.r_[np.array([
                            df_window['open'][df_window['date_time'] == df_window['date_time'].min()].tail(1).values[0],
                            df_window['low'].min(),  # low
                            df_window['high'].max(),  # high
                            # df_window['close'].iloc[0],  # close
                            df_window['close'][df_window['date_time'] == df_window['date_time'].max()].head(1).values[0],  # close
                            df_window['instrument_token'].iloc[-1] if len(df_window) > 1 else
                            df_window['instrument_token'].iloc[0]
                                ]), current_start])
                        else:
                            groups.append(np.r_[np.array([
                            df_window_1['open'][df_window_1['date_time'] == df_window_1['date_time'].min()].tail(1).values[0],#open
                            min(df_window['low'].min(),df_window_1['low'].min()),  # low
                            max(df_window['high'].max(),df_window_1['high'].max()),  # high
                            # df_window['close'].iloc[0],  # close
                            df_window['close'][df_window['date_time'] == df_window['date_time'].max()].head(1).values[0],#close
                            df_window['instrument_token'].iloc[-1] if len(df_window) > 1 else df_window['instrument_token'].iloc[0]# instrument_token
                            ]),current_start])
                        # general_logger.info('recent close time for : %s', str(df_window['date_time'].iloc[0]))
                        # general_logger.info('start time is : %s', str(current_start))
                        # general_logger.info('end time is : %s', str(current_end))

                    except:
                        current_start += window_size_td

                        continue
                    current_start +=  window_size_td
                sampled_df = pd.DataFrame(groups,columns=['open','low','high','close','instrument_token','date_time'])
                if not bod:
                    # sampled_df = pd.concat([sampled_df,df_all],ignore_index=True)
                    sampled_df = pd.concat([sampled_df, df_all[df_all['date_time']!=pd.to_datetime(sampled_df['date_time'].max())]], ignore_index=True)
                sampled_df = sampled_df.drop_duplicates(subset='date_time',keep = 'first')
            else:
                sampled_df = df

            sampled_df = sampled_df.sort_values(by='date_time', ascending=False)
            # self.data_ready = True
            if len(sampled_df) < 10:
                sampled_df = pd.concat([sampled_df, *[sampled_df.tail(1)] * 10])
            sampled_df.reset_index(drop=True, inplace=True)
        else:
            sampled_df = pd.DataFrame([])
        return sampled_df

    def tkn_to_symbol(self, tkn_lst):
        '''
        nfo_cds_mcx = all instruments in nfo exchange
        nse_inst_data = all instruments in nse exchange
        tkn = instrument token number
        Gives you symbol from token number
        it is assumed instrument token is present in master list
        get instrument token as input and give symbol as output using master_inst_token
        currently not in use
        '''
        self.symbols = []
        for tkn in tkn_lst:
            if tkn in self.nfo_cds_mcx['instrument_token'].values:
                self.symbols.append(self.nfo_cds_mcx['tradingsymbol'][self.nfo_cds_mcx['instrument_token'] == tkn].values[0])
            elif tkn in self.nse_inst_data['instrument_token'].values:
                self.symbols.append(self.nse_inst_data['tradingsymbol'][self.nse_inst_data['instrument_token'] == tkn].values[0])
        return self.symbols


    def symbol_to_tkn(self, symbol):
        '''
        nfo_cds_mcx = all instruments in nfo exchange
        nse_inst_data = all instruments in nse exchange
        tkn = instrument token number
        not needed in option any more
        Gives you symbol from token number
        it is assumed instrument token is present in master list
        get instrument token as input and give symbol as output using master_inst_token
        currently in use
        '''
        if symbol in self.nfo_cds_mcx['tradingsymbol'].values:
            self.tkn = np.array(self.nfo_cds_mcx['instrument_token'][self.nfo_cds_mcx['tradingsymbol'] == symbol])
        elif symbol in self.nse_inst_data['tradingsymbol'].values:
            self.tkn = np.array(self.nse_inst_data['instrument_token'][self.nse_inst_data['tradingsymbol'] == symbol])
        elif symbol in self.bse_inst_data['tradingsymbol'].values:
            self.tkn = np.array(self.bse_inst_data['instrument_token'][self.bse_inst_data['tradingsymbol'] == symbol])
        else:
            self.tkn = -1
        return int(self.tkn)


    def tkn_to_exchg(self, tkn):
        '''
        not needed in option any more
        nfo_cds_mcx = all instruments in nfo exchange
        nse_inst_data = all instruments in nse exchange
        tkn = instrument token number
        it is assumed instrument token is present in master list
        get instrument token as input and give exchange as output using master_inst_token
        currently not in use
        '''
        if tkn in self.nfo_cds_mcx['instrument_token'].values:
            self.exchg = self.nfo_cds_mcx['exchange'][self.nfo_cds_mcx['instrument_token'] == tkn].values
        elif tkn in self.nse_inst_data['instrument_token'].values:
            self.exchg = self.nse_inst_data['exchange'][self.nse_inst_data['instrument_token'] == tkn].values
        return self.exchg


    def heikin_ashi(self, df):
        heikin_ashi_df = df.copy()
        heikin_ashi_df['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        for i in range(len(df)):
            if i == 0:
                heikin_ashi_df['open'][0] = df['open'].iloc[0]
            else:
                heikin_ashi_df['open'][i] = (heikin_ashi_df['open'].iloc[i - 1] + heikin_ashi_df['close'].iloc[i - 1]) / 2
        heikin_ashi_df['high'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
        heikin_ashi_df['low'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
        return heikin_ashi_df

    # @timeit
    def instrument_CE_PE_analysis(self, token_number, session_ref_data):
        '''
        token_number = instrument token number
        cap_config = configuration data from excel
        ren_frame = denoised data
        nfo_cds_mcx = all instruments in nse exchange used to remove tokens based on expiry date
        buy_signal_CE = 1 attempt to buy
        buy_signal_PE = -1 attempt to sell
        buy_signal_PE = -2 attempt to buy
        buy_signal_PE = 1 attempt to sell
        buy_signal = -5 attempt to sell
        buy_signal = 0 not tradeable
        function to add attributes like min and max of share etc...
        denoised price is from live denoised data
        '''
        self.buy_signal_CE = 0
        self.buy_signal_PE = 0
        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        # prev_day = self.prev_tick_data[self.prev_tick_data['instrument_token'] == token_number].tail(1)
        if not session_ref_data.empty:
            _, ce_rev = self.olhc_trans(session_ref_data, '15min', 0.5, 'end', 'right', 'right')  # for CE signal
            _, ce_fwd = self.olhc_trans(session_ref_data, '15min', 0.5, 'start', 'left', 'left')
            _, fwd_5 = self.olhc_trans(session_ref_data, '5min', 0.5, 'start', 'left', 'left')
            _, ref_long = self.olhc_trans(session_ref_data, '45min', 1, 'end', 'left', 'right')
            _, pe_rev = self.olhc_trans(session_ref_data, '1min', 0.5, 'end', 'right', 'right')  # for PE signal
            _, pe_fwd = self.olhc_trans(session_ref_data, '1min', 0.5, 'start', 'left', 'left')
            _, pe_fwd_5 = self.olhc_trans(session_ref_data, '5min', 0.5, 'start', 'left', 'left')
            _, pe_fwd_10 = self.olhc_trans(session_ref_data, '10min', 1, 'start', 'left', 'left')
            if len(pe_fwd_10) <= 4:
                pe_fwd_10 = pd.concat([pe_fwd_10,pe_fwd_10[-1:],pe_fwd_10[-1:],pe_fwd_10[-1:],pe_fwd_10[-1:]], ignore_index=True)
            if len(pe_fwd_5) <= 4:
                pe_fwd_5 = pd.concat([pe_fwd_5,pe_fwd_5[-1:],pe_fwd_5[-1:],pe_fwd_5[-1:],pe_fwd_5[-1:]], ignore_index=True)
            ref_min_max, _ = self.olhc_trans(session_ref_data, '15min', 0.3, 'start', 'left', 'left')
            _, long_rev = self.olhc_trans(session_ref_data, '90min', 1, 'end', 'left', 'right')  # end session
            _, pe_rev_60 = self.olhc_trans(session_ref_data, '60min', 1, 'end', 'left', 'right')
            _, pe_rev_30 = self.olhc_trans(session_ref_data, '30min', 1, 'end', 'left', 'right')
            _, pe_rev_15 = self.olhc_trans(session_ref_data, '15min', 1, 'end', 'left', 'right')
            _, half_day_cdl = self.olhc_trans(session_ref_data, '240min', 1, 'end', 'left','right')  # end session
            _, day_cdl = self.olhc_trans(session_ref_data, '1d', 1, 'start', 'left', 'left')  # end session
            now = timezone.make_naive(timezone.now())
            risk_buy_time = now.replace(hour=9, minute=24, second=0, microsecond=0)
            pe_start_time = now.replace(hour=9, minute=31, second=0, microsecond=0)

            # pe_end_time = now.replace(hour=15, minute=15, second=0, microsecond=0)
            # olhc_max = ref_min_max[ref_min_max['date_time'] > (ref_min_max['date_time'].max() - pd.Timedelta(seconds=long_window))]['close'].max()
            # olhc_min = ref_min_max[ref_min_max['date_time'] > (ref_min_max['date_time'].max() - pd.Timedelta(seconds=long_window))]['close'].min()
            # max_chg = olhc_max - ce_rev['close'].values[0]
            # min_chg = ce_rev['close'].values[0] - olhc_min
            # delta_limit_CE = self.cum_table[self.cum_table['instrument_token'] == token_number]['delta_limit_CE'].values
            # delta_limit_PE = self.cum_table[self.cum_table['instrument_token'] == token_number]['delta_limit_PE'].values
            # striked_value = np.max(StrikeEntry.get_data(ref_symbol=self.tkn_to_symbol([token_number])[0]))
            # general_logger.info('not skipping analysis')

            if long_rev['close'].values[0] > long_rev['open'].values[0] and pe_rev_30['close'].values[0] > pe_rev_30['open'].values[0] and pe_rev_15['close'].values[0] > pe_rev_15['open'].values[0] and pe_fwd_5[['close','open']][0:2].mean(axis = 1).is_monotonic_decreasing and pe_fwd_10[['close','open']][0:4].mean(axis = 1).is_monotonic_decreasing:
                if pe_fwd['close'][0] > pe_fwd['open'][0] and ((pe_fwd[['close','open']][1:4].mean(axis = 1)).is_monotonic_decreasing or all(pe_fwd['close'][1:4]>pe_fwd['open'][1:4])):

                    self.one_counter['buy_counter'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                    self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = 0
                    if int(self.one_counter['buy_counter'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                        # buy
                        self.buy_signal_CE = 1  # buy CE
                        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        inst_analysis_logger.info('path_1')

            if long_rev['close'].values[0] < long_rev['open'].values[0] and pe_rev_30['close'].values[0] < pe_rev_30['open'].values[0] and pe_rev_15['close'].values[0] < pe_rev_15['open'].values[0] and pe_fwd_5[['close','open']][0:2].mean(axis = 1).is_monotonic_increasing and pe_fwd_10[['close','open']][0:4].mean(axis = 1).is_monotonic_increasing:

                # if (ce_rev['close'].values[0] < ce_rev['open'].values[0]) and (olhc_max - ce_rev['close'].values[0]) > delta_limit_CE:
                self.one_counter['buy_counter'].loc[self.one_counter['instrument_token'] == token_number] = 0
                self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = 0

                if now < risk_buy_time:
                    if all(ce_fwd['close'] < ce_fwd['open']) and (fwd_5['close'][1] < fwd_5['close'][6]):
                        self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] = self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] + 1
                        self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number] = 0
                        self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = 0
                        if int(self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                            # buy
                            # self.buy_signal_PE = -2  # buy PE
                            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -2
                            self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -2
                            inst_analysis_logger.info('path_2')

                elif now >= risk_buy_time and now < pe_start_time:#currently inactive
                    # if (pe_rev['close'].values[0] < pe_rev['close'].values[1]) and (pe_rev['close'].values[0] < pe_rev['open'].values[0]) and (pe_fwd['close'].values[0] < pe_fwd['open'].values[0]) and (pe_fwd['close'].values[1] < pe_fwd['open'].values[1]):
                    if all(pe_fwd['close'] < pe_fwd['open']) and any(self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] != -3) :
                        self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] = self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] + 1
                        self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number] = 0
                        self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = 0
                        if int(self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                            # buy
                            self.buy_signal_PE = 1  # buy PE
                            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                            self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                            inst_analysis_logger.info('path_3')
                else:
                    # if half_day_cdl['close'].values[0] < half_day_cdl['open'].values[0]:
                        # if (pe_rev['close'].values[0] < pe_rev['close'].values[1]) and (pe_rev['close'].values[0] < pe_rev['open'].values[0]) and (pe_fwd['close'].values[0] < pe_fwd['open'].values[0]) and (pe_fwd['close'].values[1] < pe_fwd['open'].values[1]):
                    if pe_fwd['close'][0] < pe_fwd['open'][0] and ((pe_fwd[['close','open']][1:4].mean(axis = 1)).is_monotonic_increasing or all(pe_fwd['close'][1:4]<pe_fwd['open'][1:4])) :
                        self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] = self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] + 1
                        self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number] = 0
                        self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = 0
                        if int(self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                            if now > pe_start_time :
                                # buy
                                self.buy_signal_PE = 1  # buy PE
                                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                                self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                                inst_analysis_logger.info('path_4')

            # if pe_fwd_5['close'].values[1] > pe_fwd_5['open'].values[1] and (pe_fwd_5['close'].values[1] > pe_fwd_5['open'].values[2] or pe_fwd_5['close'].values[0] > pe_fwd_5['open'].values[2]):
            if pe_fwd_5['close'].values[1] > pe_fwd_5['close'].values[2] or pe_fwd_5['close'].values[0] > pe_fwd_5['close'].values[2]:
                self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] = 0
                self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number] = self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number] + 1
                self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = 0
                if int(self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                    # sell
                    # self.buy_signal_PE = -3  # sell PE
                    # self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -3
                    self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    inst_analysis_logger.info('path_5')

            if pe_fwd_5['close'].values[1] < pe_fwd_5['close'].values[2] or pe_fwd_5['close'].values[0] < pe_fwd_5['close'].values[2]:
                self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number] = 0
                self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = 0
                if int(self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                    # sell
                    # self.buy_signal_CE = -1  # sell CE
                    # self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    inst_analysis_logger.info('path_6')

            if self.session_end(self.tkn_to_exchg(token_number)):  # for gap up prediction

                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                inst_analysis_logger.info('path_7')
                if token_number in np.unique(self.cum_table[self.cum_table['exchange'] != 'CDS']['Ref_stock_tkn'].values):
                    if day_cdl['close'].values[0] <= day_cdl['open'].values[0]:
                        inst_analysis_logger.info('low day end session path buy CE sell PE')
                        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1



            if DEBUG:
                self.buy_signal_PE = 1
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                # self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -3
                self.buy_signal_CE = 1
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                # self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
        else:
            general_logger.info('skipping analysis')
            self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] + 1
            if int(self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                self.buy_signal_PE = -5
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -5
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -5
                self.buy_signal_CE = -5
                inst_analysis_logger.info('path_8')

        self.one_counter_val = int(self.one_counter['buy_counter'].loc[self.one_counter['instrument_token'] == token_number].values)
        self.minus_one_counter_val = int(self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number].values)
        self.minus_two_counter_val = int(self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number].values)
        self.minus_three_counter_val = int(self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number].values)
        self.minus_five_counter_val = int(self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number].values)
        self.exchg = self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]  # find which exchange the instrument belongs to
        self.cur_symbol = self.cum_table[self.cum_table['instrument_token'] == token_number]['tradingsymbol'].values[0]

        self.stock_info = pd.DataFrame.from_dict(
            {'instrument_token': [int(token_number)],
             'buy_signal_CE': [int(self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])], 'buy_signal_PE': [int(self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'CE_jump': [int(self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'PE_jump': [int(self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'exchange': [self.exchg], 'symbol': [self.cur_symbol]})

        if not DEBUG and not session_ref_data.empty:
            inst_analysis_logger.info('%s / %s / %s / %s / %s / %s / %s' % (token_number, self.cur_symbol, session_ref_data['last_price'][0],self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],
            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0]))
            # AlgoOut.add_output(self.stock_info, 'Zerodha')

        return self.stock_info

    def mom_old_1(self, token_number, session_ref_data):
        '''
        token_number = instrument token number
        cap_config = configuration data from excel
        ren_frame = denoised data
        nfo_cds_mcx = all instruments in nse exchange used to remove tokens based on expiry date
        buy_signal_CE = 1 attempt to buy
        buy_signal_PE = -1 attempt to sell
        buy_signal_PE = 1 attempt to buy
        buy_signal_PE = -1 attempt to sell
        buy_signal = -5 attempt to sell
        buy_signal = 0 not tradeable
        function to add attributes like min and max of share etc...
        denoised price is from live denoised data
        '''

        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        # prev_day = self.prev_tick_data[self.prev_tick_data['instrument_token'] == token_number].tail(1)
        if not session_ref_data.empty:

            _, fwd_15 = self.olhc_trans(session_ref_data, '15min', 0.5, 'start_day', 'left', 'left')
            _, fwd_5 = self.olhc_trans(session_ref_data, '5min', 0.5, 'start_day', 'left', 'left')
            _, fwd_3 = self.olhc_trans(session_ref_data, '3min', 0.5, 'start_day', 'left', 'left')
            _, fwd_60 = self.olhc_trans(session_ref_data, '60min', 0.5, 'start_day', 'left', 'left')
            _, fwd_30 = self.olhc_trans(session_ref_data, '30min', 0.5, 'start_day', 'left', 'left')
            _, rev_45 = self.olhc_trans(session_ref_data, '45min', 1, 'end', 'left', 'right')
            _, rev_1 = self.olhc_trans(session_ref_data, '1min', 0.5, 'end', 'right', 'right')
            _, fwd_1 = self.olhc_trans(session_ref_data, '1min', 0.5, 'start_day', 'left', 'left')
            _, fwd_10 = self.olhc_trans(session_ref_data, '10min', 1, 'start_day', 'left', 'left')
            _,ref_min_max = self.olhc_trans(session_ref_data, '5min', 0.3, 'start_day', 'left', 'left')
            _, rev_90 = self.olhc_trans(session_ref_data, '90min', 1, 'end', 'left', 'right')
            _, rev_60 = self.olhc_trans(session_ref_data, '60min', 1, 'end', 'left', 'right')
            _, rev_30 = self.olhc_trans(session_ref_data, '30min', 1, 'end', 'left', 'right')
            _, rev_15 = self.olhc_trans(session_ref_data, '15min', 1, 'end', 'left', 'right')
            _, half_day_cdl = self.olhc_trans(session_ref_data, '240min', 1, 'end', 'left', 'right')  # end session
            _, day_cdl = self.olhc_trans(session_ref_data, '1d', 1, 'start', 'left', 'left')  # end session
            olhc_max = ref_min_max[ref_min_max['date_time'] > (ref_min_max['date_time'].max() - pd.Timedelta(seconds=3600))]['close'].max()
            olhc_min = ref_min_max[ref_min_max['date_time'] > (ref_min_max['date_time'].max() - pd.Timedelta(seconds=3600))]['close'].min()
            # max_chg = olhc_max - ce_rev['close'].values[0]
            # min_chg = ce_rev['close'].values[0] - olhc_min
            session_ref_data = session_ref_data.sort_values(by='date_time', ascending=False)
            now = timezone.make_naive(timezone.now())
            risk_buy_time = now.replace(hour=9, minute=25, second=0, microsecond=0)
            pe_start_time = now.replace(hour=9, minute=31, second=0, microsecond=0)
            sq_off_time = now.replace(hour=23, minute=40, second=0, microsecond=0)
            # general_logger.info('close_value_end: %s', session_ref_data['last_price'].tail(1).values[0])
            # general_logger.info('close_value_recent: %s', session_ref_data['last_price'].head(1).values[0])
            # if rev_60['close'].values[0] < rev_60['open'].values[0] and rev_30['close'].values[0] < rev_30['open'].values[0] and rev_15['close'].values[0] < rev_15['open'].values[0]:
            if now < risk_buy_time:
                if fwd_60['close'].values[0] > fwd_60['close'].values[1] and rev_60['close'].values[0] > rev_60['open'].values[0] and (fwd_15['close'].values[0:2] >= fwd_15['open'].values[0:2]).all() and (fwd_5['close'].values[1] >= fwd_5['close'].values[6]):
                    self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                    if int(self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        # buy CE
                        # self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        # self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        inst_analysis_logger.info('path_1')

                if fwd_30['close'].values[0] > fwd_30['close'].values[1] :
                    self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    if int(self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        # sell PE
                        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                        inst_analysis_logger.info('path_1')

                if fwd_60['close'].values[0] < fwd_60['close'].values[1] and rev_60['close'].values[0] < rev_60['open'].values[0] and (fwd_15['close'].values[0:2] <= fwd_15['open'].values[0:2]).all() and (fwd_5['close'].values[1] <= fwd_5['close'].values[6]):
                    self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                    if int(self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        # buy PE
                        # self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        # self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        inst_analysis_logger.info('path_2')

                if fwd_30['close'].values[1] < fwd_30['close'].values[2]:
                    self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # sell CE
                    if int(self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                        inst_analysis_logger.info('path_2')

            else:
                if rev_30['close'].values[0] < rev_30['open'].values[0] and (fwd_15['close'].values[1] < fwd_15[['open', 'close']].values[2].min()):

                    # self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        # sell CE
                        # self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                        inst_analysis_logger.info('path_10')

                # elif (rev_60['close'].values[0] > rev_60['open'].values[0]) and rev_30['close'].values[0] > rev_30['open'].values[0] and ((rev_15['close'].values[0] > rev_15['open'].values[0] and (fwd_15[['close','open']][1:4].mean(axis = 1)).is_monotonic_decreasing and (fwd_5[['close']][1:4]).is_monotonic_decreasing) or (day_cdl['high'].values[0] >= fwd_1['close'].values[0])):
                elif (rev_60['close'].values[0] > rev_60['open'].values[0]) and  (session_ref_data['last_price'].values[0] >= olhc_max) :
                    self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                    if int(self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        # buy CE
                        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        # self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        inst_analysis_logger.info('path_3')

                if fwd_5[['open', 'close']].values[1].max() > fwd_5['open'].values[2] :

                    # self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        # sell PE
                        # self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                        inst_analysis_logger.info('path_9')

                # elif (rev_90['close'].values[0] < rev_90['open'].values[0]) and rev_30['close'].values[0] < rev_30['open'].values[0] and ((rev_15['close'].values[0] < rev_15['open'].values[0] and (fwd_5[['close','open']][1:4].mean(axis = 1)).is_monotonic_increasing and (fwd_15[['close','open']][1:4].mean(axis = 1)).is_monotonic_increasing and (fwd_5[['open', 'close']].values[1].max() < fwd_5['open'].values[2] or fwd_5['close'].values[0] < fwd_5[['open', 'close']].values[2].max()) or (day_cdl['low'].values[0] <= fwd_1['close'].values[0]))):
                elif (rev_90['close'].values[0] < rev_90['open'].values[0]) and (session_ref_data['last_price'].values[0] < olhc_min) :
                    self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                    if int(self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                        # buy PE
                        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        # self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        inst_analysis_logger.info('path_5')



            if self.session_end(self.tkn_to_exchg(token_number)):  # for gap up prediction
                if token_number in np.unique(self.cum_table[self.cum_table['exchange'] == 'MCX']['Ref_stock_tkn'].values):
                    if (fwd_60[['close','open']][0:3].mean(axis = 1)).is_monotonic_increasing:
                        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    else:
                        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                if token_number in np.unique(self.cum_table[self.cum_table['exchange'] == 'NFO']['Ref_stock_tkn'].values):
                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                    inst_analysis_logger.info('path_11')

            if datetime.today().weekday() == 4:#friday chk
                if now > sq_off_time:  # for weekend sq off
                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                    inst_analysis_logger.info('path_12')

            if DEBUG:
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        else:
            general_logger.info('skipping analysis')
            self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] + 1
            if int(self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number]) > self.debounce_counter_threshold:
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -5
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -5

        # self.one_counter_val = int(self.one_counter['buy_counter'].loc[self.one_counter['instrument_token'] == token_number].values)
        # self.minus_one_counter_val = int(self.minus_one_counter['buy_counter'].loc[self.minus_one_counter['instrument_token'] == token_number].values)
        # self.minus_two_counter_val = int(self.minus_two_counter['buy_counter'].loc[self.minus_two_counter['instrument_token'] == token_number].values)
        # self.minus_three_counter_val = int(self.minus_three_counter['buy_counter'].loc[self.minus_three_counter['instrument_token'] == token_number].values)
        # self.minus_five_counter_val = int(self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number].values)
        self.exchg = self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]  # find which exchange the instrument belongs to
        self.cur_symbol = self.cum_table[self.cum_table['instrument_token'] == token_number]['tradingsymbol'].values[0]

        self.stock_info = pd.DataFrame.from_dict(
            {'instrument_token': [int(token_number)],
             'buy_signal_CE': [int(self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])], 'buy_signal_PE': [int(self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'CE_jump': [int(self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'PE_jump': [int(self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'exchange': [self.exchg], 'symbol': [self.cur_symbol]})

        if not DEBUG and not session_ref_data.empty:
            inst_analysis_logger.info('%s / %s / %s / %s / %s / %s / %s' % (token_number, self.cur_symbol, session_ref_data['last_price'][0],self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],
            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0]))
            # AlgoOut.add_output(self.stock_info, 'Zerodha')

        return self.stock_info

    def mom_old(self, token_number, session_ref_data):
        '''
        token_number = instrument token number
        cap_config = configuration data from excel
        ren_frame = denoised data
        nfo_cds_mcx = all instruments in nse exchange used to remove tokens based on expiry date
        buy_signal_CE = 1 attempt to buy
        buy_signal_PE = -1 attempt to sell
        buy_signal_PE = 1 attempt to buy
        buy_signal_PE = -1 attempt to sell
        buy_signal = -5 attempt to sell
        buy_signal = 0 not tradeable
        function to add attributes like min and max of share etc...
        denoised price is from live denoised data
        '''
        general_logger.info('entered mom')
        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        olhc_max = 0
        olhc_min = 0
        # prev_day = self.prev_tick_data[self.prev_tick_data['instrument_token'] == token_number].tail(1)

        if not session_ref_data.empty and self.data_ready == True:
            general_logger.info('mom analysis started')
            self.recent_olhc = self.tick_to_olhc(session_ref_data, '1s', 'start_day', 'left', 'left')
            # fwd_15 = self.olhc_trans(session_ref_data, '15min', 0.5, 'start_day', 'left', 'left')
            self.fwd_15 = self.group_by_rolling_window( self.fwd_15_all[self.fwd_15_all['instrument_token'] == token_number],self.recent_olhc, window_size='15min')
            # self.fwd_15 = self.olhc_trans(pd.concat([self.fwd_15,self.recent_olhc]), '15min', 0.5, 'start_day', 'left', 'left')
            # general_logger.info('current close value is '+str(self.fwd_15['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.fwd_15['close'].values[1]))
            # general_logger.info('length of 15 min cdl is '+str(len(self.fwd_15['close'])))
            # fwd_5 = self.olhc_trans(session_ref_data, '5min', 0.5, 'start_day', 'left', 'left')
            self.fwd_5 =  self.group_by_rolling_window( self.fwd_5_all[self.fwd_5_all['instrument_token'] == token_number],self.recent_olhc, window_size='5min')
            # general_logger.info('length of 5 min cdl is ' + str(len(self.fwd_5['close'])))
            # general_logger.info('current close value is ' + str(self.fwd_5['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.fwd_5['close'].values[1]))
            # fwd_3 = self.olhc_trans(session_ref_data, '3min', 0.5, 'start_day', 'left', 'left')
            self.fwd_3 =  self.group_by_rolling_window( self.fwd_3_all[self.fwd_3_all['instrument_token'] == token_number],self.recent_olhc, window_size='3min')
            # general_logger.info('length of 3 min cdl is ' + str(len(self.fwd_3['close'])))
            # general_logger.info('current close value is ' + str(self.fwd_3['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.fwd_3['close'].values[1]))
            # fwd_60 = self.olhc_trans(session_ref_data, '60min', 0.5, 'start_day', 'left', 'left')
            self.fwd_60 =  self.group_by_rolling_window( self.fwd_60_all[self.fwd_60_all['instrument_token'] == token_number],self.recent_olhc, window_size='60min')
            # general_logger.info('length of 60 min cdl is ' + str(len(self.fwd_60['close'])))
            # general_logger.info('current close value is ' + str(self.fwd_60['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.fwd_60['close'].values[1]))
            # fwd_30 = self.olhc_trans(session_ref_data, '30min', 0.5, 'start_day', 'left', 'left')
            self.fwd_30 =  self.group_by_rolling_window( self.fwd_30_all[self.fwd_30_all['instrument_token'] == token_number],self.recent_olhc, window_size='30min')
            # general_logger.info('length of 30 min cdl is ' + str(len(self.fwd_30['close'])))
            # general_logger.info('current close value is ' + str(self.fwd_30['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.fwd_30['close'].values[1]))
            # _, rev_45 = self.olhc_trans(session_ref_data, '45min', 1, 'end', 'left', 'right')
            # _, rev_1 = self.olhc_trans(session_ref_data, '1min', 0.5, 'end', 'right', 'right')
            # fwd_1 = self.olhc_trans(session_ref_data, '1min', 0.5, 'start_day', 'left', 'left')
            self.fwd_1 =  self.group_by_rolling_window( self.fwd_1_all[self.fwd_1_all['instrument_token'] == token_number],self.recent_olhc, window_size='1min')
            # general_logger.info('length of 1 min cdl is ' + str(len(self.fwd_1['close'])))
            # general_logger.info('current close value is ' + str(self.fwd_1['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.fwd_1['close'].values[1]))
            # fwd_10 = self.olhc_trans(session_ref_data, '10min', 1, 'start_day', 'left', 'left')
            self.fwd_10 =  self.group_by_rolling_window( self.fwd_10_all[self.fwd_10_all['instrument_token'] == token_number],self.recent_olhc, window_size='10min')
            # print(self.recent_olhc)
            print(self.fwd_10)
            # print(self.fwd_15)
            general_logger.info('length of 10 min cdl is ' + str(len(self.fwd_10['close'])))
            # general_logger.info('current close value is ' + str(self.fwd_10['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.fwd_10['close'].values[1]))
            # self.ref_min_max =   self.group_by_rolling_window( df = pd.concat([self.ref_min_max_all[self.ref_min_max_all['instrument_token'] == token_number],self.recent_olhc],ignore_index=True), window_size='10min')
            self.ref_min_max = self.fwd_10
            # general_logger.info('length of ref_min_max cdl is ' + str(len(self.ref_min_max['close'])))
            # _, rev_90 = self.olhc_trans(session_ref_data, '90min', 1, 'end', 'left', 'right')
            # _, rev_60 = self.olhc_trans(session_ref_data, '60min', 1, 'end', 'left', 'right')
            # _, rev_30 = self.olhc_trans(session_ref_data, '30min', 1, 'end', 'left', 'right')
            # _, rev_15 = self.olhc_trans(session_ref_data, '15min', 1, 'end', 'left', 'right')
            # _, rev_10 = self.olhc_trans(session_ref_data, '10min', 1, 'end', 'left', 'right')
            self.half_day_cdl =  self.group_by_rolling_window( self.half_day_cdl_all[self.half_day_cdl_all['instrument_token'] == token_number],self.recent_olhc, window_size='0.5D')
            # general_logger.info('length of half_day_cdl cdl is ' + str(len(self.half_day_cdl['close'])))
            # general_logger.info('current close value is ' + str(self.half_day_cdl['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.half_day_cdl['close'].values[1]))
            # self.half_day_cdl = self.olhc_trans(session_ref_data, '240min', 1, 'end', 'left', 'right')  # end session
            # self.day_cdl = self.olhc_trans(session_ref_data, '1d', 1, 'start', 'left', 'left')  # end session
            self.day_cdl = self.group_by_rolling_window( self.day_cdl_all[self.day_cdl_all['instrument_token'] == token_number],self.recent_olhc, window_size='1D')
            # general_logger.info('length of day_cdl cdl is ' + str(len(self.day_cdl['close'])))
            # general_logger.info('current close value is ' + str(self.day_cdl['close'].values[0]))
            # general_logger.info('previous close value is ' + str(self.day_cdl['close'].values[1]))
            # self.ref_min_max.reset_index(drop=False, inplace=True)
            olhc_max = self.ref_min_max[self.ref_min_max['date_time'] >= (self.ref_min_max['date_time'].max() - pd.Timedelta(seconds=self.scan_window ))][['close','open']].max(axis=1).max(axis=0)
            olhc_min = self.ref_min_max[self.ref_min_max['date_time'] >= (self.ref_min_max['date_time'].max() - pd.Timedelta(seconds=self.scan_window))][['close','open']].min(axis=1).min(axis=0)
            # self.ref_min_max = self.ref_min_max.set_index('date_time')
            # max_chg = olhc_max - ce_rev['close'].values[0]
            # min_chg = ce_rev['close'].values[0] - olhc_min
            session_ref_data = session_ref_data.sort_values(by='date_time', ascending=False)
            now = timezone.make_naive(timezone.now())
            risk_buy_time = now.replace(hour=9, minute=25, second=0, microsecond=0)
            pe_start_time = now.replace(hour=9, minute=31, second=0, microsecond=0)
            sq_off_time = now.replace(hour=23, minute=40, second=0, microsecond=0)
            # if rev_60['close'].values[0] < rev_60['open'].values[0] and rev_30['close'].values[0] < rev_30['open'].values[0] and rev_15['close'].values[0] < rev_15['open'].values[0]:
            general_logger.info('mom data transformation ended')
            # if now < risk_buy_time:
            if self.session_start(exchg = self.tkn_to_exchg(token_number)):
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0

                if ((self.fwd_10['close'].values[1] < self.fwd_10[['open', 'close']].values[2].min() or session_ref_data['last_price'].values[0] < self.fwd_10[['open', 'close']].values[2:5].min() or
                        ((olhc_max - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 1.5)) and
                  (self.fwd_10['close'].values[1] > self.fwd_10['close'].values[0]) and
                        (self.fwd_10['close'].values[1] < self.fwd_10['open'].values[1]) and
                            # self.fwd_10[['close','open']][1:3].mean(axis = 1).is_monotonic_increasing and
                        (self.fwd_10['close'].values[0] < self.fwd_10['open'].values[0])
                ) :

                    # self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                        # sell CE
                        # self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    inst_analysis_logger.info('path_10')

                if (
                        (self.fwd_10[['open', 'close']].values[2].max() < self.fwd_10['close'].values[1] or session_ref_data['last_price'].values[0] > self.fwd_10[['open', 'close']].values[2:5].max() or
                        ((session_ref_data['last_price'].values[0] - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0]  * 1.5)) and
                        (self.fwd_10['close'].values[1] < self.fwd_10['close'].values[0]) and
                        (self.fwd_10['close'].values[1] > self.fwd_10['open'].values[1]) and
                        # self.fwd_10[['close','open']][1:3].mean(axis = 1).is_monotonic_decreasing and
                        (self.fwd_10['close'].values[0] > self.fwd_10['open'].values[0])
                ):
                    # self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                        # sell PE
                        # self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    inst_analysis_logger.info('path_9')
                # if self.fwd_30['close'].values[0] > self.fwd_30['close'].values[1] :
                #     self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                #     self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                #     if int(self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                #         sell PE
                        # self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                        # inst_analysis_logger.info('path_1')
                # else:
                #     self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                #     self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0


                # if self.fwd_30['close'].values[1] < self.fwd_30['close'].values[2]:
                #     self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                #     self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # sell CE
                    # if int(self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                    #     self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    #     inst_analysis_logger.info('path_2')
                # else:
                #     self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                #     self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0


            else:
                if ((self.fwd_10['close'].values[1] < self.fwd_10[['open', 'close']].values[2].min() or session_ref_data['last_price'].values[0] < self.fwd_10[['open', 'close']].values[2:5].min() or
                        ((olhc_max - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 1.5)) and
                  (self.fwd_10['close'].values[1] > self.fwd_10['close'].values[0]) and
                        (self.fwd_10['close'].values[1] < self.fwd_10['open'].values[1])
                        #     and self.fwd_10[['close','open']][1:3].mean(axis = 1).is_monotonic_increasing
                        and (self.fwd_10['close'].values[0] < self.fwd_10['open'].values[0])
                ) :

                    # self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                        # sell CE
                        # self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    inst_analysis_logger.info('path_10')

                # else:
                #     self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                #     self.minus_two_counter['buy_counter_CE'].loc[self.minus_two_counter['instrument_token'] == token_number] = 0

                # elif (rev_60['close'].values[0] > rev_60['open'].values[0]) and rev_30['close'].values[0] > rev_30['open'].values[0] and ((rev_15['close'].values[0] > rev_15['open'].values[0] and (fwd_15[['close','open']][1:4].mean(axis = 1)).is_monotonic_decreasing and (fwd_5[['close']][1:4]).is_monotonic_decreasing) or (day_cdl['high'].values[0] >= fwd_1['close'].values[0])):
                else:
                    if ((
                                  # ((self.fwd_10[['open', 'close']].values[1:3].max() - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 1.5) or
                                  ((session_ref_data['last_price'].values[0] - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 2)
                            or (self.fwd_10[['close','open']][0:4].mean(axis = 1).is_monotonic_decreasing and ((session_ref_data['last_price'].values[0] - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0])))
                           # and (self.fwd_10['close'].values[1] >self.fwd_10[['open', 'close']].values[2].max())
                            and (self.fwd_10['close'].values[0] > self.fwd_10['open'].values[0])
                            # or (abs(self.day_cdl['high'] - session_ref_data['last_price'].values[0])  < 1)
                    ) :
                        self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] + 1

                        if int(self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                            # buy CE
                            self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                            # self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                            inst_analysis_logger.info('path_3')
                    else:
                        self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0

                    # if (self.fwd_10[['close', 'open']][0:3].mean(axis=1)).is_monotonic_decreasing and (abs(self.day_cdl['high'].values[0] - session_ref_data['last_price'].values[0])  < 1):
                    #     self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    #
                    #     if int(self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                    #         # buy CE
                    #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 2
                    #         # self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                    #         inst_analysis_logger.info('path_3_1')
                    # else:
                    #     self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0

                if (
                        (self.fwd_10[['open', 'close']].values[2].max() < self.fwd_10['close'].values[1] or session_ref_data['last_price'].values[0] > self.fwd_10[['open', 'close']].values[2:5].max() or
                        ((session_ref_data['last_price'].values[0] - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0]  * 1.5)) and
                        (self.fwd_10['close'].values[1] < self.fwd_10['close'].values[0]) and
                        (self.fwd_10['close'].values[1] > self.fwd_10['open'].values[1]) and
                        # self.fwd_10[['close','open']][1:3].mean(axis = 1).is_monotonic_decreasing and
                        (self.fwd_10['close'].values[0] > self.fwd_10['open'].values[0])
                ):
                    # self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                        # sell PE
                        # self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    inst_analysis_logger.info('path_9')

                # else:
                #     self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                #     self.minus_two_counter['buy_counter_PE'].loc[self.minus_two_counter['instrument_token'] == token_number] = 0
                # elif (rev_90['close'].values[0] < rev_90['open'].values[0]) and rev_30['close'].values[0] < rev_30['open'].values[0] and ((rev_15['close'].values[0] < rev_15['open'].values[0] and (fwd_5[['close','open']][1:4].mean(axis = 1)).is_monotonic_increasing and (fwd_15[['close','open']][1:4].mean(axis = 1)).is_monotonic_increasing and (fwd_5[['open', 'close']].values[1].max() < fwd_5['open'].values[2] or fwd_5['close'].values[0] < fwd_5[['open', 'close']].values[2].max()) or (day_cdl['low'].values[0] <= fwd_1['close'].values[0]))):
                else:
                    if ((
                                  # ((olhc_max - self.fwd_10[['open', 'close']].values[1:3].min()) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] * 1.5) or
                           ((olhc_max - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] * 2)
                            or (self.fwd_10[['close','open']][0:4].mean(axis = 1).is_monotonic_increasing and ((olhc_max - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] * 1)))
                          # and (self.fwd_10['close'].values[1] <self.fwd_10[['open', 'close']].values[2].min())
                            and (self.fwd_10['close'].values[0] < self.fwd_10['open'].values[0])
                            # or  (abs(self.day_cdl['low'] - session_ref_data['last_price'].values[0]) < 1)
                    ) :
                        self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                        if int(self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                            # buy PE
                            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                            # self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                            inst_analysis_logger.info('path_5')
                    else:
                        self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # if (self.fwd_10[['close', 'open']][0:3].mean(axis=1)).is_monotonic_increasing and (abs(self.day_cdl['low'].values[0] - session_ref_data['last_price'].values[0]) < 1):
                    #     self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    #     if int(self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                    #         # buy PE
                    #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 2
                    #         # self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                    #         inst_analysis_logger.info('path_5_1')
                    # else:
                    #     self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0

            if self.session_end(self.tkn_to_exchg(token_number)):  # for gap up prediction
                if token_number in np.unique(self.cum_table[self.cum_table['exchange'] == 'MCX']['Ref_stock_tkn'].values):
                #     if (fwd_60[['close','open']][0:3].mean(axis = 1)).is_monotonic_increasing:
                #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 2 # change to 2 for gap allocation
                #         self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                #     else:
                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                    self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # change to 2 for gap allocation
                    self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                if token_number in np.unique(self.cum_table[self.cum_table['exchange'] == 'NFO']['Ref_stock_tkn'].values):
                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0 # change to 2 for gap allocation
                    self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0 # change to 2 for gap allocation
                    self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                    inst_analysis_logger.info('path_11')

            # if datetime.today().weekday() <= 4:#friday chk
            #     if now > sq_off_time:  # for weekend sq off
            #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -2 # sell without going into jump function
            #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -2 # sell without going into jump function
            #         inst_analysis_logger.info('path_12')

            if DEBUG:
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        else:
            general_logger.info('skipping analysis')
            self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] + 1
            if int(self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -5
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -5

        self.exchg = self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]  # find which exchange the instrument belongs to
        self.cur_symbol = self.cum_table[self.cum_table['instrument_token'] == token_number]['tradingsymbol'].values[0]
        general_logger.info('concat started')
        self.fwd_15_all = self.fwd_15_all.drop(self.fwd_15_all[(self.fwd_15_all['instrument_token']==token_number)].index)
        self.fwd_15_all = pd.concat([self.fwd_15_all,self.fwd_15],ignore_index=True)
        self.fwd_10_all = self.fwd_10_all.drop(self.fwd_10_all[(self.fwd_10_all['instrument_token'] == token_number)].index)
        self.fwd_10_all = pd.concat([self.fwd_10_all,self.fwd_10],ignore_index=True)
        self.fwd_30_all = self.fwd_30_all.drop(self.fwd_30_all[(self.fwd_30_all['instrument_token'] == token_number)].index)
        self.fwd_30_all = pd.concat([self.fwd_30_all,self.fwd_30],ignore_index=True)
        self.fwd_1_all = self.fwd_1_all.drop(self.fwd_1_all[(self.fwd_1_all['instrument_token'] == token_number)].index)
        self.fwd_1_all = pd.concat([self.fwd_1_all,self.fwd_1],ignore_index=True)
        self.fwd_3_all = self.fwd_3_all.drop(self.fwd_3_all[(self.fwd_3_all['instrument_token'] == token_number)].index)
        self.fwd_3_all = pd.concat([self.fwd_3_all,self.fwd_3],ignore_index=True)
        self.fwd_60_all = self.fwd_60_all.drop(self.fwd_60_all[(self.fwd_60_all['instrument_token'] == token_number)].index)
        self.fwd_60_all = pd.concat([self.fwd_60_all,self.fwd_60],ignore_index=True)
        self.fwd_5_all = self.fwd_5_all.drop(self.fwd_5_all[(self.fwd_5_all['instrument_token'] == token_number)].index)
        self.fwd_5_all = pd.concat([self.fwd_5_all,self.fwd_5],ignore_index=True)
        self.ref_min_max_all = self.ref_min_max_all.drop(self.ref_min_max_all[(self.ref_min_max_all['instrument_token'] == token_number)].index)
        self.ref_min_max_all = pd.concat([self.ref_min_max_all,self.ref_min_max],ignore_index=True)
        self.half_day_cdl_all = self.half_day_cdl_all.drop(self.half_day_cdl_all[(self.half_day_cdl_all['instrument_token'] == token_number)].index)
        self.half_day_cdl_all = pd.concat([self.half_day_cdl_all,self.half_day_cdl],ignore_index=True)
        self.day_cdl_all = self.day_cdl_all.drop(self.day_cdl_all[(self.day_cdl_all['instrument_token'] == token_number)].index)
        self.day_cdl_all = pd.concat([self.day_cdl_all,self.day_cdl],ignore_index=True)

        general_logger.info('concat finished')
        self.stock_info = pd.DataFrame.from_dict(
            {'instrument_token': [int(token_number)],
             'buy_signal_CE': [int(self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])], 'buy_signal_PE': [int(self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'CE_jump': [int(self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'PE_jump': [int(self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'exchange': [self.exchg], 'symbol': [self.cur_symbol]})

        if not DEBUG and not session_ref_data.empty:
            inst_analysis_logger.info('%s / %s / %s / %s / %s / %s / %s / %s / %s' % (token_number, self.cur_symbol,session_ref_data['last_price'].values[0], olhc_max, olhc_min,self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],
            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0]))
            # AlgoOut.add_output(self.stock_info, 'Zerodha')


        return self.stock_info

    def mom(self, token_number):
        '''
        token_number = instrument token number
        cap_config = configuration data from excel
        ren_frame = denoised data
        nfo_cds_mcx = all instruments in nse exchange used to remove tokens based on expiry date
        buy_signal_CE = 1 attempt to buy
        buy_signal_PE = -1 attempt to sell
        buy_signal_PE = 1 attempt to buy
        buy_signal_PE = -1 attempt to sell
        buy_signal = -5 attempt to sell
        buy_signal = 0 not tradeable
        function to add attributes like min and max of share etc...
        denoised price is from live denoised data
        '''

        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['day_fall'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['day_rise'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
        index_token_number = self.cum_table['Index_tkn'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0]
        olhc_max = 0
        olhc_min = 0
        # prev_day = self.prev_tick_data[self.prev_tick_data['instrument_token'] == token_number].tail(1)
        # general_logger.info(str(self.tkn_to_symbol([token_number])))
        session_ref_data = self.tick_data[self.tick_data['instrument_token'] == token_number]
        session_ref_data.reset_index(drop=True, inplace=True)
        session_ref_data = session_ref_data.set_index('date_time')
        index_ref_data = self.tick_data[self.tick_data['instrument_token'] == index_token_number]
        index_ref_data.reset_index(drop=True, inplace=True)
        index_ref_data = index_ref_data.set_index('date_time')
        if not session_ref_data[session_ref_data['instrument_token']== token_number].empty and not index_ref_data[index_ref_data['instrument_token']== index_token_number].empty and self.data_ready == True:
            # general_logger.info('mom analysis started for '+str(self.tkn_to_symbol([token_number])))
            self.recent_olhc = self.tick_to_olhc(session_ref_data, '1s', 'start_day', 'left', 'left')
            self.recent_index_olhc = self.tick_to_olhc(index_ref_data, '1s', 'start_day', 'left', 'left')
            self.fwd_15 = self.group_by_rolling_window(self.fwd_15_all[self.fwd_15_all['instrument_token'] == token_number],self.recent_olhc, window_size='15min')
            self.fwd_5 = self.group_by_rolling_window(self.fwd_5_all[self.fwd_5_all['instrument_token'] == token_number],self.recent_olhc, window_size='5min')
            self.fwd_3 = self.group_by_rolling_window(self.fwd_3_all[self.fwd_3_all['instrument_token'] == token_number],self.recent_olhc, window_size='3min')
            self.fwd_60 = self.group_by_rolling_window(self.fwd_60_all[self.fwd_60_all['instrument_token'] == token_number],self.recent_olhc, window_size='60min')
            self.fwd_30 = self.group_by_rolling_window(self.fwd_30_all[self.fwd_30_all['instrument_token'] == token_number],self.recent_olhc, window_size='30min')
            self.fwd_30_index = self.group_by_rolling_window(self.fwd_30_all[self.fwd_30_all['instrument_token'] == index_token_number], self.recent_index_olhc,window_size='30min')
            self.fwd_1 = self.group_by_rolling_window(self.fwd_1_all[self.fwd_1_all['instrument_token'] == token_number],self.recent_olhc, window_size='1min')
            self.fwd_10 = self.group_by_rolling_window(self.fwd_10_all[self.fwd_10_all['instrument_token'] == token_number],self.recent_olhc, window_size='10min')
            self.ref_min_max = self.fwd_10
            self.half_day_cdl = self.group_by_rolling_window(self.half_day_cdl_all[self.half_day_cdl_all['instrument_token'] == token_number], self.recent_olhc,window_size='0.5D')
            self.day_cdl = self.group_by_rolling_window(self.day_cdl_all[self.day_cdl_all['instrument_token'] == token_number], self.recent_olhc,window_size='1D')
            # print(self.cum_table[self.cum_table['instrument_token'] == token_number]['Scan_window'].values[0])
            try :
                olhc_max = self.ref_min_max[self.ref_min_max['date_time'] >= (self.ref_min_max['date_time'].max() - pd.Timedelta(seconds=int(self.cum_table[self.cum_table['instrument_token'] == token_number]['Scan_window'].values[0])))][['close', 'open']].max(axis=1).max(axis=0)
                olhc_min = self.ref_min_max[self.ref_min_max['date_time'] >= (self.ref_min_max['date_time'].max() - pd.Timedelta(seconds=int(self.cum_table[self.cum_table['instrument_token'] == token_number]['Scan_window'].values[0])))][['close', 'open']].min(axis=1).min(axis=0)
            except:
                olhc_max = session_ref_data['last_price'].values[0]
                olhc_min = session_ref_data['last_price'].values[0]
            session_ref_data = session_ref_data.sort_values(by='date_time', ascending=False)
            self.recent_olhc = pd.DataFrame([])
            self.prev_day_cdl = self.prev_day_cdl_all[self.prev_day_cdl_all['instrument_token'] == token_number]
            if len(self.prev_day_cdl)<1:
                self.prev_day_cdl = session_ref_data
            # general_logger.info('mom data transformation ended')
            if self.session_start(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]) and not self.prev_day_cdl_all.empty:
                if not self.prev_day_cdl.empty :
                    if self.cum_table['exchange'][self.cum_table['instrument_token'] == token_number].values[0] == 'MCX':
                        if (self.prev_day_cdl['last_price'].values[0] - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] :
                            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        elif (session_ref_data['last_price'].values[0] - self.prev_day_cdl['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] :
                            self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                    if self.cum_table['exchange'][self.cum_table['instrument_token'] == token_number].values[0] == 'NFO':
                        if (self.fwd_30['open'].values[0] - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] :
                            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        elif ( session_ref_data['last_price'].values[0] - self.fwd_30['open'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] :
                            self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1

            elif not self.fwd_30.empty and not self.fwd_30_index.empty :
                # after session start

                if ((self.fwd_30['close'].values[1] <= self.fwd_30[['open','close']].values[2].mean()) and
                        ((olhc_max - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 1.5) and
                        (self.fwd_30['close'].values[1] > self.fwd_30['close'].values[0]) and
                        (self.fwd_30['close'].values[1] < self.fwd_30['open'].values[1])
                        #     and self.fwd_10[['close','open']][1:3].mean(axis = 1).is_monotonic_increasing
                        and (self.fwd_30['close'].values[0] < self.fwd_30['open'].values[0])
                ):

                    # self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                    # sell CE
                    # if  not self.prev_day_cdl_all.empty:
                    #     if (self.day_cdl['close'].values[0] > self.prev_day_cdl_all[self.prev_day_cdl_all['instrument_token'] == token_number]['close'].values[0]) and (self.day_cdl['close'].values[0] < self.prev_day_cdl_all[self.prev_day_cdl_all['instrument_token'] == token_number]['open'].values[0]):
                            self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                            inst_analysis_logger.info('path_10')

                # else:
                #     self.minus_one_counter['buy_counter_CE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                #     self.minus_two_counter['buy_counter_CE'].loc[self.minus_two_counter['instrument_token'] == token_number] = 0


                if ((
                        # ((self.fwd_10[['open', 'close']].values[1:3].max() - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 1) or
                        # ((session_ref_data['last_price'].values[0] - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 2) or
                         (self.fwd_10[['close', 'open']][0:2].mean(axis=1).is_monotonic_decreasing and (
                        (session_ref_data['last_price'].values[0] - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] * 1)))
                        # and (self.fwd_10['close'].values[1] >self.fwd_10[['open', 'close']].values[2].max())
                        and (self.fwd_30['close'].values[0] > self.fwd_30['open'].values[0]) and (self.fwd_30['close'].values[1] > self.fwd_30['open'].values[1])
                        # or (abs(self.day_cdl['high'] - session_ref_data['last_price'].values[0])  < 1)
                ):
                    self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] + 1

                    if int(self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                        # buy CE
                        self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        # self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        inst_analysis_logger.info('path_3')
                else:
                    self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0

                    # if (self.fwd_10[['close', 'open']][0:3].mean(axis=1)).is_monotonic_decreasing and (abs(self.day_cdl['high'].values[0] - session_ref_data['last_price'].values[0])  < 1):
                    #     self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    #
                    #     if int(self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                    #         # buy CE
                    #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 2
                    #         # self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                    #         inst_analysis_logger.info('path_3_1')
                    # else:
                    #     self.one_counter['buy_counter_CE'].loc[self.one_counter['instrument_token'] == token_number] = 0

                if (
                        (self.fwd_30[['open','close']].values[2].mean() <= self.fwd_30['close'].values[1]) and
                        ((session_ref_data['last_price'].values[0] - olhc_min) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] * 1.5) and
                        (self.fwd_30['close'].values[1] < self.fwd_30['close'].values[0]) and
                        (self.fwd_30['close'].values[1] > self.fwd_30['open'].values[1]) and
                        # self.fwd_10[['close','open']][1:3].mean(axis = 1).is_monotonic_decreasing and
                        (self.fwd_30['close'].values[0] > self.fwd_30['open'].values[0])
                ):
                    # self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0
                    # self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] + 1
                    # if int(self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                    # sell PE
                    # self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                    # if  not self.prev_day_cdl_all.empty:
                    #     if (self.day_cdl['close'].values[0] > self.prev_day_cdl_all[self.prev_day_cdl_all['instrument_token'] == token_number]['close'].values[0]) and (self.day_cdl['close'].values[0] < self.prev_day_cdl_all[self.prev_day_cdl_all['instrument_token'] == token_number]['open'].values[0]):
                            self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                            inst_analysis_logger.info('path_9')

                # else:
                #     self.minus_one_counter['buy_counter_PE'].loc[self.minus_one_counter['instrument_token'] == token_number] = 0
                #     self.minus_two_counter['buy_counter_PE'].loc[self.minus_two_counter['instrument_token'] == token_number] = 0
                # elif (rev_90['close'].values[0] < rev_90['open'].values[0]) and rev_30['close'].values[0] < rev_30['open'].values[0] and ((rev_15['close'].values[0] < rev_15['open'].values[0] and (fwd_5[['close','open']][1:4].mean(axis = 1)).is_monotonic_increasing and (fwd_15[['close','open']][1:4].mean(axis = 1)).is_monotonic_increasing and (fwd_5[['open', 'close']].values[1].max() < fwd_5['open'].values[2] or fwd_5['close'].values[0] < fwd_5[['open', 'close']].values[2].max()) or (day_cdl['low'].values[0] <= fwd_1['close'].values[0]))):

                if ((
                        # ((olhc_max - self.fwd_10[['open', 'close']].values[1:3].min()) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] * 1) or
                        # ((olhc_max - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] * 2) or
                        (self.fwd_10[['close', 'open']][0:2].mean(axis=1).is_monotonic_increasing and (
                        (olhc_max - session_ref_data['last_price'].values[0]) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_PE'].values[0] * 1)))
                        # and (self.fwd_10['close'].values[1] <self.fwd_10[['open', 'close']].values[2].min())
                        and (self.fwd_30['close'].values[0] < self.fwd_30['open'].values[0]) and (self.fwd_30['close'].values[1] < self.fwd_30['open'].values[1])
                        # or  (abs(self.day_cdl['low'] - session_ref_data['last_price'].values[0]) < 1)
                ):
                    self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] + 1
                    if int(self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                        # buy PE
                        self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        # self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        inst_analysis_logger.info('path_5')
                else:
                    self.one_counter['buy_counter_PE'].loc[self.one_counter['instrument_token'] == token_number] = 0

                # half time logic only for nfo exchange
                # if self.cum_table['exchange'][self.cum_table['instrument_token'] == token_number].values[0] == 'NFO':
                #     if (self.fwd_30[['open','close']].values[-1].min() < self.prev_day_cdl['last_price'].values[0]) and any(self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] == 1) and not self.half_time(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]):
                #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                #
                #     if (self.fwd_30[['open','close']].values[-1].min() > self.prev_day_cdl['last_price'].values[0]) and any(self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] == 1) and not self.half_time(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]):
                #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0

                # day rise and fall after half time using index and ref(currently dissabled)
                # if (self.fwd_30['open'].values[-1] > self.fwd_30['close'].values[1] and self.fwd_30_index['open'].values[-1] > self.fwd_30_index['close'].values[1]) or (self.fwd_30['open'].values[-1] < self.prev_day_cdl['last_price'].values[0]) and self.half_time(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]):
                #     self.cum_table['day_fall'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                #     inst_analysis_logger.info('day fall detected for'+ str(self.tkn_to_symbol([token_number])))
                #     if any(self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] == 1):
                #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                #     else:
                #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] =0
                #
                # if (self.fwd_30['close'].values[1] > self.fwd_30['open'].values[-1] and self.fwd_30_index['close'].values[1] > self.fwd_30_index['open'].values[-1]) or (self.fwd_30['open'].values[-1] > self.prev_day_cdl['last_price'].values[0]) and self.half_time(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]) :
                #     self.cum_table['day_rise'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                #     inst_analysis_logger.info('day rise detected for' + str(self.tkn_to_symbol([token_number])))
                #     if any(self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] == 1):
                #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                #     else:
                #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] =0

                # if self.half_time(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]) and not self.session_end(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]):
                #     if self.fwd_30[['open', 'close']].values[-1].min() > self.fwd_30[['open', 'close']].values[1].min() and self.fwd_30_index[['open', 'close']].values[-1].min() > self.fwd_30_index[['open', 'close']].values[1].min():
                #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                #         inst_analysis_logger.info('inflection down for' + str(self.tkn_to_symbol([token_number])))
                #     if self.fwd_30[['open', 'close']].values[1].max() > self.fwd_30[['open', 'close']].values[-1].max() and self.fwd_30_index[['open', 'close']].values[1].max() > self.fwd_30_index[['open', 'close']].values[-1].max():
                #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                #         inst_analysis_logger.info('inflection up for' + str(self.tkn_to_symbol([token_number])))

                if self.session_end(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]) :  # for gap up prediction
                    if self.cum_table['exchange'][self.cum_table['instrument_token'] == token_number].values[0] =='NFO':
                        if  ((self.fwd_30['close'].values[1] < self.fwd_30[['high','low']].values[-1].min()) or  (self.fwd_30['close'].values[1] > self.fwd_30[['high','low']].values[-1].max()))\
                                and (not ( self.fwd_30['open'].values[-1] <=self.fwd_30['high'].values[1] <= self.fwd_30['high'].values[-1] ) or not (self.fwd_30['open'].values[-1] <=self.fwd_30['low'].values[1] <= self.fwd_30['high'].values[-1])):
                            inst_analysis_logger.info('today trendy for' + str(self.tkn_to_symbol([token_number])))
                            if self.fwd_30['close'].values[1] > self.fwd_30['close'].values[-1] :
                                if all(self.fwd_30[['high','low']].values[5].max() > self.fwd_30['high'].values[1:4]) and self.fwd_30[['close', 'open']][1:5].mean(axis=1).is_monotonic_increasing and self.fwd_30['close'].values[1] < self.fwd_30['close'].values[-2] :
                                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1  # buy when day high
                                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                                    inst_analysis_logger.info('tomorrow down for' + str(self.tkn_to_symbol([token_number])))
                                else:
                                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1  # buy when day high
                                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                                    inst_analysis_logger.info('tomorrow up for' + str(self.tkn_to_symbol([token_number])))
                            elif self.fwd_30['close'].values[1] < self.fwd_30['close'].values[-1] :
                                if any(self.fwd_30[['open','close']].values[5].min() < self.fwd_30['close'].values[1:5]) or not self.fwd_30['close'][1:5].is_monotonic_decreasing:
                                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1 # buy otherwise
                                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                                    inst_analysis_logger.info('tomorrow up for' + str(self.tkn_to_symbol([token_number])))
                                elif all(self.fwd_30[['high','low']].values[5].max() > self.fwd_30['high'].values[1:4]) and self.fwd_30['close'][1:5].is_monotonic_increasing:
                                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # buy otherwise
                                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                                    inst_analysis_logger.info('tomorrow down for' + str(self.tkn_to_symbol([token_number])))
                        else:
                            inst_analysis_logger.info('today flat for' + str(self.tkn_to_symbol([token_number])))
                            if ((self.fwd_30['high'].values[1] > self.fwd_30['high'].values[-2]) or self.fwd_30['close'][1:4].is_monotonic_decreasing) and (self.fwd_30[['close', 'open']].values[-1].mean() < self.fwd_30['close'].values[1]):
                                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1  # buy when day high
                                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                                inst_analysis_logger.info('tomorrow up for' + str(self.tkn_to_symbol([token_number])))
                            # elif all(self.fwd_30[['high','low']].values[5].max() > self.fwd_30['high'].values[1:4]) and self.fwd_30['close'][1:5].is_monotonic_increasing:
                            else:
                                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1  # buy pe after a flat day
                                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                                inst_analysis_logger.info('tomorrow down for' + str(self.tkn_to_symbol([token_number])))
                    if self.nfo_risk_hold:
                        if self.fwd_30['close'].values[1] < self.fwd_30['close'].values[-1]:
                            self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1  # buy when day high
                            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1


                #
                #         if (((self.fwd_30['close'].values[1] - self.fwd_30[['open','close']].values[2:6].max()) > self.cum_table[self.cum_table['instrument_token'] == token_number]['Hedge_points_CE'].values[0] and
                #                 (self.fwd_30[['open','close']].values[2:6].max() < self.fwd_30['close'].values[1] ))
                #                 or self.fwd_30['close'][1:5].is_monotonic_decreasing):
                #             self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                #             inst_analysis_logger.info('tomorrow up for' + str(self.tkn_to_symbol([token_number])))
                #         else:
                #
                #             self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                #             inst_analysis_logger.info('tomorrow down for' + str(self.tkn_to_symbol([token_number])))

                    if self.next_session_closed(self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]) :
                        if self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0] == 'MCX' :
                            self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # buy without going into jump function
                            self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # buy without going into jump function
                            inst_analysis_logger.info('path_13')
                        if self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0] == 'NFO' :
                            if ((self.fwd_30['close'].values[1] < self.fwd_30[['high', 'low']].values[-1].min()) or (self.fwd_30['close'].values[1] > self.fwd_30[['high', 'low']].values[-1].max())) \
                                    and (not (self.fwd_30['low'].values[-1] <= self.fwd_30['high'].values[1] <= self.fwd_30['high'].values[-1]) or not (self.fwd_30['low'].values[-1] <= self.fwd_30['low'].values[1] <= self.fwd_30['high'].values[-1])):
                                inst_analysis_logger.info('today trendy for' + str(self.tkn_to_symbol([token_number])))
                                if self.fwd_30['close'].values[1] < self.fwd_30['open'].values[-1]:
                                    self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1 # buy when day high
                                    self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                                    inst_analysis_logger.info('path_13_1')
                            else:
                                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1  # buy when day high
                                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1
                                inst_analysis_logger.info('sell all')
                        #     else:
                        #         self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = -1 # buy otherwise
                        #         self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                        #         inst_analysis_logger.info('path_13_2')

            if DEBUG:
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 1
                self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0


            self.fwd_15_all = self.fwd_15_all.drop(self.fwd_15_all[(self.fwd_15_all['instrument_token'] == token_number)].index)
            self.fwd_15_all = pd.concat([self.fwd_15_all, self.fwd_15], ignore_index=True)
            self.fwd_10_all = self.fwd_10_all.drop(self.fwd_10_all[(self.fwd_10_all['instrument_token'] == token_number)].index)
            self.fwd_10_all = pd.concat([self.fwd_10_all, self.fwd_10], ignore_index=True)
            self.fwd_30_all = self.fwd_30_all.drop(self.fwd_30_all[(self.fwd_30_all['instrument_token'] == token_number)].index)
            self.fwd_30_all = self.fwd_30_all.drop(self.fwd_30_all[(self.fwd_30_all['instrument_token'] == index_token_number)].index)
            self.fwd_30_all = pd.concat([self.fwd_30_all, self.fwd_30, self.fwd_30_index], ignore_index=True)
            self.fwd_1_all = self.fwd_1_all.drop(self.fwd_1_all[(self.fwd_1_all['instrument_token'] == token_number)].index)
            self.fwd_1_all = pd.concat([self.fwd_1_all, self.fwd_1], ignore_index=True)
            self.fwd_3_all = self.fwd_3_all.drop(self.fwd_3_all[(self.fwd_3_all['instrument_token'] == token_number)].index)
            self.fwd_3_all = pd.concat([self.fwd_3_all, self.fwd_3], ignore_index=True)
            self.fwd_60_all = self.fwd_60_all.drop(self.fwd_60_all[(self.fwd_60_all['instrument_token'] == token_number)].index)
            self.fwd_60_all = pd.concat([self.fwd_60_all, self.fwd_60], ignore_index=True)
            self.fwd_5_all = self.fwd_5_all.drop(self.fwd_5_all[(self.fwd_5_all['instrument_token'] == token_number)].index)
            self.fwd_5_all = pd.concat([self.fwd_5_all, self.fwd_5], ignore_index=True)
            self.ref_min_max_all = self.ref_min_max_all.drop(self.ref_min_max_all[(self.ref_min_max_all['instrument_token'] == token_number)].index)
            self.ref_min_max_all = pd.concat([self.ref_min_max_all, self.ref_min_max], ignore_index=True)
            self.half_day_cdl_all = self.half_day_cdl_all.drop(self.half_day_cdl_all[(self.half_day_cdl_all['instrument_token'] == token_number)].index)
            self.half_day_cdl_all = pd.concat([self.half_day_cdl_all, self.half_day_cdl], ignore_index=True)
            self.day_cdl_all = self.day_cdl_all.drop(self.day_cdl_all[(self.day_cdl_all['instrument_token'] == token_number)].index)
            self.day_cdl_all = pd.concat([self.day_cdl_all, self.day_cdl], ignore_index=True)
            # if self.session_end(exchg=self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]):
            #     self.prev_day_cdl_all = self.day_cdl_all

        else:
            general_logger.info('skipping analysis')
            self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] = self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number] + 1
            if int(self.minus_five_counter['buy_counter'].loc[self.minus_five_counter['instrument_token'] == token_number]) >= self.debounce_counter_threshold:
                self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0
                self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = 0

        self.exchg = self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]  # find which exchange the instrument belongs to
        self.cur_symbol = self.cum_table[self.cum_table['instrument_token'] == token_number]['tradingsymbol'].values[0]
        self.cum_table['current_value'].loc[self.cum_table['Ref_stock_tkn'] == token_number] = session_ref_data['last_price'].values[0]
        self.stock_info = pd.DataFrame.from_dict(
            {'instrument_token': [int(token_number)],
             'buy_signal_CE': [int(self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'buy_signal_PE': [int(self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'CE_jump': [int(self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'PE_jump': [int(self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0])],
             'exchange': [self.exchg], 'symbol': [self.cur_symbol]})

        if not DEBUG and not session_ref_data.empty:
            inst_analysis_logger.info('%s / %s / %s / %s / %s / %s / %s / %s / %s' % (token_number, self.cur_symbol,session_ref_data['last_price'].values[0], olhc_max, olhc_min,
                                                                                      self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],
                                                                                      self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],
                                                                                      self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0],
                                                                                      self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == token_number].values[0]))

        return self.stock_info
    def osc_det(self,tick_data,token_number):
        '''Ued to detect market side ways when we buy both CE and PE. Not needed now'''
        if not self.open_positions.empty:
            self.open_positions['Ref_stock'] = [self.cum_table['Ref_stock_tkn'].loc[self.cum_table['instrument_token'] == token_number].values[0] for token_number in self.open_positions['instrument_token'].values]
            self.open_positions['instrument_type'] = [self.cum_table['instrument_type'].loc[self.cum_table['instrument_token'] == token_number].values[0] for token_number in self.open_positions['instrument_token'].values]
            for inst_tkn in self.open_positions['instrument_token']:
                ref_tkn = self.cum_table['Ref_stock_tkn'].loc[self.cum_table['instrument_token'] == inst_tkn].values[0]
                if not self.order_status.empty:
                    self.order_status['Ref_token'] = [self.cum_table['Ref_stock_tkn'].loc[self.cum_table['instrument_token'] == token_number].values[0] for token_number in self.order_status['instrument_token'].values]
                    self.order_status['instrument_type'] = [self.cum_table['instrument_type'].loc[self.cum_table['instrument_token'] == token_number].values[0] for token_number in self.order_status['instrument_token'].values]
                    #check both PE and CE and in open position
                    if (self.open_positions['instrument_type'][self.open_positions['instrument_token'] == ref_tkn].values == 'PE').any() and (self.open_positions['instrument_type'][self.open_positions['instrument_token'] == ref_tkn].values == 'CE').any():
                        self.ordr_lst = self.order_status.groupby('instrument_token').get_group(inst_tkn)  # to get all the latest entry
                        self.ordr_idx_ce = self.ordr_lst[(self.order_status['Ref_token'] == ref_tkn) &(self.ordr_lst['transaction_type']=='BUY') & (self.ordr_lst['status']=='COMPLETE') & (self.ordr_lst['instrument_type']=='CE') ].tail(1)
                        self.ordr_idx_pe = self.ordr_lst[(self.order_status['Ref_token'] == ref_tkn) &(self.ordr_lst['transaction_type']=='BUY') & (self.ordr_lst['status']=='COMPLETE') & (self.ordr_lst['instrument_type']=='PE') ].tail(1)# take last(recent) entry
                        temp_tick_data = tick_data[tick_data['instrument_token'] == ref_tkn]
                        if not self.ordr_idx_ce.empty:  # checking if order is present in the order dataframe
                            order_time = pd.to_datetime(self.ordr_idx_ce['exchange_timestamp'].values[0],utc=False).tz_localize('ASIA/Kolkata')
                            nearest_price_data = temp_tick_data[temp_tick_data['date_time'] > order_time]
                            # nearest_price_data.reset_index(drop=True, inplace=True)
                            nearest_price_data = nearest_price_data.set_index('date_time')
                            recent_olhc = self.tick_to_olhc(nearest_price_data, '1min', 'end', 'left', 'left')
                            fwd_10 = self.group_by_rolling_window(df=pd.concat([self.fwd_10, recent_olhc], ignore_index=True), window_size='10min')
                            # _, fwd_10 = self.olhc_trans(nearest_price_data, '10min', 1, 'start_day', 'left', 'left')
                            if fwd_10['close'][1:4].is_monotonic_decreasing :
                                self.minus_two_counter['buy_counter_CE'].loc[self.minus_two_counter['instrument_token'] == ref_tkn] = self.minus_two_counter['buy_counter_CE'].loc[self.minus_two_counter['instrument_token'] == ref_tkn] + 1
                                if int(self.minus_two_counter['buy_counter_CE'].loc[self.minus_two_counter['instrument_token'] == ref_tkn]) >= self.debounce_counter_threshold:
                                    self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == ref_tkn] = -2
                                    inst_analysis_logger.info('path_10_1')
                        if not self.ordr_idx_pe.empty:
                            order_time = pd.to_datetime(self.ordr_idx_pe['exchange_timestamp'].values[0],utc=False).tz_localize('ASIA/Kolkata')
                            nearest_price_data = temp_tick_data[temp_tick_data['date_time'] > order_time]
                            # nearest_price_data.reset_index(drop=True, inplace=True)
                            nearest_price_data = nearest_price_data.set_index('date_time')
                            recent_olhc = self.tick_to_olhc(nearest_price_data, '1min', 'end', 'left', 'left')
                            fwd_10 = self.group_by_rolling_window(df=pd.concat([self.fwd_10, recent_olhc], ignore_index=True), window_size='10min')
                            # _, fwd_10 = self.olhc_trans(nearest_price_data, '10min', 1, 'start_day', 'left', 'left')
                            if fwd_10['close'][1:4].is_monotonic_increasing:
                                self.minus_two_counter['buy_counter_PE'].loc[self.minus_two_counter['instrument_token'] == ref_tkn] = self.minus_two_counter['buy_counter_PE'].loc[self.minus_two_counter['instrument_token'] == ref_tkn] + 1
                                if int(self.minus_two_counter['buy_counter_PE'].loc[self.minus_two_counter['instrument_token'] == ref_tkn]) >= self.debounce_counter_threshold:
                                    self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == ref_tkn] = -2
                                    inst_analysis_logger.info('path_9_1')
                    self.exchg = self.cum_table[self.cum_table['instrument_token'] == token_number]['exchange'].values[0]  # find which exchange the instrument belongs to
                    self.cur_symbol = self.cum_table[self.cum_table['instrument_token'] == token_number]['tradingsymbol'].values[0]

                    self.osc_info = pd.DataFrame.from_dict(
                        {'instrument_token': [int(ref_tkn)],
                         'buy_signal_CE': [int(self.cum_table['buy_signal_CE'].loc[self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0])],
                         'buy_signal_PE': [int(self.cum_table['buy_signal_PE'].loc[self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0])],
                         'CE_jump': [int(self.cum_table['CE_jump'].loc[self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0])],
                         'PE_jump': [int(self.cum_table['PE_jump'].loc[self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0])],
                         'exchange': [self.exchg], 'symbol': [self.cur_symbol]})

        logger.info('exited osc detection')


    # @timeit
    def capital_allocation_calc(self, symbol, buy_list):
        '''Calculate the capital based on information from token ref excel'''
        capital_share_max = 0
        self.avail_cash, self.live_balance = self.client.chk_live_bal()
        if not self.order_status.empty:
            self.ordr_lst = self.order_status.loc[(self.order_status['Ref_stock']==self.cum_table[self.cum_table['tradingsymbol']==symbol]['Ref_stock'].values[0]) &(self.order_status['instrument_type']==self.cum_table[self.cum_table['tradingsymbol']==symbol]['instrument_type'].values[0])]
            # self.ordr_idx = self.ordr_lst.loc[self.ordr_lst['exchange_timestamp'] == self.ordr_lst['exchange_timestamp'].max()]
            self.ordr_idx = self.ordr_lst.tail(1)  # take last(recent) entry
            if not self.ordr_idx.empty:
                if self.ordr_idx['status'].values == 'OPEN':
                    current_order_open = True
                else:
                    current_order_open = False
            else:
                current_order_open = False
        else:
            current_order_open = False

        # pos_ref_info = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['Ref_stock'].values[0]
        if not current_order_open:
            if not self.open_positions.empty:
                pos_ref_info = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['Ref_stock'].values
                # total_invested = sum(abs((self.open_positions['multiplier']*self.open_positions['quantity']*self.open_positions['buy_price'])))
                total_invested = sum(abs((self.open_positions['quantity'] *self.open_positions['multiplier'] * self.open_positions['buy_price'] )))
                self.capital_available = self.live_balance + total_invested
                if self.capital_available > self.capital_allowed:
                    self.capital_available = self.capital_allowed
                if self.cum_table[self.cum_table['tradingsymbol']==symbol]['Ref_stock'].values[0] in pos_ref_info:
                    ref_stock = self.cum_table[self.cum_table['tradingsymbol']==symbol]['Ref_stock'].values[0]
                    if buy_list['instrument_type'].loc[buy_list['tradingsymbol'] == symbol].values == 'CE' :
                        if buy_list['buy_signal_CE'].loc[buy_list['tradingsymbol'] == symbol].values >= 1:
                            if len(pos_ref_info) > 0:
                                # self.open_positions['Ref_stock'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['Ref_stock'].values[::-1]#reversing
                                # self.open_positions['instrument_type'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['instrument_type'].values[::-1]#reversing
                                # current_pos = self.open_positions.groupby(['Ref_stock','instrument_type']).get_group((pos_ref_info,'CE'))
                                current_pos = self.open_positions.loc[(self.open_positions['Ref_stock'] == ref_stock) & (self.open_positions['instrument_type'] == 'CE')]
                                # current_capital_share = sum(abs((current_pos['multiplier']*current_pos['quantity']*current_pos['buy_price'])))
                                current_capital_share = sum(abs((current_pos['quantity'] *current_pos['multiplier'] * current_pos['buy_price'] )))

                            else:
                                self.pos_net_frame.assign(Ref_stock=None)
                                self.pos_net_frame.assign(intrument_type=None)
                                current_capital_share = 0

                            # capital_share_max = (self.capital_available * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values ) - current_capital_share
                            capital_share_max = self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values - current_capital_share
                            general_logger.info('allocated %s'% int(capital_share_max) + ' for ' + symbol )

                        elif buy_list['buy_signal_CE'].loc[buy_list['tradingsymbol'] == symbol].values == 2:
                            if len(pos_ref_info) > 0:
                                # self.open_positions['Ref_stock'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['Ref_stock'].values[::-1]#reversing
                                # self.open_positions['instrument_type'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['instrument_type'].values[::-1]#reversing
                                # current_pos = self.open_positions.groupby(['Ref_stock','instrument_type']).get_group((pos_ref_info,'CE'))
                                current_pos = self.open_positions.loc[(self.open_positions['Ref_stock'] == ref_stock) & (self.open_positions['instrument_type'] == 'CE') ]
                                # current_capital_share = sum(abs((current_pos['multiplier']*current_pos['quantity']*current_pos['buy_price'])))
                                current_capital_share =  sum(abs((current_pos['quantity'] *current_pos['multiplier'] * current_pos['buy_price'] )))

                            else:
                                self.pos_net_frame.assign(Ref_stock=None)
                                self.pos_net_frame.assign(intrument_type=None)
                                current_capital_share = 0

                            # capital_share_max = (self.capital_available * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values ) - current_capital_share
                            capital_share_max = self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values - current_capital_share
                            general_logger.info('allocated %s'% int(capital_share_max) + ' for ' + symbol )

                    elif buy_list['instrument_type'].loc[buy_list['tradingsymbol'] == symbol].values == 'PE':
                        if buy_list['buy_signal_PE'].loc[buy_list['tradingsymbol'] == symbol].values >= 1:

                            if len(pos_ref_info) > 0:
                                # self.open_positions['Ref_stock'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['Ref_stock'].values[::-1]  # reversing
                                # self.open_positions['instrument_type'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['instrument_type'].values[::-1]  # reversing
                                # current_pos = self.open_positions.groupby(['Ref_stock','instrument_type']).get_group((pos_ref_info,'CE'))
                                current_pos = self.open_positions.loc[(self.open_positions['Ref_stock'] == ref_stock) & (self.open_positions['instrument_type'] == 'PE') ]
                                # current_capital_share = sum(abs((current_pos['multiplier']*current_pos['quantity']*current_pos['buy_price'])))
                                current_capital_share =  sum(abs((current_pos['quantity'] *current_pos['multiplier'] * current_pos['buy_price'] )))

                            else:
                                self.pos_net_frame.assign(Ref_stock=None)
                                self.pos_net_frame.assign(intrument_type=None)
                                current_capital_share = 0

                            # capital_share_max = (self.capital_available * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values) - current_capital_share
                            capital_share_max = self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values - current_capital_share
                            general_logger.info('allocated %s' % int(capital_share_max) + ' for ' + symbol)

                        elif buy_list['buy_signal_PE'].loc[buy_list['tradingsymbol'] == symbol].values == 2:
                            if len(pos_ref_info) > 0:
                                # self.open_positions['Ref_stock'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['Ref_stock'].values[::-1]  # reversing
                                # self.open_positions['instrument_type'] = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]['instrument_type'].values[::-1]  # reversing
                                # current_pos = self.open_positions.groupby(['Ref_stock','instrument_type']).get_group((pos_ref_info,'CE'))
                                current_pos = self.open_positions.loc[(self.open_positions['Ref_stock'] == ref_stock) & (self.open_positions['instrument_type'] == 'PE')]
                                # current_capital_share = sum(abs((current_pos['multiplier']*current_pos['quantity']*current_pos['buy_price'])))
                                current_capital_share =  sum(abs((current_pos['quantity'] *current_pos['multiplier'] * current_pos['buy_price'] )))

                            else:
                                self.pos_net_frame.assign(Ref_stock=None)
                                self.pos_net_frame.assign(intrument_type=None)
                                current_capital_share = 0

                            # capital_share_max = (self.capital_available * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values ) - current_capital_share
                            capital_share_max = self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values - current_capital_share
                            general_logger.info('allocated %s' % int(capital_share_max) + ' for ' + symbol)
                elif buy_list['instrument_type'].loc[buy_list['tradingsymbol'] == symbol].values == 'CE':
                    if self.live_balance > self.capital_allowed:
                        # capital_share_max = self.capital_allowed * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                        capital_share_max = self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    else:
                        # capital_share_max = self.live_balance * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                        capital_share_max = self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    general_logger.info('allocated %s' % int(capital_share_max) + ' for ' + symbol + ' position empty detected')
                elif buy_list['instrument_type'].loc[buy_list['tradingsymbol'] == symbol].values == 'PE':
                    if self.live_balance > self.capital_allowed:
                        # capital_share_max = self.capital_allowed * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                        capital_share_max = self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    else:
                        # capital_share_max = self.live_balance * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                        capital_share_max = self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    general_logger.info('allocated %s' % int(capital_share_max) + ' for ' + symbol + ' position empty detected')
            elif buy_list['instrument_type'].loc[buy_list['tradingsymbol'] == symbol].values == 'CE':
                if self.live_balance > self.capital_allowed:
                    # capital_share_max = self.capital_allowed * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    capital_share_max = self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                else:
                    # capital_share_max = self.live_balance * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    capital_share_max = self.cum_table['Max_capital_CE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                general_logger.info('allocated %s'% int(capital_share_max) + ' for ' + symbol + ' position empty detected')
            elif buy_list['instrument_type'].loc[buy_list['tradingsymbol'] == symbol].values == 'PE':
                if self.live_balance > self.capital_allowed:
                    # capital_share_max = self.capital_allowed * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    capital_share_max = self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                else:
                    # capital_share_max = self.live_balance * self.cum_table['Capital_share'].loc[self.cum_table['tradingsymbol'] == symbol].values * self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                    capital_share_max = self.cum_table['Max_capital_PE'].loc[self.cum_table['tradingsymbol'] == symbol].values
                general_logger.info('allocated %s'% int(capital_share_max) + ' for ' + symbol + ' position empty detected')

        if capital_share_max <= 0:
            capital_share = 0
        else:
            capital_share = capital_share_max
        general_logger.info('allocated capital is %s' % int(capital_share) + ' for ' + symbol)

        return int(capital_share)

    # @timeit
    def buy_stk_qty(self, symbol, capital_share,buy_list):
        ''''this function is to find how many stocke to buy for given money and max and min stock qty allowed
        allocated_cash = comes from excel sheet
        stock_price = current price after denoising
        max_qty = maximum qty of lots to buy
        min_qty = minimum qty of lots to buy
        balance_chk_done = condition to chk live balance
        live_balance = avaliable cash with the broker
        '''
        # general_logger.info(symbol)

        try:
            if self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol].values == 'PE' and all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('CDS')) :
                # self.pu_stock_price_lst = pd.json_normalize(TickStore.get_recent_tickstore(instrument_tokens=[self.symbol_to_tkn(symbol)]))  # needs to be tested
                self.pu_stock_price_lst = self.tick_data.loc[(self.tick_data['instrument_token'] == self.symbol_to_tkn(symbol)) & (self.tick_data['date_time'].notna())].tail(1)['last_price'].values
                if len(self.pu_stock_price_lst) > 0:
                    self.adj_pu_buy_price = (self.pu_stock_price_lst['last_price'].values + (self.cum_table['tick_size'].loc[self.cum_table['tradingsymbol'] == symbol].values) * 0) * 1000
                else:
                    self.adj_pu_buy_price = 0
            elif self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol].values == 'CE' and all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('CDS')) :
                # self.pu_stock_price_lst = pd.json_normalize(TickStore.get_recent_tickstore(instrument_tokens=[self.symbol_to_tkn(symbol)]))  # needs to be tested
                self.pu_stock_price_lst = self.tick_data.loc[(self.tick_data['instrument_token'] == self.symbol_to_tkn(symbol)) & (self.tick_data['date_time'].notna())].tail(1)['last_price'].values
                if len(self.pu_stock_price_lst) > 0:
                    self.adj_pu_buy_price = (self.pu_stock_price_lst['last_price'].values + (self.cum_table['tick_size'].loc[self.cum_table['tradingsymbol'] == symbol].values) * 0) * 1000
                else:
                    self.adj_pu_buy_price = 0

            elif self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol].values == 'CE' and  all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('NFO')):
                # self.pu_stock_price_lst = pd.json_normalize(TickStore.get_recent_tickstore(instrument_tokens=[self.symbol_to_tkn(symbol)]))
                self.pu_stock_price_lst = self.tick_data.loc[(self.tick_data['instrument_token'] == self.symbol_to_tkn(symbol)) & (self.tick_data['date_time'].notna())].tail(1)['last_price'].values
                if len(self.pu_stock_price_lst) > 0:
                    self.adj_pu_buy_price = self.pu_stock_price_lst
                else:
                    self.adj_pu_buy_price = 0
            elif self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol].values == 'PE' and  all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('NFO')):
                # self.pu_stock_price_lst = pd.json_normalize(TickStore.get_recent_tickstore(instrument_tokens=[self.symbol_to_tkn(symbol)]))
                self.pu_stock_price_lst = self.tick_data.loc[(self.tick_data['instrument_token'] == self.symbol_to_tkn(symbol)) & (self.tick_data['date_time'].notna())].tail(1)['last_price'].values
                if len(self.pu_stock_price_lst) > 0:
                    self.adj_pu_buy_price = self.pu_stock_price_lst
                else:
                    self.adj_pu_buy_price = 0
            elif self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol].values == 'CE' and  all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('MCX')):
                # self.pu_stock_price_lst = pd.json_normalize(TickStore.get_recent_tickstore(instrument_tokens=[self.symbol_to_tkn(symbol)]))
                self.pu_stock_price_lst = self.tick_data.loc[(self.tick_data['instrument_token'] == self.symbol_to_tkn(symbol)) & (self.tick_data['date_time'].notna())].tail(1)['last_price'].values
                if len(self.pu_stock_price_lst) > 0:
                    self.adj_pu_buy_price = np.astype(self.pu_stock_price_lst * self.cum_table['Multiplier'][self.cum_table['tradingsymbol'] == symbol].values,float)
                else:
                    self.adj_pu_buy_price = 0
            elif self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol].values == 'PE' and  all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('MCX')):
                # self.pu_stock_price_lst = pd.json_normalize(TickStore.get_recent_tickstore(instrument_tokens=[self.symbol_to_tkn(symbol)]))
                self.pu_stock_price_lst = self.tick_data.loc[(self.tick_data['instrument_token'] == self.symbol_to_tkn(symbol)) & (self.tick_data['date_time'].notna())].tail(1)['last_price'].values
                if len(self.pu_stock_price_lst) > 0:
                    self.adj_pu_buy_price = np.astype(self.pu_stock_price_lst * self.cum_table['Multiplier'][self.cum_table['tradingsymbol'] == symbol].values,float)
                else:
                    self.adj_pu_buy_price = 0
            else:
                # self.max_capital = 0
                self.adj_pu_buy_price = 0
            general_logger.info('adjusted price is %s'% int(self.adj_pu_buy_price) + ' for ' + symbol)
        except Exception as e:
            general_logger.info('%s' % e, exc_info=True)
            self.adj_pu_buy_price = 0
            general_logger.info('adjusted price is zero for ' + symbol)
            raise
        # self.adj_pu_buy_price = np.round(self.adj_pu_buy_price - (np.divmod(self.adj_pu_buy_price,self.cum_table['tick_size'].loc[self.cum_table['tradingsymbol'] == symbol].values)[1][0]), 2)
        self.adj_pu_buy_price = np.ceil(self.adj_pu_buy_price/self.cum_table['tick_size'].loc[self.cum_table['tradingsymbol'] == symbol].values[0]) * self.cum_table['tick_size'].loc[self.cum_table['tradingsymbol'] == symbol].values[0]
        if all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('CDS')):  # for scaling down
            self.final_adj_pu_buy_price = np.astype(self.adj_pu_buy_price / 1000,float) + 0
        elif all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('MCX')):  # for scaling down
            self.final_adj_pu_buy_price = np.astype(self.adj_pu_buy_price / self.cum_table['Multiplier'][self.cum_table['tradingsymbol'] == symbol].values,float) + 0
        else:
            self.final_adj_pu_buy_price = np.astype(self.adj_pu_buy_price,float)

        if self.adj_pu_buy_price > 0 and capital_share > 0:
           self.qty, _ = np.divmod(capital_share,self.adj_pu_buy_price)  # replace stock price by value from margin update

        else:
            self.qty = 0
        general_logger.info('initial buy quantity is %s' % int(self.qty) + ' for ' + symbol + ' at %s' % int(self.adj_pu_buy_price))

        if all(self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].str.contains('NFO')):
            self.lot_size = self.cum_table['lot_size'].loc[self.cum_table['tradingsymbol'] == symbol].values[0]
            self.lot_count, _ = np.floor(np.divmod(int(self.qty), self.lot_size)).astype(int)
            if self.lot_count > self.cum_table['Max_lots_per_order'].loc[self.cum_table['tradingsymbol'] == symbol].values:  # to chk order freeze limit
                self.lot_count = self.cum_table['Max_lots_per_order'].loc[self.cum_table['tradingsymbol'] == symbol].values
            self.qty = self.lot_count * self.lot_size
        #lot limit logic
        if not self.open_positions.empty:
            sym_ref_info = self.cum_table[self.cum_table['tradingsymbol']==symbol]['Ref_stock'].values[0]
            sym_type_info = self.cum_table[self.cum_table['tradingsymbol']==symbol]['instrument_type'].values[0]
            self.existing_qty = sum(self.open_positions.loc[(self.open_positions['Ref_stock'] == sym_ref_info)  & (self.open_positions['instrument_type'] == sym_type_info)]['quantity']) # to enable repeated buy at different strike
        else:
            self.existing_qty = 0

        if self.qty > 0:
            if self.existing_qty < self.cum_table['Max_lots_per_order'].loc[self.cum_table['tradingsymbol'] == symbol].values :
                if self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == symbol].values == 'CE':
                    self.qty = min(self.cum_table['Max_lots_per_order'].loc[self.cum_table['tradingsymbol'] == symbol].values - self.existing_qty,self.qty)
                else:
                    self.qty = min(self.cum_table['Max_lots_per_order'].loc[self.cum_table['tradingsymbol'] == symbol].values - self.existing_qty,self.qty)
            elif self.existing_qty >= self.cum_table['Max_lots_per_order'].loc[self.cum_table['tradingsymbol'] == symbol].values:
                self.qty = 0
            if self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == symbol].values != 'MCX':
                self.qty = self.qty * self.lot_size
            if self.qty <0:
                self.qty = 0
        general_logger.info('buy quantity is %s' % int(self.qty) + ' for ' + symbol + ' at %s' % int(self.adj_pu_buy_price) )
        return int(self.qty), self.final_adj_pu_buy_price

    # @timeit
    def hold_pos_buy_chk(self, symbol_string, open_pos):
        '''
        pos_net_frame = our position
        symbol_string = stock name
        hold_pos_buy_sts = 1 means buy it
        hold_pos_buy_sts = 0 means do not buy
        '''

        self.hold_pos_buy_sts = 0  # init not to buy
        open_pos_lst = pd.DataFrame([])
        loss_value_exist = False
        ref_stock = self.cum_table['Ref_stock'][self.cum_table['tradingsymbol'] == symbol_string].values[0]
        inst_type = self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol_string].values[0]
        strike = self.cum_table['strike'][self.cum_table['tradingsymbol'] == symbol_string].values[0]
        for s,_ in self.loss_table_info.columns:
            if self.cum_table['Ref_stock'][self.cum_table['tradingsymbol'] == symbol_string].values[0][0:len(s)].startswith(s):
                loss_value_exist = True
                general_logger.info('loss value exists for ' + symbol_string)
                break

        if not open_pos.empty and loss_value_exist :
            open_pos_lst = open_pos[(open_pos['quantity'] > 0)]['tradingsymbol'].values
            open_pos_data = self.cum_table[self.cum_table.tradingsymbol.isin(open_pos_lst)]
            # if not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock) & (open_pos_data['instrument_type'] == inst_type)].empty:# enabling multiple buy attempts
            # if not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty: # disabling CE and PE simultaneous buy
                # enabling CE and PE simultaneous buy
                # if not any(open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)]): # disabling CE and PE simultaneous buy
                #     self.hold_pos_buy_sts = 1
                #     general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

            # else:
            #     self.hold_pos_buy_sts = 0
            #     general_logger.info('hold_pos_buy_sts is zero for ' + symbol_string)
            if not self.half_time(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]):
                if  open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty:
                    # if  open_pos_data[(open_pos_data['instrument_type'] == inst_type)].empty:
                    #     if not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock) & (open_pos_data['instrument_type'] != inst_type)]['strike'].empty:
                            # if strike != open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)& (open_pos_data['instrument_type'] != inst_type)]['strike'].values:
                    self.hold_pos_buy_sts = 1
                    general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

                elif not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty:
                    if not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock) & (open_pos_data['instrument_type'] != inst_type)].empty:

                                self.hold_pos_buy_sts = 0 # 1 means  simultaneous ce and pe buy
                                general_logger.info('hold_pos_buy_sts is zero for ' + symbol_string)
                    else:
                        self.hold_pos_buy_sts = 0 # 1 will prevent simultaneous ce and pe buy
                        general_logger.info('hold_pos_buy_sts is zero for ' + symbol_string)
            else:
                # if open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty:
                self.hold_pos_buy_sts = 1
                general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

                # elif not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty:
                #     if not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock) & (open_pos_data['instrument_type'] != inst_type)].empty:
                #
                #                 self.hold_pos_buy_sts = 0 # 1 means  simultaneous ce and pe buy
                #                 general_logger.info('hold_pos_buy_sts is zero for ' + symbol_string)
                #     else:
                #         self.hold_pos_buy_sts = 0 # 1 will prevent simultaneous ce and pe buy
                #         general_logger.info('hold_pos_buy_sts is zero for ' + symbol_string)

            if self.session_start(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]) and loss_value_exist:
                if open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty:
                    self.hold_pos_buy_sts = 1
                    general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

                # elif not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty:
                #     if (not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock) & (open_pos_data['instrument_type'] != inst_type)].empty) :
                #
                #         self.hold_pos_buy_sts = 1
                #         general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

        elif open_pos.empty and loss_value_exist:
            self.hold_pos_buy_sts = 1
            # if self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == 'MCX':
            #     if (self.cum_table['day_rise'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == 1 and inst_type == 'CE') or(self.cum_table['day_fall'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == 1 and inst_type == 'PE'):
            #         self.hold_pos_buy_sts = 1
            #         general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)
            #     else:
            #         self.hold_pos_buy_sts = 0
            #         general_logger.info('hold_pos_buy_sts is zero for ' + symbol_string)
            if self.session_start(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]):
                self.hold_pos_buy_sts = 1
                general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

        if self.next_session_closed(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]) and  self.session_end(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]) and loss_value_exist:
            self.hold_pos_buy_sts = 1
            general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

        if self.session_end(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]) and loss_value_exist:
            self.hold_pos_buy_sts = 1
            general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

        #     if not open_pos.empty :
        #         open_pos_lst = open_pos[(open_pos['quantity'] > 0)]['tradingsymbol'].values
        #         open_pos_data = self.cum_table[self.cum_table.tradingsymbol.isin(open_pos_lst)]
        #         if not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)  & (open_pos_data['instrument_type'] == inst_type)].empty:# enabling CE and PE simultaneous buy
        #         if not open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)].empty:  # disabling CE and PE simultaneous buy
            #         if not any(open_pos_data[(open_pos_data['Ref_stock'] == ref_stock)]):  # disabling CE and PE simultaneous buy
            #         self.hold_pos_buy_sts = 1
            #         general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)
            #
            #     else:
            #         self.hold_pos_buy_sts = 0
            #         general_logger.info('hold_pos_buy_sts is zero for ' + symbol_string)
            # else:
            #     self.hold_pos_buy_sts = 1
            #     general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

        # else:
        #     self.hold_pos_buy_sts = 1
        #     general_logger.info('hold_pos_buy_sts is one for ' + symbol_string)

        return self.hold_pos_buy_sts

    # @timeit
    def hold_pos_sell_chk(self, symbol_string, open_pos):
        '''
        hold_frame = our holdings
        pos_day_frame and pos_net_frame = our position
        symbol_string = stock name
        check stock is already present in holding or position
        hold_pos_sell_sts = 1 stock is ready to be sold
        hold_pos_sell_sts = 0 dont place sell order
        sell quantity shall be calculated based on holdings either by pos net data only not by summing both
        '''
        self.hold_pos_sell_sts = 0
        self.sell_qty = 0
        # check if stock is already bought

        if not open_pos.empty and len(open_pos[open_pos['tradingsymbol'] == symbol_string]) != 0:
            self.hold_pos_sell_sts = 1
            self.sell_qty = open_pos[open_pos['tradingsymbol'] == symbol_string]['quantity'].values

        else:
            self.hold_pos_sell_sts = 0
            self.sell_qty = 0

        general_logger.info(f'sell quantity is {self.sell_qty} for {symbol_string}')

        return self.hold_pos_sell_sts, self.sell_qty

    # @timeit
    def ordr_chk(self, symbol_string):
        ''''
        order_status = dataframe of placed orders
        symbol_string = stock name
        check stock is already present in order list
        ordr_sts = 0 means order not placed
        ordr_sts = 1 means order success
        ordr_sts = 2 means order pending or open
        ordr_sts = 3 means suspended from trading
        ordr_sts = 4 means fund not available
        ordr_sts = 5 exclusively added to sell after buying on the same day
        ordr_sts = 6 means order cancelled
        ordr_sts = 7 means order partially filled
        '''
        # print(symbol_string)
        self.ordr_sts = 0
        self.ordr_id = -1
        self.buy_list_remove = 0
        # reject the order if none of the below conditions satisfy
        if not self.order_status.empty:
            self.ordr_lst = self.order_status.loc[(self.order_status['Ref_stock'] == self.cum_table[self.cum_table['tradingsymbol'] == symbol_string]['Ref_stock'].values[0]) & (self.order_status['instrument_type'] == self.cum_table[self.cum_table['tradingsymbol'] == symbol_string]['instrument_type'].values[0])]
            self.ordr_idx = self.ordr_lst.tail(1)  # take last(recent) entry
            if not self.ordr_idx.empty:  # checking if order is present in the order dataframe
                self.trans_type = self.ordr_idx['transaction_type'].values[0]
                general_logger.info('transaction type ' + str(self.trans_type))
                if self.ordr_idx['status'].values == 'COMPLETE':
                    self.ordr_sts = 1
                elif self.ordr_idx['status'].values == 'PENDING':
                    self.ordr_sts = 0
                elif self.ordr_idx['status'].values == 'OPEN':
                    self.ordr_sts = 0
                elif  self.ordr_idx['status'].values == 'CANCELLED':
                    self.ordr_sts = 1
                elif self.ordr_idx['status'].values == 'REJECTED':
                    if self.ordr_idx['status_message'].str.contains('suspended from trading').any():
                        self.ordr_sts = 0
                    elif self.ordr_idx['status_message'].str.contains('Insufficient funds').any():
                        self.ordr_sts = 1
                    else:
                        self.ordr_sts = 8
                    # self.ordr_sts = 3
                elif self.ordr_idx['status_message'].str.contains('PARTIAL').any():
                    self.ordr_sts = 7
                # if all(self.ordr_idx['status'].str.contains('COMPLETE')) and any(self.ordr_idx['transaction_type'].str.contains('BUY')) and all(self.cum_table[self.cum_table['tradingsymbol'] == symbol_string]['instrument_type'] == 'PE'):  # allow selling and buying again on bought day
                #     self.ordr_sts = 5
            else:
                self.trans_type = 'none'
                self.ordr_sts = 1
                self.ordr_id = -1
            self.ordr_id = self.ordr_lst.loc[self.ordr_lst['exchange_timestamp'] == self.ordr_lst['exchange_timestamp'].max()]['order_id'].values
            # general_logger.info('%s / %s / %s ' % (self.ordr_sts, int(self.ordr_id), self.trans_type) + symbol_string + ' order chk result')
        else:
            self.trans_type = 'none'
            self.ordr_sts = 1
            self.ordr_id = -1
            # general_logger.info('%s / %s / %s ' % (self.ordr_sts, int(self.ordr_id), self.trans_type) + symbol_string + ' order chk result')
        # order delay logic
        try:
            # self.prev_order = self.order_status.groupby('Ref_stock').get_group(self.cum_table['Ref_stock'][self.cum_table['tradingsymbol'] == symbol_string].values[0]).tail(1)
            self.prev_order = self.order_status.groupby('tradingsymbol').get_group(symbol_string).tail(1)
        except:
            self.prev_order = pd.DataFrame([])

        if not self.prev_order.empty and not self.session_end(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]):
        #     if self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == self.prev_order['instrument_type'].values[0]:
        #         if self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == 'CE':
        #             if self.cum_table['buy_signal_CE'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == 1:
            if not self.prev_order['status'].values[0] == 'CANCELLED' :
                        if (datetime.now()- pd.to_datetime(self.prev_order['exchange_timestamp'].values[0])).total_seconds() > 30:

                                        self.ordr_sts = 1
                                        self.trans_type =self.prev_order['transaction_type'].values[0]
                                        self.buy_list_remove = 0
                        else:
                                        self.ordr_sts = 0
                                        general_logger.info('buy order delayed')
                                        self.trans_type = self.prev_order['transaction_type'].values[0]
                                        self.buy_list_remove = 1

            if (timezone.make_naive(timezone.now()) - pd.to_datetime(self.cum_table['recent_buy_order_time'][self.cum_table['tradingsymbol'] == symbol_string].values[0])).total_seconds() < 125:
                self.ordr_sts = 0
                general_logger.info('buy order delayed')
                general_logger.info(str(self.cum_table['recent_buy_order_time'][self.cum_table['tradingsymbol'] == symbol_string].values[0]))
                general_logger.info('current time ' + str(timezone.make_naive(timezone.now())))
                general_logger.info('diff ' + str((timezone.make_naive(timezone.now()) - pd.to_datetime(self.cum_table['recent_sell_order_time'][self.cum_table['tradingsymbol'] == symbol_string].values[0])).total_seconds()))
                self.trans_type = 'none'
                self.buy_list_remove = 0

        else:
            self.trans_type = 'none'
            self.ordr_sts = 1
            self.ordr_id = -1
            self.buy_list_remove = 0

        general_logger.info('%s  / %s ' % (self.ordr_sts,  self.trans_type) + symbol_string + ' order chk result')
        return self.ordr_sts, self.ordr_id, self.trans_type,self.buy_list_remove

    def ordr_sell_chk(self, symbol_string):
        ''''
        order_status = dataframe of placed orders
        symbol_string = stock name
        check stock is already present in order list
        ordr_sts = 0 means order not placed
        ordr_sts = 1 means order success
        ordr_sts = 2 means order pending or open
        ordr_sts = 3 means suspended from trading
        ordr_sts = 4 means fund not available
        ordr_sts = 5 exclusively added to sell after buying on the same day
        ordr_sts = 6 means order cancelled
        ordr_sts = 7 means order partially filled
        '''
        # print(symbol_string)
        self.ordr_sts = 0
        self.ordr_id = -1

        # reject the order if none of the below conditions satisfy
        if not self.order_status.empty:
            # if self.order_status['tradingsymbol'].str.fullmatch(symbol_string).any():  # check if symbol is present
            self.ordr_lst = self.order_status.loc[(self.order_status['Ref_stock'] == self.cum_table[self.cum_table['tradingsymbol'] == symbol_string]['Ref_stock'].values[0]) & (self.order_status['instrument_type'] == self.cum_table[self.cum_table['tradingsymbol'] == symbol_string]['instrument_type'].values[0])]
            # self.ordr_idx = self.ordr_lst.loc[self.ordr_lst['exchange_timestamp'] == self.ordr_lst['exchange_timestamp'].max()]
            self.ordr_idx = self.ordr_lst.tail(1)  # take last(recent) entry
            if not self.ordr_idx.empty:  # checking if order is present in the order dataframe
                self.trans_type = self.ordr_idx['transaction_type'].values[0]
                general_logger.info('transaction type '+str(self.trans_type))
                if  self.ordr_idx['status'].values == 'COMPLETE':
                    self.ordr_sts = 1
                elif self.ordr_idx['status'].values == 'PENDING':
                    self.ordr_sts = 0
                elif self.ordr_idx['status'].values == 'OPEN':
                    self.ordr_sts = 0
                elif  self.ordr_idx['status'].values == 'CANCELLED':
                    self.ordr_sts = 1
                elif self.ordr_idx['status'].values == 'REJECTED':
                    if self.ordr_idx['status_message'].str.contains('suspended from trading').any():
                        self.ordr_sts = 0
                    elif self.ordr_idx['status_message'].str.contains('Insufficient funds').any():
                        self.ordr_sts = 1
                    else:
                        self.ordr_sts = 8
                    # self.ordr_sts = 3
                elif self.ordr_idx['status_message'].str.contains('PARTIAL').any():
                    self.ordr_sts = 7
                # if all(self.ordr_idx['status'].str.contains('COMPLETE')) and any(self.ordr_idx['transaction_type'].str.contains('BUY')) and all(self.cum_table[self.cum_table['tradingsymbol'] == symbol_string]['instrument_type'] == 'PE'):  # allow selling and buying again on bought day
                #     self.ordr_sts = 5
            else:
                self.trans_type = 'none'
                self.ordr_sts = 1
                self.ordr_id = -1
            self.ordr_id = self.ordr_lst.loc[self.ordr_lst['exchange_timestamp'] == self.ordr_lst['exchange_timestamp'].max()]['order_id'].values
            # order delay logic
            try:
                self.prev_order = self.order_status.groupby('tradingsymbol').get_group(symbol_string).tail(1)
                # self.prev_order = self.prev_order.groupby('instrument_type').get_group(self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol_string].values[0]).tail(1)
            except:
                self.prev_order = pd.DataFrame([])

            if not self.prev_order.empty and not self.session_end(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == symbol_string].values[0]):
            #     if self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == self.prev_order['instrument_type'].values[0]:
            #         if self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == 'CE':
            #             if self.cum_table['buy_signal_CE'][self.cum_table['tradingsymbol'] == symbol_string].values[0] == 1:
                if not self.prev_order['status'].values[0] == 'CANCELLED' :
                            if (datetime.now()- pd.to_datetime(self.prev_order['exchange_timestamp'].values[0])).total_seconds() > 5:

                                            self.ordr_sts = 1
                                            self.trans_type =self.prev_order['transaction_type'].values[0]
                                            self.buy_list_remove = 0
                            else:
                                            self.ordr_sts = 0
                                            general_logger.info('sell order delayed')
                                            self.trans_type = self.prev_order['transaction_type'].values[0]
                                            self.buy_list_remove = 1

                if (timezone.make_naive(timezone.now())- pd.to_datetime(self.cum_table['recent_sell_order_time'][self.cum_table['tradingsymbol'] == symbol_string].values[0])).total_seconds() < 5:
                    self.ordr_sts = 0
                    general_logger.info('sell order delayed')
                    general_logger.info(str(self.cum_table['recent_sell_order_time'][self.cum_table['tradingsymbol'] == symbol_string].values[0]))
                    general_logger.info('current time '+ str(timezone.make_naive(timezone.now())))
                    general_logger.info('diff ' + str((timezone.make_naive(timezone.now())- pd.to_datetime(self.cum_table['recent_sell_order_time'][self.cum_table['tradingsymbol'] == symbol_string].values[0])).total_seconds()))
                    self.trans_type =  'none'
                    self.buy_list_remove = 1
        else:
            self.trans_type = 'none'
            self.ordr_sts = 1
            self.ordr_id = -1


        general_logger.info('%s  / %s ' % (self.ordr_sts, self.trans_type) + symbol_string + ' order chk result')
        return self.ordr_sts, self.ordr_id, self.trans_type

    @timeit
    def buy_margin_update(self):
        ''' thi function calculate the margin needed for one quantity only
        How many quantity to buy will be calculated by stk_qty function
        # https://github.com/zerodha/pykiteconnect/blob/master/examples/order_margins.py#L33
        '''
        self.buy_margin_lsit = pd.DataFrame()
        self.buy_margin_lsit['tradingsymbol'] = self.buy_stock_cap['tradingsymbol']
        self.buy_margin_lsit['exchange'] = self.buy_stock_cap['exchange']
        self.buy_margin_lsit['transaction_type'] = ['BUY'] * len(self.buy_stock_cap)
        self.buy_margin_lsit['variety'] = ['regular'] * len(self.buy_stock_cap)
        # if exchange is 'NFO' use MIS instead of 'CNC'
        self.buy_margin_lsit['product'] = ['CNC' if self.buy_stock_cap['exchange'][self.buy_stock_cap['instrument_token'] == x].values == 'NFO' else 'MIS' for x in self.buy_stock_cap['instrument_token']]
        self.buy_margin_lsit['order_type'] = ['MARKET'] * len(self.buy_stock_cap)
        self.buy_margin_lsit['quantity'] = np.ones(len(self.buy_stock_cap))  # get price of one stock
        self.buy_margin_lsit = self.buy_margin_lsit.astype(
            {'tradingsymbol': 'str', 'exchange': 'str', 'transaction_type': 'str', 'variety': 'str', 'product': 'str',
             'order_type': 'str', 'quantity': 'int'})
        self.calc_buy_margin = self.client.get_margin(self.buy_margin_lsit)
        return self.calc_buy_margin

    @timeit
    def sell_margin_update(self):
        ''' thi function calculate the margin needed for one quantity only
        How many quantity to buy will be calculated by stk_qty function
        # https://github.com/zerodha/pykiteconnect/blob/master/examples/order_margins.py#L33
        '''
        self.sell_margin_lsit = pd.DataFrame()
        self.sell_margin_lsit['tradingsymbol'] = self.sell_stock_table['symbol']
        self.sell_margin_lsit['exchange'] = self.sell_stock_table['exchange']
        self.sell_margin_lsit['transaction_type'] = ['SELL'] * len(self.sell_stock_table)
        self.sell_margin_lsit['variety'] = ['regular'] * len(self.sell_stock_table)
        # if exchange is 'NFO' use MIS instead of 'CNC'
        self.sell_margin_lsit['product'] = ['CNC' if self.sell_stock_table['exchange'][self.sell_stock_table['instrument_token'] == x].values == 'NFO' else 'MIS' for x in self.sell_stock_table['instrument_token']]
        self.sell_margin_lsit['order_type'] = ['LIMIT'] * len(self.sell_stock_table)
        self.sell_margin_lsit['quantity'] = np.ones(len(self.sell_stock_table))  # get price of one stock
        self.sell_margin_lsit = self.sell_margin_lsit.astype(
            {'tradingsymbol': 'str', 'exchange': 'str', 'transaction_type': 'str', 'variety': 'str', 'product': 'str',
             'order_type': 'str', 'quantity': 'int'})
        self.calc_sell_margin = self.client.get_margin(self.sell_margin_lsit)
        return self.calc_sell_margin

    # @timeit
    def execute_sell(self, sym, tkn, qty, exchg, order_status, order_id):
        '''Execute the limit order based on the order status (i.e. if order is pending for a long time cancel it)  and counter
        cancel the previous order if not successful in the worst case execute a market order
        - increase order counter only if order is pending if it is completed reset counter
        - if the counter is crossing a threshold then cancel the limit order and put a market order
        '''
        general_logger.info('entered execute sell function')
        self.order_result = -1
        ordr_msg = 'order not placed'
        if exchg == 'MCX' and self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'CE':
            validity = 'DAY'
        elif exchg == 'MCX' and self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'PE':
            validity = 'DAY'
        else:
            validity = 'TTL'
        general_logger.info('updating price for selling')
        # self.pu_sell_price = TickStore.get_recent_tickstore(instrument_tokens=[tkn])
        self.pu_sell_price = self.tick_data[self.tick_data['instrument_token'] == tkn].tail(1)['last_price'].values
        if len(self.pu_sell_price) > 0:
            self.adj_sell_price = self.pu_sell_price   # sell at slightly low price
            self.adj_sell_price = np.round(self.adj_sell_price - (np.divmod(self.adj_sell_price,self.cum_table['tick_size'].loc[self.cum_table['tradingsymbol'] == sym].values)[1][0]), 2)

            if order_status != 2 or order_status != 7:
                if (not DEBUG and not self.is_weekend()  and self.exchg_time_sell_chk(exchg) and not self.is_holiday(exchg)) or self.special_session_chk(exchg) :
                    general_logger.info('placing limit order for sell')
                    if self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'PE' and self.session_end(self.tkn_to_exchg(tkn)):
                        self.order_result,ordr_msg = self.client.lim_ordr(sym, qty, 'SELL', exchg, 'NRML', self.adj_sell_price,self.sell_ttl_value,validity)
                        # trade_logger.info('%s / %s / %s / %s ' % (sym, 'SELL', self.adj_sell_price, 'LIMIT'))
                    elif self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'PE':
                        self.order_result,ordr_msg = self.client.lim_ordr(sym, qty, 'SELL', exchg, 'NRML', self.adj_sell_price,self.sell_ttl_value,validity)
                        # trade_logger.info('%s / %s / %s / %s ' % (sym, 'SELL', self.adj_sell_price, 'LIMIT'))
                    if self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'CE' and self.session_end(self.tkn_to_exchg(tkn)):
                        self.order_result,ordr_msg = self.client.lim_ordr(sym, qty, 'SELL', exchg, 'NRML', self.adj_sell_price,self.sell_ttl_value,validity)
                        # trade_logger.info('%s / %s / %s / %s ' % (sym, 'SELL', self.adj_sell_price, 'LIMIT'))
                    elif self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'CE':
                        self.order_result,ordr_msg = self.client.lim_ordr(sym, qty, 'SELL', exchg, 'NRML', self.adj_sell_price,self.sell_ttl_value,validity)

                    time.sleep(2)
                    if self.order_result != -1:
                        self.cum_table['recent_sell_order_time'].loc[self.cum_table['tradingsymbol'] == sym] =[timezone.make_naive(timezone.now())]
                        trade_logger.info('%s / %s / %s / %s ' % (sym, 'SELL', self.adj_sell_price, 'LIMIT'))
                    else :
                        trade_logger.info('%s / %s / %s / %s ' % (sym, 'SELL', self.adj_sell_price,ordr_msg))


        return self.order_result

    # @timeit
    def execute_buy(self, sym, tkn, qty, exchg, order_status, order_id, price):
        '''Execute the limit order based on the order status (i.e. if order is pending for a long time cancel it)  and counter
                cancel the previous order if not successful cancel the order
                -never attempt a market order
                if limit order is pending and counter threshold is reached cancel the limit order and put a new limit order
                '''
        # general_logger.info('entered execute buy function')

        self.order_result = -1
        ordr_msg = 'order not placed'
        # if (order_status != 2 or order_status != 7 or order_status != 5):
        logger.info('calculating order type')
        self.lot_size = self.cum_table['lot_size'].loc[self.cum_table['tradingsymbol'] == sym].values
        self.max_lots_per_order = self.cum_table['Max_lots_per_order'].loc[self.cum_table['tradingsymbol'] == sym].values
        self.lot_count, _ = np.floor(np.divmod(qty, self.lot_size)).astype(int)

        if exchg == 'MCX' and self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'CE':
            validity = 'DAY'
        elif exchg == 'MCX' and self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'PE':
            validity = 'DAY'
        else:
            validity = 'TTL'

        if self.max_lots_per_order >= self.lot_count or self.cum_table['exchange'].loc[self.cum_table['tradingsymbol'] == sym].values == 'MCX':
            self.qty_per_order = self.lot_count * self.lot_size

            if (not DEBUG and not self.is_weekend()  and self.exchg_time_buy_chk(exchg) and not self.is_holiday(exchg)) or self.special_session_chk(exchg) :
                if self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'PE' :  # to place intraday PE
                    general_logger.info('placing limit order to buy')
                    self.order_result,ordr_msg = self.client.lim_ordr(sym, self.qty_per_order, 'BUY', exchg, 'NRML', price,self.buy_ttl_value,validity ) #ass

                    # StopLoss.create(order_id=self.order_result, buy_price=price,token_no=tkn,tradingsymbol=sym)
                    general_logger.info('order executed for ' + sym)

                elif self.cum_table['instrument_type'].loc[self.cum_table['tradingsymbol'] == sym].values == 'CE':
                    general_logger.info('placing limit order to buy')
                    self.order_result,ordr_msg = self.client.lim_ordr(sym, self.qty_per_order, 'BUY', exchg, 'NRML', price,self.buy_ttl_value,validity)#ass

                    # StopLoss.create(order_id=self.order_result, buy_price=price,token_no=tkn,tradingsymbol=sym)
                    general_logger.info('order executed for ' + sym)
                time.sleep(2)
                if self.order_result != -1:

                    self.cum_table['recent_buy_order_time'].loc[self.cum_table['tradingsymbol'] == sym] = [timezone.make_naive(timezone.now())]
                    # StrikeEntry.create(token_no=self.cum_table['instrument_token'].loc[self.cum_table['tradingsymbol'] == sym].values[0], strike_value=self.tick_data[self.tick_data['instrument_token'] == self.cum_table[self.cum_table['tradingsymbol'] == sym]['Ref_stock_tkn'].values[0]]['last_price'].tail(1), tradingsymbol=sym,ref_symbol=self.cum_table['Ref_stock'].loc[self.cum_table['tradingsymbol'] == sym].values[0])
                    trade_logger.info('%s / %s / %s / %s / %s ' % (self.order_result, sym, 'BUY', price, 'LIMIT'))
                else:
                    trade_logger.info('%s / %s / %s / %s / %s ' % (self.order_result, sym, 'BUY', price, ordr_msg))

        else:
            self.lot_per_leg = self.max_lots_per_order  # optimising iceberg orders for minimising order count
            self.total_legs, _ = np.floor(np.divmod(self.lot_count[0], self.lot_per_leg[0])).astype(int)
            if self.total_legs >= 9:
                self.total_legs = 9
            self.qty_per_order = self.lot_per_leg * self.lot_size
            self.total_qty = self.qty_per_order * self.total_legs

            if not DEBUG and (not self.is_weekend() or self.special_session) and self.exchg_time_buy_chk(exchg):
                general_logger.info('placing iceberg limit order')
                self.order_result,ordr_msg = self.client.ice_ordr(sym, self.qty_per_order, 'BUY', exchg, 'NRML', price, self.total_legs, self.total_qty, self.buy_ttl_value, 'LIMIT',validity) #ass
                # StopLoss.create(order_id=self.order_result, buy_price=price,token_no=tkn,tradingsymbol=sym)
                general_logger.info('order executed for ' + sym)
                # self.order_pending_counter['order_counter'].loc[self.order_pending_counter['instrument_token'] == tkn] = 0
                time.sleep(2)
                if self.order_result != -1:
                    # StrikeEntry.create(token_no=self.cum_table['instrument_token'].loc[self.cum_table['tradingsymbol'] == sym].values[0],strike_value=self.tick_data[self.tick_data['instrument_token'] == self.cum_table[self.cum_table['tradingsymbol'] == sym]['Ref_stock_tkn'].values[0]]['last_price'].tail(1),tradingsymbol=sym,ref_symbol=self.cum_table['Ref_stock'].loc[self.cum_table['tradingsymbol'] == sym].values[0])
                    self.cum_table['recent_buy_order_time'].loc[self.cum_table['tradingsymbol'] == sym] = [timezone.make_naive(timezone.now())]
                    trade_logger.info('%s / %s / %s / %s / %s ' % (self.order_result, sym, 'BUY', price, 'ICE_LIMIT'))
                else:
                    trade_logger.info('%s / %s / %s / %s / %s ' % (self.order_result, sym, 'BUY', price, ordr_msg))

        return self.order_result

    # @timeit
    def buy_sell_loop(self):
        '''
        This function takes buy,sell lists,order list, positions and holdings places order and updates position and order lists
        buy_stock_cap_1 = stocks list that can be bought
        sell_stock_table = stocks list that can be sold
        order_status = dataframe of placed orders
        hold_frame = our holdings
        pos_day_frame ,pos_net_frame = our position
        live_balance_lower_limit = minimum amout below which trade cannot be executed
        session_data = data from database
        margin_per_stock = the alloted money for every stock
        cap_config = configuration data from excel
        nfo_cds_mcx = all instruments in nfo exchange
        nse_inst_data = all instruments in nse exchange
        '''
        # general_logger.info('entered buy sell loop')
        self.balance_chk_done = 0  # initialising balance check
        if len(self.sell_stock_table) == 0:
            general_logger.info('sell loop empty')
        else:
            self.sell_stock_table = self.sell_stock_table.drop_duplicates(subset=['instrument_token'])
            self.sell_stock_table.reset_index(drop=True, inplace=True)
        if len(self.buy_stock_cap)== 0:
            general_logger.info('buy loop empty')
        else:
            self.buy_stock_cap = self.buy_stock_cap.drop_duplicates(subset=['instrument_token'])
            self.buy_stock_cap.reset_index(drop=True, inplace=True)
            # general_logger.info(self.buy_stock_cap['tradingsymbol'].values)

        for ee in range(len(self.sell_stock_table)):
            general_logger.info('entered sell loop')
            try:
                self.hold_pos_sell_sts, self.sell_qty = self.hold_pos_sell_chk(self.sell_stock_table['tradingsymbol'].loc[ee], self.open_positions)
                if self.hold_pos_sell_sts == 1:
                    general_logger.info(' sell hold postion check passed')
                    if self.sell_qty > 0:
                        self.sell_ordr_chk, self.ordr_id, self.trans_type = self.ordr_sell_chk(self.sell_stock_table['tradingsymbol'].loc[ee])
                        # if (self.trans_type == 'BUY' and self.sell_ordr_chk == 1) or self.trans_type == 'none' or (self.trans_type == 'SELL' and self.sell_ordr_chk == 6  or self.sell_ordr_chk == 3):
                            # general_logger.info('sell on hold')
                        if  self.sell_ordr_chk == 1:
                            general_logger.info('order status check passed for selling')
                            self.order_result = self.execute_sell(self.sell_stock_table['tradingsymbol'].loc[ee],
                                                                  self.sell_stock_table['instrument_token'].loc[ee],
                                                                  self.sell_qty, self.sell_stock_table['exchange'].loc[ee],
                                                                  self.sell_ordr_chk, self.ordr_id,
                                                                  )
                            time.sleep(1)
                        # self.pos_day_frame, self.pos_net_frame = self.client.pos_data()  # update position after every order
                        # self.open_positions = self.open_position_update(self.pos_day_frame, self.pos_net_frame)
                        # self.order_status = self.order_status_update()
            except Exception as e:
                general_logger.exception('%s' % e, exc_info=True)
                continue


        # buy_loop
        for e in range(len(self.buy_stock_cap)):
            self.avail_cash, self.live_balance = self.client.chk_live_bal()
            general_logger.info('entered buy loop')
            if self.live_balance > 0 and not self.buy_stock_cap.empty:  # to initiate buy only if we have balance and data
                try:
                    if self.buy_stock_cap['tradable'].loc[e] == 1:  # check stock is tradable or not
                        general_logger.info('buy loop for ' + self.buy_stock_cap['tradingsymbol'].loc[e])
                        # general_logger.info('buy tradability check done')
                        # max capital calculation
                        self.hold_pos_buy_sts = self.hold_pos_buy_chk(self.buy_stock_cap['tradingsymbol'].loc[e],self.open_positions) #disable this chk to buy same ref_stock at different strikes
                        general_logger.info('hold_pos_buy_sts is '+ str(self.hold_pos_buy_sts) +' for '+ self.buy_stock_cap['tradingsymbol'].loc[e])
                        # self.hold_pos_buy_sts = 1 #disabling hold pos chk
                        if self.hold_pos_buy_sts == 1 or DEBUG:
                            self.orde_chk_sts, self.ordr_id, self.trans_type,self.buy_list_remove = self.ordr_chk(self.buy_stock_cap['tradingsymbol'].loc[e])
                            # if self.orde_chk_sts != 2 and self.orde_chk_sts != 7 and self.orde_chk_sts != 5 and self.orde_chk_sts != 3 :
                            if  self.orde_chk_sts == 1 or DEBUG:
                                general_logger.info('order status check passed for buying '+ self.buy_stock_cap['tradingsymbol'].loc[e] )
                                self.allocated_capital = self.capital_allocation_calc(self.buy_stock_cap['tradingsymbol'].loc[e], self.buy_stock_cap)
                                if self.allocated_capital > 0:
                                    self.qty, self.adj_pu_buy_price = self.buy_stk_qty(self.buy_stock_cap['tradingsymbol'].loc[e], self.allocated_capital,self.buy_stock_cap)
                                    if self.qty > 0:
                                        # check if this stock order is pending or partly filled or insufficient balance or trading is suspended
                                        self.order_result = self.execute_buy(self.buy_stock_cap['tradingsymbol'].loc[e],
                                                                             self.buy_stock_cap['instrument_token'].loc[e],
                                                                             self.qty, self.buy_stock_cap['exchange'].loc[e],
                                                                             self.orde_chk_sts, self.ordr_id,
                                                                             self.adj_pu_buy_price,
                                                                             )

                                        time.sleep(1)
                                    else:
                                        self.order_result = -1

                                    # update balance check only if order is successful
                                    if self.order_result != -1 :
                                                self.balance_chk_done = 1
                                    else:
                                        self.balance_chk_done = 0  # check balance after order
                            else:
                                self.balance_chk_done = 0
                            # self.pos_day_frame, self.pos_net_frame = self.client.pos_data()  # update position after every order
                            # self.open_positions = self.open_position_update(self.pos_day_frame, self.pos_net_frame)
                            # self.order_status = self.order_status_update()

                except Exception as e:
                    general_logger.exception('%s' % e, exc_info=True)
                    continue


    # @timeit
    def strike_detect(self, tick_data):
        '''Used to find which strike to enter'''
        # self.cum_table['ATM_ITM_OTM'] = ['NA'] * len(self.cum_table)  # reset the status on every iteration
        self.cum_table['Buy_strike'] = ['NA'] * len(self.cum_table)
        self.upper_list_ce = pd.DataFrame([])
        self.upper_list_pe = pd.DataFrame([])
        self.grp_data_ce = pd.DataFrame([])
        self.grp_data_pe = pd.DataFrame([])
        for ref_tkn in self.all_ref_tkns:
            cap_info = self.cum_table['Cap_info'][self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0]
            if ref_tkn in self.cum_table['Ref_stock_tkn'].values and ref_tkn != -1 and not tick_data[tick_data['instrument_token'] == ref_tkn]["last_price"].empty:
                try:
                    self.grp_data_ce = self.cum_table.groupby(['Ref_stock', 'instrument_type', 'cap']).get_group((self.cum_table['Ref_stock'][self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0],  'CE',cap_info))
                    # self.grp_data_ce = self.grp_data_ce[(self.grp_data_ce['month_dist'] < 2) & (pd.to_datetime(self.grp_data_ce['expiry']) > (datetime.today() + timedelta(days=1)))]
                    self.grp_data_ce = self.grp_data_ce[pd.to_datetime(self.grp_data_ce['expiry']) > (datetime.today() + timedelta(days=1))]
                    self.grp_data_ce = self.grp_data_ce[self.grp_data_ce['exp_date_list'] > (self.month_cutoff )]
                    self.grp_data_ce = self.grp_data_ce[self.grp_data_ce['week_dist'] == self.grp_data_ce['week_dist'].min()]
                    if cap_info == 'weekely_options':
                        self.grp_data_ce = self.cum_table.groupby(['Ref_stock',  'instrument_type', 'cap']).get_group((self.cum_table['Ref_stock'][self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0],  'CE', cap_info))
                        # self.grp_data_ce = self.grp_data_ce[(self.grp_data_ce['month_dist'] < 2)&(pd.to_datetime(self.grp_data_ce['expiry']) >datetime.today()+timedelta(days=1))]
                        self.grp_data_ce = self.grp_data_ce[pd.to_datetime(self.grp_data_ce['expiry']) > (datetime.today() + timedelta(days=1))]
                        self.grp_data_ce = self.grp_data_ce[self.grp_data_ce['exp_date_list'] > (self.month_cutoff)]
                        self.grp_data_ce = self.grp_data_ce[self.grp_data_ce['week_dist'] == self.grp_data_ce['week_dist'].min()]

                except:

                        self.grp_data_ce = self.cum_table.groupby(['Ref_stock', 'instrument_type', 'cap']).get_group((self.cum_table['Ref_stock'][self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0],'CE', cap_info))
                        # self.grp_data_ce = self.grp_data_ce[(self.grp_data_ce['month_dist'] < 2) & (pd.to_datetime(self.grp_data_ce['expiry']) > datetime.today() + timedelta(days=1))]
                        self.grp_data_ce = self.grp_data_ce[pd.to_datetime(self.grp_data_ce['expiry']) > (datetime.today() + timedelta(days=1))]
                        self.grp_data_ce = self.grp_data_ce[self.grp_data_ce['exp_date_list'] > (self.month_cutoff)]
                        self.grp_data_ce = self.grp_data_ce[self.grp_data_ce['week_dist'] == self.grp_data_ce['week_dist'].min()]

                # if not tick_data['last_price'][tick_data['instrument_token'] == ref_tkn].ewm(com=2).mean().tail(1).empty and not self.grp_data_ce.empty:
                if not self.grp_data_ce.empty and not self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_tkn].empty:
                    # self.est_strike_ce = int(tick_data['last_price'][tick_data['instrument_token'] == ref_tkn].ewm(com=2).mean().values[-1]) + self.cum_table[self.cum_table['Ref_stock_tkn'] == ref_tkn]['Strike_dist_CE'].values[0]
                    self.est_strike_ce =int((self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_tkn][['close', 'open']][1:2].mean(axis=1))) + self.cum_table[self.cum_table['Ref_stock_tkn'] == ref_tkn]['Strike_dist_CE'].values[0]
                    self.dist_calc_ce = self.grp_data_ce['strike'] / self.est_strike_ce
                    self.upper_list_ce = self.dist_calc_ce[self.dist_calc_ce >= 1].nsmallest(1)
                    # self.upper_list_ce = self.grp_data_ce[self.grp_data_ce['strike'] == min(self.grp_data_ce['strike'], key=lambda x: abs(x - self.est_strike_ce))]
                    # print(min(self.grp_data_ce['strike'], key=lambda x: abs(x - self.est_strike_ce)))
                    # print(self.upper_list_ce['strike'].values)
                    # general_logger.info('estimated strike CE ' + str(self.cum_table.loc[self.upper_list_ce.index.values, 'strike'].values[0]))
                    self.cum_table.loc[self.upper_list_ce.index.values, 'Buy_strike'] = 'Yes'  # CE
                    self.cum_table.loc[self.upper_list_ce.index.values, 'Tradable_stock'] = 'Yes'

                try:
                    self.grp_data_pe = self.cum_table.groupby(['Ref_stock', 'instrument_type', 'cap']).get_group((self.cum_table['Ref_stock'][self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0], 'PE',cap_info))
                    # self.grp_data_pe = self.grp_data_pe[(self.grp_data_pe['month_dist'] < 2) & (pd.to_datetime(self.grp_data_pe['expiry']) > (datetime.today() + timedelta(days=1)))]
                    self.grp_data_pe = self.grp_data_pe[pd.to_datetime(self.grp_data_pe['expiry']) > (datetime.today() + timedelta(days=1))]
                    self.grp_data_pe = self.grp_data_pe[self.grp_data_pe['exp_date_list'] > (self.month_cutoff)]
                    self.grp_data_pe = self.grp_data_pe[self.grp_data_pe['week_dist'] == self.grp_data_pe['week_dist'].min()]
                    if cap_info == 'weekely_options':
                        self.grp_data_pe = self.cum_table.groupby(['Ref_stock', 'instrument_type', 'cap']).get_group((self.cum_table['Ref_stock'][self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0], 'PE',cap_info))
                        # self.grp_data_pe = self.grp_data_pe[(self.grp_data_pe['month_dist'] < 2) & (pd.to_datetime(self.grp_data_pe['expiry']) > datetime.today() + timedelta(days=1))]
                        self.grp_data_pe = self.grp_data_pe[pd.to_datetime(self.grp_data_pe['expiry']) > (datetime.today() + timedelta(days=1))]
                        self.grp_data_pe = self.grp_data_pe[self.grp_data_pe['exp_date_list'] > (self.month_cutoff)]
                        self.grp_data_pe = self.grp_data_pe[self.grp_data_pe['week_dist'] == self.grp_data_pe['week_dist'].min()]
                except:

                    self.grp_data_pe = self.cum_table.groupby(['Ref_stock', 'instrument_type', 'cap']).get_group((self.cum_table['Ref_stock'][self.cum_table['Ref_stock_tkn'] == ref_tkn].values[0], 'PE',cap_info))
                    # self.grp_data_pe = self.grp_data_pe[(self.grp_data_pe['month_dist'] < 2) & (pd.to_datetime(self.grp_data_pe['expiry']) > datetime.today() + timedelta(days=1))]
                    self.grp_data_pe = self.grp_data_pe[pd.to_datetime(self.grp_data_pe['expiry']) > (datetime.today() + timedelta(days=1))]
                    self.grp_data_pe = self.grp_data_pe[self.grp_data_pe['exp_date_list'] > (self.month_cutoff)]
                    self.grp_data_pe = self.grp_data_pe[self.grp_data_pe['week_dist'] == self.grp_data_pe['week_dist'].min()]


                # if not tick_data['last_price'][tick_data['instrument_token'] == ref_tkn].ewm(com=2).mean().tail(1).empty and not self.grp_data_pe.empty:
                if not self.grp_data_pe.empty and not self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_tkn].empty:
                #     self.est_strike_pe = int(tick_data['last_price'][tick_data['instrument_token'] == ref_tkn].ewm(com=2).mean().values[-1]) + self.cum_table[self.cum_table['Ref_stock_tkn'] == ref_tkn]['Strike_dist_PE'].values[0]
                    self.est_strike_pe = int((self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_tkn][['close', 'open']][1:2].mean(axis=1))) + self.cum_table[self.cum_table['Ref_stock_tkn'] == ref_tkn]['Strike_dist_PE'].values[0]
                    self.dist_calc_pe = self.grp_data_pe['strike'] / self.est_strike_pe
                    self.upper_list_pe = self.dist_calc_pe[self.dist_calc_pe < 1].nlargest(1)
                    # self.upper_list_pe = self.grp_data_pe[self.grp_data_pe['strike'] == min(self.grp_data_pe['strike'], key=lambda x: abs(x - self.est_strike_pe))]
                    # general_logger.info('estimated strike PE ' + str(self.cum_table.loc[self.upper_list_pe.index.values, 'strike'].values[0]))
                    self.cum_table.loc[self.upper_list_pe.index.values, 'Buy_strike'] = 'Yes'  # PE
                    self.cum_table.loc[self.upper_list_pe.index.values, 'Tradable_stock'] = 'Yes'

    # @timeit
    def derivative_analysis(self, all_table,tick_data):
        '''
        This function takes the list of stocks to buy and selects the strike price stock indices
        input is stock info table and output in derivative stock with features expiry date, CE/PE ,capital allocated,ATM/ITM/OTM info
        '''
        # for creating buy list
        self.ltp_price = 0  # last trade price.
        self.stop_loss_price = 0
        self.buy_stock_cap_CE = self.cum_table[(self.cum_table['Buy_strike'] == 'Yes') & (self.cum_table['instrument_type'] == 'CE') & (self.cum_table['buy_signal_CE'] >= 1) ]
        self.buy_stock_cap_PE = self.cum_table[(self.cum_table['Buy_strike'] == 'Yes') & (self.cum_table['instrument_type'] == 'PE') & (self.cum_table['buy_signal_PE'] >= 1) ]
        self.buy_stock_cap = pd.concat([self.buy_stock_cap_CE, self.buy_stock_cap_PE])
        self.sell_stock_table = pd.DataFrame([])
        # general_logger.info('before filter '+self.buy_stock_cap['tradingsymbol'].values)

        recent_data =tick_data[tick_data['instrument_token'].isin(self.buy_stock_cap['instrument_token'].values)]

        if not recent_data.empty:
            oi_filtered_tkns = recent_data[recent_data['oi']>0]['instrument_token'].unique()
            if len(oi_filtered_tkns)>0:
                oi_data = self.buy_stock_cap[self.buy_stock_cap['instrument_token'].isin(oi_filtered_tkns)]
                filtered_tkn = list(oi_data.groupby(['Ref_stock_tkn','instrument_type']).apply(lambda df: df[ (df.strike == df.strike.max()) & (df.instrument_type == 'PE')])['instrument_token'].values) +list(oi_data.groupby(['Ref_stock_tkn','instrument_type']).apply(lambda df: df[ (df.strike == df.strike.min()) & (df.instrument_type == 'CE')])['instrument_token'].values)
                self.buy_stock_cap = self.buy_stock_cap[self.buy_stock_cap['instrument_token'].isin(filtered_tkn)]

        for g in self.buy_stock_cap['instrument_token'].values:
            if self.session_end(self.cum_table['exchange'].loc[self.cum_table['instrument_token'] == g].values[0]) and datetime.today().weekday() == 4 :
                if self.buy_stock_cap['exp_date_list'][self.buy_stock_cap['instrument_token'] == g].values == 2:
                    self.buy_stock_cap = self.buy_stock_cap[self.buy_stock_cap['instrument_token'] !=g]
                    general_logger.info('removed' + self.cum_table['tradingsymbol'].loc[self.cum_table['instrument_token'] == g].values[0] +' from buy list')

        # general_logger.info('after filter ' + self.buy_stock_cap['tradingsymbol'].values)
        # add instruments from position
        if not self.open_positions.empty:
            for t in self.open_positions['instrument_token'].values:
                # self.stop_loss_val = StopLoss.get_data(token_no=t)
                self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token', keep='last')
                self.stop_loss_val = self.stop_loss_info['buy_price'][self.stop_loss_info['instrument_token'] == t].values
                if len(self.stop_loss_val) == 0:
                    self.stop_loss_val = -1
                # self.recent_data = TickStore.get_recent_tickstore(instrument_tokens=[t])  # last trade price.
                try:
                    self.recent_data = self.tick_data[self.tick_data['instrument_token'] == t].tail(1)['last_price'].values
                    if len(self.recent_data) > 0:
                        self.ltp_price = self.recent_data
                except:
                    self.ltp_price = 0
                #stop loss logic
                ref_stk_sym = None

                for s,_ in self.loss_table_info.columns:
                    if self.cum_table['Ref_stock'][self.cum_table['instrument_token'] == t].values[0][0:len(s)].startswith(s):
                        ref_stk_sym = s
                        # general_logger.info('detected ref_stk_sym '+ str(s)+' for '+self.cum_table['tradingsymbol'][self.cum_table['instrument_token'] == t].values[0])
                        break
                if ref_stk_sym == None:
                    self.stop_loss_price = False
                    der_correction = 0
                    trade_logger.info('no ref_stk_sym detected for '+ self.cum_table['tradingsymbol'][self.cum_table['instrument_token'] == t].values[0])
                else:
                    # nearest_idx = np.abs(self.loss_table_info['price'] - self.stop_loss_val).argsort()[0]
                    nearest_idx = np.abs(self.loss_table_info[(ref_stk_sym,'price')] - self.cum_table['current_value'][self.cum_table['instrument_token'] == t].values[0]).argsort()[0]
                    nearest_der_idx = np.abs(self.der_loss_table_info[(ref_stk_sym, 'der_price')] - self.stop_loss_val).argsort()[0]
                    der_correction = self.der_loss_table_info[(ref_stk_sym,self.cum_table['cap'][self.cum_table['instrument_token'] == t].values[0])].iloc[nearest_der_idx]
                # ref_stk_sym = difflib.get_close_matches(self.cum_table['Ref_stock'][self.cum_table['instrument_token'] == t].values[0], list(self.loss_table.columns), n=1)
                if self.stop_loss_val == -1:
                    self.stop_loss_price = False
                elif 0 < self.ltp_price and ref_stk_sym != None:
                    # self.stop_loss_price = (self.stop_loss_val - self.ltp_price) >=  (self.loss_table_info[ref_stk_sym].iloc[nearest_idx] * 1)
                    self.stop_loss_price = (self.stop_loss_val - self.ltp_price) >= (self.loss_table_info[(ref_stk_sym,self.cum_table['cap'][self.cum_table['instrument_token'] == t].values[0])].iloc[nearest_idx] - der_correction)
                    # if self.half_time(self.cum_table['exchange'][self.cum_table['instrument_token'] == t].values[0]):
                    #     self.stop_loss_price = (self.stop_loss_val - self.ltp_price) >= 2*(self.loss_table_info[(ref_stk_sym,self.cum_table['cap'][self.cum_table['instrument_token'] == t].values[0])].iloc[nearest_idx] - der_correction)


                if (self.open_positions['quantity'][self.open_positions['instrument_token'] == t].values > 0) or (self.open_positions['overnight_quantity'][self.open_positions['instrument_token'] == t].values > 0) or DEBUG:  # check if sold or not
                    general_logger.info(self.cum_table[self.cum_table['instrument_token'] == t]['Ref_stock'].values[0])
                    corrs_ref_tkn = self.cum_table[self.cum_table['instrument_token'] == t]['Ref_stock_tkn'].values
                    if corrs_ref_tkn.size != 0 and not all_table.empty:
                        corrs_ref_tkn = corrs_ref_tkn[0]
                        if corrs_ref_tkn in all_table['instrument_token'].values:
                            self.pos_tkn_frame = self.cum_table.loc[self.cum_table['instrument_token'] == t]
                            if all_table[all_table['instrument_token'] == corrs_ref_tkn]['CE_jump'].values == -1:
                                if self.pos_tkn_frame['instrument_type'].values == 'CE':
                                    self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_CE=np.repeat(all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_CE'].values, len(self.pos_tkn_frame)))
                                    self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])

                            if all_table[all_table['instrument_token'] == corrs_ref_tkn]['PE_jump'].values == -1:
                                if self.pos_tkn_frame['instrument_type'].values == 'PE':
                                    self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_PE=np.repeat(all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_PE'].values, len(self.pos_tkn_frame)))
                                    self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])

                            if all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_CE'].values == -2 or all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_CE'].values == -1:
                                if self.pos_tkn_frame['instrument_type'].values == 'CE':
                                    self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_CE=np.repeat(all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_CE'].values, len(self.pos_tkn_frame)))
                                    self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])

                            if all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_PE'].values == -2 or all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_PE'].values == -1 :
                                if self.pos_tkn_frame['instrument_type'].values == 'PE':
                                    self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_PE=np.repeat(all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_PE'].values, len(self.pos_tkn_frame)))
                                    self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])

                            if all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_CE'].values == -5 or all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_PE'].values == -5:
                                if self.pos_tkn_frame['instrument_type'].values == 'PE' or self.pos_tkn_frame['instrument_type'].values == 'CE':
                                    self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_CE=np.repeat(all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_CE'].values, len(self.pos_tkn_frame)))
                                    self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_PE=np.repeat(all_table[all_table['instrument_token'] == corrs_ref_tkn]['buy_signal_PE'].values, len(self.pos_tkn_frame)))
                                    self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])

                    if self.stop_loss_price  and not self.session_end(exchg = self.tkn_to_exchg(t)) and self.exchg_time_sell_chk(exchg = self.tkn_to_exchg(t)):
                        self.stop_loss_counter['stop_loss_counter'].loc[self.stop_loss_counter['instrument_token'] == t] = self.stop_loss_counter['stop_loss_counter'].loc[self.stop_loss_counter['instrument_token'] == t] + 1  # stop loss counter
                        if int(self.stop_loss_counter['stop_loss_counter'].loc[self.stop_loss_counter['instrument_token'] == t]) >= self.stoploss_threshold:
                            if  not self.day_cdl_all.empty:# do not remove this chk
                                # if self.day_cdl_all[self.day_cdl_all['instrument_token'] == corrs_ref_tkn]['close'].values[0] < self.day_cdl_all[self.day_cdl_all['instrument_token'] == corrs_ref_tkn]['open'].values[0]:
                                self.pos_tkn_frame = self.cum_table.loc[self.cum_table['instrument_token'] == t].assign(buy_signal_CE=-6)
                                self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_PE=-6)
                                self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])
                                trade_logger.info('%s / %s ' % (self.tkn_to_symbol([t])[0], str('stop loss hit at '+ str(self.ltp_price))))

                    if t in self.cum_table['instrument_token'].values:
                        if self.cum_table[self.cum_table['instrument_token'] == t]['exp_date_list'].values == self.month_cutoff:
                            if self.expiry_sell_time(self.cum_table[self.cum_table['instrument_token'] == t]['exchange'].values[0]):# checking expiry of nfo stocks and sell them
                                self.pos_tkn_frame = self.cum_table.loc[self.cum_table['instrument_token'] == t].assign(buy_signal_CE=-4)  # expiry detection
                                self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_PE=-4)
                                self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])
                                trade_logger.info('%s / %s ' % (self.tkn_to_symbol([t])[0],str(' expiry detected ')))

                        # if self.session_end(self.tkn_to_exchg(t)):
                        #     if not self.order_status.empty:
                        #         if (not t in self.order_status['instrument_token'].values) and (t in self.open_positions['instrument_token'].values) :
                        #             if self.cum_table[self.cum_table['instrument_token'] == t]['instrument_type'].values == 'PE' :
                        #                 self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_PE=-1)
                        #             if self.cum_table[self.cum_table['instrument_token'] == t]['instrument_type'].values == 'CE' :
                        #                 self.pos_tkn_frame = self.pos_tkn_frame.assign(buy_signal_CE=-1)
                        #             self.sell_stock_table = pd.concat([self.sell_stock_table, self.pos_tkn_frame])

        if not self.buy_stock_cap.empty:
            # self.buy_stock_cap = self.buy_stock_cap[(self.buy_stock_cap['buy_signal_PE'] >= 1) | (self.buy_stock_cap['buy_signal_CE'] >= 1)]
            # self.buy_stock_cap = self.buy_stock_cap.drop_duplicates(subset=['instrument_token'])
            self.buy_stock_cap = self.buy_stock_cap[self.buy_stock_cap['exp_date_list'] > (self.month_cutoff)]  # do not buy one day before expiry date
            # exp_ref = self.buy_stock_cap[self.buy_stock_cap['exp_date_list'] == self.month_cutoff]['Ref_stock'].unique()
            # if len(exp_ref) >0:
            #     self.buy_stock_cap.drop(self.buy_stock_cap[self.buy_stock_cap['Ref_stock'] == exp_ref[0]].index, inplace=True)
        if not self.sell_stock_table.empty:
            self.sell_stock_table = self.sell_stock_table[(self.sell_stock_table['buy_signal_PE'] <= -1) | (self.sell_stock_table['buy_signal_CE'] <= -1) |(self.sell_stock_table['PE_jump'] <= -1) | (self.sell_stock_table['CE_jump'] <= -1)]
            # self.sell_stock_table = self.sell_stock_table.drop_duplicates(subset=['instrument_token'])

        # if not (self.buy_stock_cap.empty or self.sell_stock_table.empty):
        #     self.buy_stock_cap.drop(self.buy_stock_cap.index[self.buy_stock_cap['instrument_token'].isin(self.sell_stock_table['instrument_token'])], inplace=True)

        # self.buy_stock_cap.reset_index(drop=True, inplace=True)
        # self.sell_stock_table.reset_index(drop=True, inplace=True)
        general_logger.info('derivative analysis done ')
        return self.buy_stock_cap, self.sell_stock_table

    def jump(self, buy_list, sell_list, tick_data):
        '''
        get buy_list , get CE stocks and compare its strike price with hedge points
        '''
        strike_diff_pe = 0
        strike_diff_ce = 0
        # tick_data = tick_data[tick_data['date_time'] >= (tick_data['date_time'].max() - pd.Timedelta(minutes=1))]  # remove old data
        if not self.open_positions.empty and (not buy_list.empty or not sell_list.empty) :
            open_pos_data = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]
            # symbol_list = pd.concat([self.open_positions['tradingsymbol'], buy_list['tradingsymbol']], ignore_index=True)
            for k in pd.concat([self.open_positions['tradingsymbol'],buy_list['tradingsymbol']]).unique():
                if k in self.open_positions['tradingsymbol'].values:
                    striked_value = -1
                    ref_stock_tkn = self.cum_table['Ref_stock_tkn'][self.cum_table['tradingsymbol'] == k].values[0]
                    ref_inst_type = self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == k].values[0]
                    # est_strike = int(tick_data['last_price'][tick_data['instrument_token'] == ref_stock_tkn].ewm(com=2).mean().values[-1])
                    # open_pos_lst = self.open_positions[(self.open_positions['quantity'] > 0)]['tradingsymbol'].values
                    # striked_value = np.max(StrikeEntry.get_data(ref_symbol=str(self.cum_table['Ref_stock'][self.cum_table['tradingsymbol'] == k].values[0])))
                    try:
                        striked_value = self.strike_entry_info['strike_value'][self.strike_entry_info['instrument_token']==self.symbol_to_tkn(k)].values[0]
                    except:
                        striked_value = -1
                    # if not open_pos_data['instrument_token'].loc[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].empty:
                    #     # striked_value = StrikeEntry.get_data(token_no=open_pos_data['instrument_token'].loc[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)])
                    #     striked_value =  StrikeEntry.get_data(token_no=self.symbol_to_tkn(k))
                    # else:
                    #     striked_value = -1
                    open_pos_ref_tick_data = tick_data[tick_data['instrument_token'] == ref_stock_tkn]
                    if striked_value != -1:
                        # if len(open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)]) == 1:  # only one position expected
                        if all(open_pos_data['exchange'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values == 'CDS'):
                            if not open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].empty:
                                striked_value = striked_value * open_pos_data['tick_size'][open_pos_data['Ref_stock_tkn'] == ref_stock_tkn].values  # do not use abs value
                        if ref_inst_type == 'PE':
                            # open_pos_ref_tick_data['diff_ref'] = open_pos_ref_tick_data['last_price'] % open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values <= 0.05
                            # strike_diff = ((striked_value - open_pos_ref_tick_data['last_price'].values[-1]) > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0]).any() or ((open_pos_ref_tick_data['last_price'].values[-1] - striked_value) > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0] * 3).any()
                            # strike_diff = ((striked_value - open_pos_ref_tick_data['last_price'].values[-1]) > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0] *1).any()
                            # strike_diff = (striked_value - open_pos_ref_tick_data['last_price'].values[-1])
                            strike_diff_pe = (striked_value -(self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_stock_tkn][['close', 'open']][1:2].mean(axis=1).mean(axis=0)))
                            inst_analysis_logger.info('%s / %s / %s / %s / %s' % ('PE_Jump:', k, strike_diff_pe,striked_value,self.cum_table['PE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values))
                        else:
                            # open_pos_ref_tick_data['diff_ref'] = open_pos_ref_tick_data['last_price'] % open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values <= 0.05
                            # strike_diff = ((open_pos_ref_tick_data['last_price'].values[-1] - striked_value) > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0] ).any() or ((striked_value - open_pos_ref_tick_data['last_price'].values[-1]) > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0]* 3).any()
                            # strike_diff = ((open_pos_ref_tick_data['last_price'].values[-1] - striked_value) > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0] *1).any()
                            # strike_diff = (open_pos_ref_tick_data['last_price'].values[-1] - striked_value)
                            strike_diff_ce = ((self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_stock_tkn][['close', 'open']][1:2].mean(axis=1).mean(axis=0)) - striked_value)
                            inst_analysis_logger.info('%s / %s / %s / %s / %s' % ('CE_Jump:',k, strike_diff_ce,striked_value, self.cum_table['CE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values))

                        # if self.session_start(self.cum_table['exchange'].loc[self.cum_table['instrument_token'] ==self.symbol_to_tkn(k)].values):
                        #     strike_diff_ce = (striked_value - (self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_stock_tkn][['close', 'open']][1:2].mean(axis=1).mean(axis=0)))
                            # strike_diff_pe = ((self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_stock_tkn][['close', 'open']][1:2].mean(axis=1).mean(axis=0)) - striked_value)
                            # if ((strike_diff_ce > (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0]* 0.5 )) and (self.cum_table['CE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values <= -1)) and open_pos_data['Hedge_points_CE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0 and ref_inst_type == 'CE':
                            #         open_ce = open_pos_data[(open_pos_data['tradingsymbol'] == k)]
                            #         open_ce = open_ce.assign(buy_signal_CE=-1)
                            #         open_ce = open_ce.assign(buy_signal_PE=0)
                            #         sell_list = pd.concat([sell_list, open_ce], ignore_index=True)
                            #         general_logger.info(k + ' sell initiated at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])
                            # elif ((strike_diff_pe > (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0] *0.5 )) and (self.cum_table['PE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values <= -1)) and ref_inst_type == 'PE' and open_pos_data['Hedge_points_PE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0:
                            #         open_pe = open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)]
                                    # open_pe = open_pos_data[(open_pos_data['tradingsymbol'] == k)]
                                    # open_pe = open_pe.assign(buy_signal_PE=-1)
                                    # open_pe = open_pe.assign(buy_signal_CE=0)
                                    # sell_list = pd.concat([sell_list, open_pe], ignore_index=True)
                                    # general_logger.info(k + ' sell initiated at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])

                        # if self.session_reset(self.cum_table['exchange'].loc[self.cum_table['instrument_token'] ==self.symbol_to_tkn(k)].values):
                        #     now = timezone.make_naive(timezone.now())
                        #     if (now - pd.to_datetime(self.strike_entry_info[self.strike_entry_info['tradingsymbol'] == k]['date_time'].values)) >= np.timedelta64(30, 'h'):
                        #         if not ((strike_diff_ce > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0] * 1).any()) and ref_inst_type == 'CE' and open_pos_data['Hedge_points_CE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0:
                                # if ref_inst_type == 'CE':
                                #     open_ce = open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)]
                                    # open_ce = open_pos_data[open_pos_data['tradingsymbol'] == k]  # sell only specific strike
                                    # self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] = self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] + 1
                                    # if int(self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)]) >= self.hedge_threshold:
                                    #
                                    #     open_ce = open_ce.assign(buy_signal_CE=-1)
                                    #     open_ce = open_ce.assign(buy_signal_PE=0)
                                    #     sell_list = pd.concat([sell_list, open_ce], ignore_index=True)
                                    #     general_logger.info(k + ' sell initiated at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])
                                #
                                # elif not ((strike_diff_pe > self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0] * 1).any()) and ref_inst_type == 'PE' and open_pos_data['Hedge_points_PE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0:
                                # elif ref_inst_type == 'PE':
                                #     open_pe = open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)]
                                    # open_pe = open_pos_data[open_pos_data['tradingsymbol'] == k]  # sell specific strike
                                    # self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] = self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] + 1
                                    # if int(self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)]) >= self.hedge_threshold:
                                    #
                                    #     open_pe = open_pe.assign(buy_signal_PE=-1)
                                    #     open_pe = open_pe.assign(buy_signal_CE=0)
                                    #     sell_list = pd.concat([sell_list, open_pe], ignore_index=True)
                                    #     general_logger.info(k + ' sell initiated at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])

                        if not self.session_start(self.cum_table['exchange'].loc[self.cum_table['instrument_token'] ==self.symbol_to_tkn(k)].values) and  self.session_end(self.cum_table['exchange'].loc[self.cum_table['instrument_token'] ==self.symbol_to_tkn(k)].values):
                            if (strike_diff_ce > (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0] * 3.5 )) and ref_inst_type == 'CE' :
                                # open_ce = open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)]#sell all strike
                                open_ce =open_pos_data[open_pos_data['tradingsymbol'] == k] # sell only specific strike
                                self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] = self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] + 1
                                if int(self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)]) >= self.hedge_threshold :
                                    if self.cum_table['CE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values <= -1 :
                                        open_ce = open_ce.assign(buy_signal_CE=-1)
                                        open_ce = open_ce.assign(buy_signal_PE=0)
                                        sell_list = pd.concat([sell_list, open_ce], ignore_index=True)
                                        trade_logger.info(k + ' sell initiated at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])
                                        self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] = 0

                            # elif (strike_diff_ce > (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0] * 1)) and self.cum_table['buy_signal_CE'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values == 1 and ref_inst_type == 'CE'  and open_pos_data['Hedge_points_CE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0:
                            #     diff_value = open_pos_ref_tick_data.loc[open_pos_ref_tick_data['date_time'] == open_pos_ref_tick_data['date_time'].max()]['last_price'].values[0] - striked_value
                            #     buy_sig_val_ce, _ = np.divmod(diff_value, open_pos_data['Hedge_points_CE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0])
                            #     buy_sig_val_ce = min(abs(buy_sig_val_ce), 1)
                            #     buy_list.loc[(buy_list['Ref_stock_tkn'] == ref_stock_tkn) & (buy_list['instrument_type'] == ref_inst_type),'buy_signal_CE'] = buy_sig_val_ce
                            #     general_logger.info(k + ' hedging done at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])

                            if (strike_diff_pe > (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0] * 3.5 )) and ref_inst_type == 'PE' :
                                # open_pe = open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)]#sell all strike
                                open_pe = open_pos_data[open_pos_data['tradingsymbol'] == k]#sell specific strike
                                self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] = self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] + 1
                                if int(self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)]) >= self.hedge_threshold:
                                    if self.cum_table['PE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values <= -1 :
                                        open_pe = open_pe.assign(buy_signal_PE=-1)
                                        open_pe = open_pe.assign(buy_signal_CE=0)
                                        sell_list = pd.concat([sell_list, open_pe], ignore_index=True)
                                        trade_logger.info(k + ' sell initiated at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])
                                        self.hedge_counter['hedge_counter'].loc[self.hedge_counter['instrument_token'] == self.symbol_to_tkn(k)] = 0
                            # elif (strike_diff_pe > (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0] * 1)) and self.cum_table['buy_signal_PE'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values == 1 and ref_inst_type == 'PE' and open_pos_data['Hedge_points_PE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0:
                            #     diff_value = open_pos_ref_tick_data.loc[open_pos_ref_tick_data['date_time'] == open_pos_ref_tick_data['date_time'].max()]['last_price'].values[0] - striked_value
                            #     buy_sig_val_pe, _ = np.divmod(diff_value, open_pos_data['Hedge_points_PE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0])
                            #     buy_sig_val_pe = min(abs(buy_sig_val_pe), 1)
                            #     buy_list.loc[(buy_list['Ref_stock_tkn'] == ref_stock_tkn) & (buy_list['instrument_type'] == ref_inst_type),'buy_signal_PE'] = buy_sig_val_pe
                            #     general_logger.info(k + ' buy initiated at ' '%s' % open_pos_ref_tick_data['last_price'].tail(1).values[0])

                if k in buy_list['tradingsymbol'].values and k not in self.open_positions['tradingsymbol'].values:
                    ref_stock_tkn = self.cum_table['Ref_stock_tkn'][self.cum_table['tradingsymbol'] == k].values[0]
                    ref_inst_type = self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == k].values[0]
                    possible_strike = open_pos_data[(open_pos_data['Ref_stock_tkn']==ref_stock_tkn) &(open_pos_data['instrument_type']==ref_inst_type)]
                    if not possible_strike.empty:
                        if ref_inst_type == 'CE':
                            possible_strike = possible_strike[possible_strike['strike']==possible_strike['strike'].max()].head(1)
                        if ref_inst_type == 'PE':
                            possible_strike = possible_strike[possible_strike['strike'] == possible_strike['strike'].min()].head(1)
                        try:
                            striked_value = self.strike_entry_info['strike_value'][self.strike_entry_info['instrument_token'] == possible_strike['instrument_token'].values[0]].values[0]
                        except:
                            striked_value = -1

                        if striked_value != -1:

                            if all(self.cum_table['exchange'][(self.cum_table['Ref_stock_tkn'] == ref_stock_tkn) & (self.cum_table['instrument_type'] == ref_inst_type)].values == 'CDS'):
                                if not self.cum_table[(self.cum_table['Ref_stock_tkn'] == ref_stock_tkn) & (self.cum_table['instrument_type'] == ref_inst_type)].empty:
                                    striked_value = striked_value * self.cum_table['tick_size'][self.cum_table['Ref_stock_tkn'] == ref_stock_tkn].values  # do not use abs value

                            if ref_inst_type == 'CE':
                                strike_diff_ce = ((self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_stock_tkn][['close', 'open']][1:2].mean(axis=1).mean(axis=0)) - striked_value)
                                inst_analysis_logger.info('%s / %s / %s / %s / %s' % ('CE_Jump:', k, strike_diff_ce,striked_value,self.cum_table['CE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values))
                                if (strike_diff_ce > (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_CE'].values[0] * 1)) and self.cum_table['buy_signal_CE'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values == 1 and ref_inst_type == 'CE' and open_pos_data['Hedge_points_CE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0:
                                    # diff_value = open_pos_ref_tick_data.loc[open_pos_ref_tick_data['date_time'] == open_pos_ref_tick_data['date_time'].max()]['last_price'].values[0] - striked_value
                                    # buy_sig_val_ce, _ = np.divmod(diff_value, open_pos_data['Hedge_points_CE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0])
                                    buy_sig_val_ce = 1
                                    buy_list.loc[(buy_list['Ref_stock_tkn'] == ref_stock_tkn) & (buy_list['instrument_type'] == ref_inst_type), 'buy_signal_CE'] = buy_sig_val_ce
                                    general_logger.info(k + ' buy initiated' )
                                else:
                                    buy_sig_val_ce = 0
                                    buy_list.loc[(buy_list['Ref_stock_tkn'] == ref_stock_tkn) & (buy_list['instrument_type'] == ref_inst_type), 'buy_signal_CE'] = buy_sig_val_ce
                            if ref_inst_type == 'PE':
                                strike_diff_pe = (striked_value - (self.fwd_3_all[self.fwd_3_all['instrument_token'] == ref_stock_tkn][['close', 'open']][1:2].mean(axis=1).mean(axis=0)))
                                inst_analysis_logger.info('%s / %s / %s / %s / %s' % ('PE_Jump:', k, strike_diff_pe,striked_value,self.cum_table['PE_jump'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values))
                                if (strike_diff_pe >  (self.cum_table[self.cum_table['tradingsymbol'] == k]['Hedge_points_PE'].values[0] * 1)) and self.cum_table['buy_signal_PE'].loc[self.cum_table['instrument_token'] == self.symbol_to_tkn(k)].values == 1 and ref_inst_type == 'PE' and open_pos_data['Hedge_points_PE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] > 0:
                                    # diff_value = open_pos_ref_tick_data.loc[open_pos_ref_tick_data['date_time'] == open_pos_ref_tick_data['date_time'].max()]['last_price'].values[0] - striked_value
                                    # buy_sig_val_pe, _ = np.divmod(diff_value, open_pos_data['Hedge_points_PE'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0])
                                    buy_sig_val_pe = 1
                                    buy_list.loc[(buy_list['Ref_stock_tkn'] == ref_stock_tkn) & (buy_list['instrument_type'] == ref_inst_type), 'buy_signal_PE'] = buy_sig_val_pe
                                    general_logger.info(k + ' buy initiated')
                                else:
                                    buy_sig_val_pe = 0
                                    buy_list.loc[(buy_list['Ref_stock_tkn'] == ref_stock_tkn) & (buy_list['instrument_type'] == ref_inst_type), 'buy_signal_PE'] = buy_sig_val_pe

        if not buy_list.empty and not sell_list.empty:
            buy_list.drop(buy_list.index[buy_list['instrument_token'].isin(sell_list['instrument_token'])],inplace=True)
        if not buy_list.empty:
            buy_list = buy_list[(buy_list['buy_signal_PE'] >= 1) | (buy_list['buy_signal_CE'] >= 1)]
            # buy_list = buy_list.drop_duplicates(subset=['instrument_token'])
            buy_list.reset_index(drop=True, inplace=True)
        if not sell_list.empty:
            sell_list = sell_list[(sell_list['buy_signal_PE'] <= -1) | (sell_list['buy_signal_CE'] <= -1)]
            # sell_list = sell_list.drop_duplicates(subset=['instrument_token'])
            sell_list.reset_index(drop=True, inplace=True)
        general_logger.info('jump done ')
        return buy_list, sell_list

    # @timeit
    def slu(self):
        '''
        perform stoploss update based on strike entry and ref index point rise
        '''
        # tick_data = tick_data[tick_data['date_time'] >= (tick_data['date_time'].max() - pd.Timedelta(minutes=1))]  # remove old data
        if not self.order_status.empty and  not self.stop_loss_info.empty and not self.open_positions.empty:
            for n in self.cum_table['Ref_stock_tkn'].unique():
                recent_ce_sell = self.order_status.loc[(self.order_status['Ref_stock']== self.cum_table['tradingsymbol'][self.cum_table['instrument_token']== n].values[0]) & (self.order_status['status']== 'COMPLETE') & (self.order_status['transaction_type']== 'SELL') & (self.order_status['instrument_type']== 'CE')]['instrument_token'].values
                recent_pe_sell = self.order_status.loc[(self.order_status['Ref_stock']== self.cum_table['tradingsymbol'][self.cum_table['instrument_token']== n].values[0]) & (self.order_status['status']== 'COMPLETE') & (self.order_status['transaction_type']== 'SELL') & (self.order_status['instrument_type'] == 'PE')]['instrument_token'].values
                if len(recent_ce_sell) > 0:
                    for nn in recent_ce_sell:
                        if nn not in self.open_positions['instrument_token'].values and nn in self.stop_loss_info['instrument_token'].values:
                            self.stop_loss_info = self.stop_loss_info[self.stop_loss_info['instrument_token'] != nn]
                if len(recent_pe_sell) > 0:
                    for nk in recent_pe_sell:
                        if nk not in self.open_positions['instrument_token'].values and nk in self.stop_loss_info['instrument_token'].values:
                            self.stop_loss_info = self.stop_loss_info[self.stop_loss_info['instrument_token'] != nk]
            drp_tkn = np.setdiff1d(self.stop_loss_info['instrument_token'].values,np.r_[self.order_status['instrument_token'].values,self.open_positions['instrument_token'].values])
            if len(drp_tkn)>0:
                self.stop_loss_info = self.stop_loss_info.drop(self.stop_loss_info[self.stop_loss_info['instrument_token'].isin(drp_tkn)].index)
                self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token', keep='last')
                # for tkn in drp_tkn:
                #     self.stop_loss_info = self.stop_loss_info[self.stop_loss_info['instrument_token'] != tkn]
        if self.order_status.empty and not self.stop_loss_info.empty and not self.open_positions.empty:
            # rem_token_ord = list(set(self.stop_loss_info['instrument_token']).symmetric_difference(self.order_status['instrument_token'].values))
            rem_tkn_open_pos = list(set(self.stop_loss_info['instrument_token']).symmetric_difference(self.open_positions['instrument_token'].values))
            if len(rem_tkn_open_pos) >0 :
                self.stop_loss_info = self.stop_loss_info.drop(self.stop_loss_info[self.stop_loss_info['instrument_token'].isin(rem_tkn_open_pos)].index)
                self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token',keep='last')
        # elif self.open_positions.empty and not self.stop_loss_info.empty and not self.tick_data.empty:
        #     self.stop_loss_info = self.stop_loss_info[0:0]
        #     trade_logger.info(' stoploss table made empty ')

            # traded_symbol_list = np.unique(self.order_status['tradingsymbol'])
            # for e in traded_symbol_list:
            #     all_entries = self.order_status.loc[(self.order_status['tradingsymbol'] == e) & self.order_status['status'].str.contains('COMPLETE')]
            #     if not all_entries.empty:
            #         recent_status = all_entries.iloc[all_entries['exchange_timestamp'].argmax()]['transaction_type']
            #         order_success = all_entries.iloc[all_entries['exchange_timestamp'].argmax()]['status']
            #         # if recent_status == 'SELL' and order_success == 'COMPLETE':
            #         if not e in self.open_positions['tradingsymbol']:
            #             # if len(self.stop_loss_info['buy_price'][self.stop_loss_info['tradingsymbol'] == self.symbol_to_tkn(e)]) >=1:
            #             self.stop_loss_info.drop(self.stop_loss_info[self.stop_loss_info['tradingsymbol']==self.symbol_to_tkn(e)].index)
            #                 # StopLoss.delete_data(token_no=self.symbol_to_tkn(e))

        if not self.open_positions.empty :
            for k in self.open_positions['tradingsymbol']:
                if not self.session_start(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == k].values[0]):
                    # open_pos_lst = self.open_positions[(self.open_positions['quantity'] > 0)]['tradingsymbol'].values
                    # try:
                    #     striked_value = self.strike_entry_info['strike_value'][self.strike_entry_info['instrument_token']==self.symbol_to_tkn(k)].values[0]
                    # except:
                    #     striked_value = -1
                    # open_pos_data = self.cum_table[self.cum_table.tradingsymbol.isin(self.open_positions['tradingsymbol'].values)]
                    # ref_stock_tkn = self.cum_table['Ref_stock_tkn'][self.cum_table['tradingsymbol'] == k].values[0]
                    # ref_inst_type = self.cum_table['instrument_type'][self.cum_table['tradingsymbol'] == k].values[0]
                    # tkn_tick_data = tick_data[tick_data['instrument_token']==self.symbol_to_tkn(k)]
                    # open_pos_ref_tick_data = tick_data[tick_data['instrument_token'] == ref_stock_tkn]
                    # prev_stoploss = StopLoss.get_data(token_no=self.open_positions['instrument_token'][self.open_positions['tradingsymbol'] == k].values)
                    try:
                        self.prev_stoploss = self.stop_loss_info['buy_price'][self.stop_loss_info['tradingsymbol'] == k].values[0]
                    except:
                        self.prev_stoploss = -1
                    #if len(open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)]) == 1 and ref_inst_type == 'PE':  # only one position expected
                    # if  ref_inst_type == 'PE':  # only one position expected
                    #     if striked_value != -1:
                    #         if not open_pos_data[(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].empty:
                    #             if all(open_pos_data['exchange'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values == 'CDS'):
                    #                 striked_value = striked_value * open_pos_data['tick_size'][open_pos_data['Ref_stock_tkn'] == ref_stock_tkn].values  # do not use abs value
                    #             if ref_inst_type == 'PE':
                    #                 # open_pos_ref_tick_data['diff_ref'] = open_pos_ref_tick_data['last_price'] % open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values <= 0.05
                    #                 open_pos_ref_tick_data['diff_strike_entry'] = abs((open_pos_ref_tick_data['last_price'] - striked_value)) % (open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] * 1) <= 0.05
                    #             else:
                    #                 # open_pos_ref_tick_data['diff_ref'] = open_pos_ref_tick_data['last_price'] % open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values <= 0.05
                    #                 open_pos_ref_tick_data['diff_strike_entry'] = abs((open_pos_ref_tick_data['last_price'] - striked_value)) % open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == ref_inst_type)].values[0] <= 0.05
                    #             # time_stp = pd.to_datetime(open_pos_ref_tick_data['date_time'][(open_pos_ref_tick_data['diff_ref'] == True) | (open_pos_ref_tick_data['diff_strike_entry'] == True)].max())
                    #             time_stp = pd.to_datetime(open_pos_ref_tick_data['date_time'][open_pos_ref_tick_data['diff_strike_entry'] == True].max())
                    #             try:
                    #                 der_value = pd.DataFrame(TickStore.get_with_time(days=1, fields=['instrument_token','last_price','date_time'],instrument_tokens=[self.symbol_to_tkn(k)]))
                    #                 curr_close_price = der_value['last_price'][(der_value['date_time'] < (time_stp + pd.Timedelta(seconds=100))) & (der_value['date_time'] > (time_stp - pd.Timedelta(seconds=100)))].max()
                    #             except:
                    #                 curr_close_price = -1
                    #
                    #             if not np.isnan(curr_close_price):
                    #                 # if (open_pos_ref_tick_data['last_price'][-1] - striked_value) < (open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == 'CE') & (open_pos_data['instrument_type'] == 'CE')].values[0] * 0.5):
                    #                 #     curr_close_price = curr_close_price * 0.6
                    #                 # else:
                    #                 #     curr_close_price = curr_close_price
                    #                 if curr_close_price != -1 and prev_stoploss != -1:
                    #                     if prev_stoploss < curr_close_price and ref_inst_type == 'PE':
                    #                         StopLoss.create(order_id=-1, buy_price=curr_close_price,token_no=self.open_positions['instrument_token'][self.open_positions['tradingsymbol'] == k].values,tradingsymbol=k)  # enclosed in try since price may not be available sometimes
                    #                         trade_logger.info(' sl update attempted for ' + k + ' from ' + str(prev_stoploss) + ' to ' + str(curr_close_price))
                    # if self.prev_stoploss == -1:
                    if not self.order_status.empty:
                        all_order_status = self.order_status.loc[(self.order_status['tradingsymbol'] == k) & (self.order_status['transaction_type'] == 'BUY') & (self.order_status['status'] == 'COMPLETE' )]
                        if not all_order_status.empty:
                            recent_order_status = all_order_status.tail(1)
                            if not recent_order_status.empty :
                                # if (open_pos_ref_tick_data['last_price'].tail(1).values - striked_value) < (open_pos_data['Sl_update_points'][(open_pos_data['Ref_stock_tkn'] == ref_stock_tkn) & (open_pos_data['instrument_type'] == 'PE')].values * 1):
                                #     updated_price = recent_order_status['average_price'] * 1
                                # else:
                                #     updated_price = recent_order_status['average_price']
                                updated_price = recent_order_status['average_price'].values
                                # StopLoss.create(order_id=recent_order_status['order_id'],buy_price=updated_price,token_no=recent_order_status['instrument_token'],tradingsymbol=recent_order_status['tradingsymbol'].values)
                                if self.prev_stoploss == -1 or updated_price != self.prev_stoploss :
                                    self.stop_loss_info = pd.concat([self.stop_loss_info,
                                                                     pd.DataFrame.from_dict({
                                                                         'order_id':recent_order_status['order_id'].values[0],'buy_price':updated_price,
                                                                         'instrument_token':recent_order_status['instrument_token'].values[0],
                                                                         'date_time': recent_order_status['exchange_timestamp'].values[0],
                                                                         'tradingsymbol':recent_order_status['tradingsymbol'].values}
                                                                     )],ignore_index=True)
                                    self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token',keep = 'last')
                                    general_logger.info(' sl created for ' + k + ' from ' + str(self.prev_stoploss) + ' to ' + str(updated_price))
                #update stoploss during halftime
                if self.sl_update_time(self.cum_table['exchange'][self.cum_table['tradingsymbol'] == k].values[0]) and self.cum_table['exchange'][self.cum_table['tradingsymbol'] == k].values[0] == 'NFO' :
                    update_sl = True
                    #logic toupdate stoploss for older purchase
                    if not self.order_status.empty:
                        all_order_status = self.order_status.loc[(self.order_status['tradingsymbol'] == k) & (self.order_status['transaction_type'] == 'BUY') & (self.order_status['status'] == 'COMPLETE' )]
                        if not all_order_status.empty:
                            recent_order_status = all_order_status.tail(1)
                            update_sl = not recent_order_status['order_timestamp'].dt.date.values == datetime.now().date()
                        else:
                            update_sl = True

                    if update_sl or self.order_status.empty:
                        ref_sym = self.cum_table['instrument_token'][self.cum_table['tradingsymbol'] == k].values[0]
                        nearest_price = self.tick_data[self.tick_data['instrument_token']==ref_sym ].tail(1)['last_price'].values[0]
                        if (nearest_price > self.stop_loss_info.loc[self.stop_loss_info['tradingsymbol'] == k,'buy_price'].values[0] and not DEBUG) or self.stop_loss_info.loc[self.stop_loss_info['tradingsymbol'] == k,'buy_price'].empty:
                            self.stop_loss_info.loc[self.stop_loss_info['tradingsymbol'] == k,'buy_price'] = nearest_price
                            self.stop_loss_info.loc[self.stop_loss_info['tradingsymbol'] == k, 'date_time'] = timezone.make_naive(timezone.now())
                            self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token', keep='last')
                            general_logger.info(' sl update attempted for ' + k + ' from ' + str(self.prev_stoploss) + ' to ' + str(nearest_price))

        general_logger.info('slu done ')

    def order_pending_chk(self):
        '''This function deletes orders based on time and condition especially on MCX exchange'''
        now = timezone.make_naive(timezone.now())
        if self.session_start(exchg = 'MCX') :
            delta_time = 60
        elif self.session_end(exchg = 'MCX'):
            delta_time = 60
        else:
            delta_time = 60

        if not self.order_status.empty:
            # del_order = self.order_status[((now - self.order_status['order_timestamp']) > np.timedelta64(delta_time, 's')) & (self.order_status['status'].str.contains('OPEN|OPEN PENDING|PARTIALLY FILLED'))]['order_id']
            del_order = self.order_status[((now - self.order_status['order_timestamp']) > np.timedelta64(delta_time, 's')) & (self.order_status['status'] =='OPEN')]['order_id']
            # if not del_order.empty:
            #     general_logger.info('order_status is :'+str(del_order['status'].values))
            for f in del_order:
            # for f in orders['order_id'].values:
                if self.order_status[self.order_status['order_id'] == f]['exchange'].str.contains('MCX').any():
                    try:
                        cancelled_order = self.client.cancel_ordr(variety = 'regular',order_id=f)
                        trade_logger.info('cancelled_order' '%s' % cancelled_order)
                    except Exception as e:
                        trade_logger.info("Order cancellation failed: {}".format(e))
                        continue
            self.order_status = self.order_status_update()
        general_logger.info('order_pending_chk done ')

    def clear_positions(self):
        now = timezone.make_naive(timezone.now())
        delta_time= 3600
        if not self.order_status.empty:
            del_order = self.order_status[((now - self.order_status['order_timestamp']) > np.timedelta64(delta_time, 's')) & (self.order_status['status'].str.contains('OPEN|OPEN PENDING|PARTIALLY FILLED'))]['order_id']
            for f in del_order:
            # for f in orders['order_id'].values:
                if self.order_status[self.order_status['order_id'] == f]['exchange'].str.contains('MCX').any():
                    try:
                        cancelled_order = self.client.cancel_ordr(variety = 'regular',order_id=f)
                        trade_logger.info('cancelled_order' '%s' % cancelled_order)
                    except Exception as e:
                        trade_logger.info("Order cancellation failed: {}".format(e))
                        continue
            self.order_status = self.order_status_update()
        general_logger.info('order_pending_chk done ')

    def strike_update(self):
        '''Update strike entry table after successful order'''
        # tick_data = tick_data[tick_data['date_time'] >= (tick_data['date_time'].max() - pd.Timedelta(seconds=60))]
        # tick_data['date_time'] = pd.to_datetime(tick_data['date_time'])
        # all_strike = pd.json_normalize(StrikeEntry.get_all())
        if not self.open_positions.empty :
            opn_pos_tkn = self.open_positions['instrument_token'].unique()
            if not self.strike_entry_info.empty:
                all_striked_tkn = self.strike_entry_info['instrument_token'].unique()
                if not self.open_positions.empty:
                    unwanted_tkn = list(set(all_striked_tkn).symmetric_difference(opn_pos_tkn))
                else:
                    unwanted_tkn = []
            else:
                unwanted_tkn = []
            if len(unwanted_tkn)>0 and not self.tick_data.empty:
                self.strike_entry_info = self.strike_entry_info[self.strike_entry_info.instrument_token.isin(opn_pos_tkn) == True]
            if  not self.order_status.empty :
                # [StrikeEntry.delete_data(x) for x in unwanted_tkn]
                general_logger.info('strike entry deleted for ' + str(unwanted_tkn))
                successful_orders = self.order_status[((self.order_status['status'].str.contains('COMPLETE') | self.order_status['status'].str.contains('PARTIAL')) & self.order_status['transaction_type'].str.contains('BUY'))]
                # opn_pos_tkn = self.open_positions['instrument_token'].values
                if not successful_orders.empty:
                    successful_orders = successful_orders.groupby('instrument_token').tail(1)
                    for s in successful_orders['order_id']:
                        if successful_orders[successful_orders['order_id'] == s]['instrument_token'].values[0] in opn_pos_tkn:
                            sym = self.cum_table['tradingsymbol'].loc[self.cum_table['tradingsymbol'] == successful_orders[successful_orders['order_id']==s]['tradingsymbol'].values[0]].values[0]
                            ref_sym = self.cum_table['Ref_stock_tkn'].loc[self.cum_table['tradingsymbol'] == successful_orders[successful_orders['order_id']==s]['tradingsymbol'].values[0]].values[0]
                            temp_tick_data = self.tick_data[self.tick_data['instrument_token']==ref_sym ]
                            temp_tick_data.reset_index(drop=False, inplace=True)
                            order_time = successful_orders[successful_orders['order_id']==s]['exchange_timestamp'].values[0]
                            try:
                                nearest_price = temp_tick_data[temp_tick_data['date_time'] >order_time].head(1)['last_price'].values[0]
                            except:
                                nearest_price = 0
                            # existing_entry =  StrikeEntry.get_data(token_no=successful_orders[successful_orders['order_id']==s]['instrument_token'].values[0])
                            try:
                                self.strike_entry_info = self.strike_entry_info[self.strike_entry_info.instrument_token.isin(unwanted_tkn) == False]
                                existing_entry = self.strike_entry_info['strike_value'][self.strike_entry_info['instrument_token'] == successful_orders[successful_orders['order_id']==s]['instrument_token'].values[0]].values[0]
                            except:
                                existing_entry = -1
                            if existing_entry == -1 and nearest_price != 0 :
                                self.strike_entry_info = pd.concat([self.strike_entry_info,pd.DataFrame([
                                    {'instrument_token':successful_orders[successful_orders['order_id']==s]['instrument_token'].values[0],
                                    'strike_value':nearest_price, 'tradingsymbol':sym,
                                     'date_time': order_time,
                                    'ref_symbol':ref_sym}])],ignore_index=True)
                                self.strike_entry_info = self.strike_entry_info.drop_duplicates(subset='instrument_token',keep='last')
                                self.strike_entry_info['Ref_stock'] = [str(self.cum_table[self.cum_table['instrument_token'] == x]['Ref_stock'].values[0]) for x in self.strike_entry_info['instrument_token'].values]
                                self.strike_entry_info['instrument_type'] = [str(self.cum_table[self.cum_table['instrument_token'] == x]['instrument_type'].values[0])for x in self.strike_entry_info['instrument_token'].values]

                                general_logger.info('buy strike_update done for ' + sym + ' at '+str(nearest_price))

                elif len(unwanted_tkn)>0:
                    try:
                        self.strike_entry_info = self.strike_entry_info[self.strike_entry_info.instrument_token.isin(unwanted_tkn) == False]
                    except Exception:
                        pass
            # to update strike detect based on day candle
            for e in opn_pos_tkn:
                    ref_sym = self.cum_table['Ref_stock_tkn'].loc[self.cum_table['instrument_token'] ==  e].values[0]
                    try:
                        nearest_price =self.day_cdl_all[self.day_cdl_all['instrument_token'] == ref_sym][['high','low']].values[0].mean()
                    except:
                        nearest_price = 0
                    if nearest_price != 0 and not self.strike_entry_info[self.strike_entry_info['instrument_token'] == e].empty and not self.session_end(self.cum_table['exchange'].loc[self.cum_table['instrument_token'] ==  e].values[0]):
                        if nearest_price > self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e,'strike_value'].values[0] and self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e,'instrument_type'].values[0] == 'CE':
                            self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e,'strike_value'] = nearest_price
                            self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e, 'date_time'] = timezone.make_naive(timezone.now())
                            self.strike_entry_info = self.strike_entry_info.drop_duplicates(subset='instrument_token', keep='last')
                        elif nearest_price < self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e,'strike_value'].values[0] and self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e,'instrument_type'].values[0] == 'PE':
                            self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e,'strike_value'] = nearest_price
                            self.strike_entry_info.loc[self.strike_entry_info['instrument_token'] == e, 'date_time'] = timezone.make_naive(timezone.now())
                            self.strike_entry_info = self.strike_entry_info.drop_duplicates(subset='instrument_token', keep='last')
        else:
            general_logger.info('open position empty detected')
        # 'This part removes the strike entries when open position is empty-- this feature not needed now'
        #     if not self.strike_entry_info.empty:
        #         all_striked_tkn = list(self.strike_entry_info['instrument_token'].unique())
                #[StrikeEntry.delete_data(x) for x in all_striked_tkn]#not needed
                # self.strike_entry_info = self.strike_entry_info[self.strike_entry_info.instrument_token.isin(all_striked_tkn) == False]
                # logger.info(' No strike_update done ')

    def update_order_list(self):
        '''buy and sell tables are updated based on order placed so we can place repeated order '''
        if not self.buy_stock_cap.empty:
            for r in self.buy_stock_cap['instrument_token']:
                if not self.order_status.empty:
                    if (self.order_status['instrument_token']==r).any():  # check if symbol is present
                        self.ordr_lst = self.order_status.groupby('instrument_token').get_group(r)  # to get all the latest entry
                        ordr_idx = self.ordr_lst.tail(1) #get latest entry
                        if ordr_idx['transaction_type'].values[0] == 'BUY' and ordr_idx['status'].values == 'COMPLETE':
                            self.buy_stock_cap.drop(self.buy_stock_cap[self.buy_stock_cap['instrument_token']==r].index,inplace=True)

        if not self.sell_stock_table.empty:
            for s in self.sell_stock_table['instrument_token']:
                if not self.order_status.empty:
                    if (self.order_status['instrument_token']==s).any():  # check if symbol is present
                        self.ordr_lst = self.order_status.groupby('instrument_token').get_group(s)  # to get all the  entry
                        ordr_idx = self.ordr_lst.tail(1) #get latest entry
                        if ordr_idx['transaction_type'].values[0] == 'SELL' and ordr_idx['status'].values == 'COMPLETE':
                            self.sell_stock_table.drop(self.sell_stock_table[self.sell_stock_table['instrument_token']==s].index,inplace=True)

    def update_processed_data_table(self):
        general_logger.info('ProcessedTick function entered')
        self.processed_data = pd.DataFrame([])
        self.recent_tick_data = pd.DataFrame([])
        # try:
        if not self.tick_data.empty:
            self.recent_tick_data = self.tick_data[self.tick_data['date_time'] >= (self.tick_data['date_time'].max() - pd.Timedelta(seconds=int(60)))]
            # if not self.recent_tick_data.empty:
                # self.recent_tick_data = self.recent_tick_data.set_index(['date_time'])
            # self.processed_data = self.recent_tick_data.groupby(['date_time','instrument_token']).head(1).set_index(['date_time']).groupby(['instrument_token']).resample('1s', origin='start_day', label='left',closed='left').bfill()
            self.processed_data = self.recent_tick_data.drop_duplicates(subset='date_time',keep='last')
            if not self.processed_data.empty:
                self.processed_data = self.processed_data.reset_index(drop=True).reset_index(level=0).dropna().drop(columns=['index']).to_dict(orient = 'records')
                tick_objects = []
                for tick in self.processed_data:
                    # Ensure the tick data is split and processed independently
                    # Serialize the tick
                    serialized_tick = orjson.dumps(tick, default=self.json_datetime_converter).decode("utf-8")
                    # Create a TickStore object for the serialized tick
                    tick_objects.append(
                        ProcessedTickStore(
                            broker=self.client.broker,
                            data=serialized_tick,
                            timestamp=timezone.now()  # Use current timestamp
                        )
                    )

                if len(tick_objects)>0 and not DEBUG:
                    # Bulk save tick objects to the database
                    ProcessedTickStore.objects.bulk_create(tick_objects, ignore_conflicts=True)
                    general_logger.info('ProcessedTickStore data inserted')
                else:
                    general_logger.info('tick object length is zero')
            else:
                general_logger.info('ProcessedTick data empty')
        else:
            general_logger.info('ProcessedTick data before processing empty')
        # delete_redis_keys(broker_name="zerodha") #remove redis data
        # except Exception as e:
        #     general_logger.info('%s' % e, exc_info=True)
    def prev_cdl_save(self):

        general_logger.info('prev_cdl_save')
        x= 'self.prev_day_cdl_all'
        #read from db
        try:
            cdl_data_json = flatten(AlgoInfo.get_table_data(
                broker=self.client.broker,
                tablename=x[5:]
            ))
            if not pd.read_json(orjson.loads(cdl_data_json[x[5:]])).empty:
                exec('{}=pd.read_json(orjson.loads(cdl_data_json[x[5:]]))'.format(x))
        except:
            self.prev_day_cdl_all = pd.DataFrame([])
        if not self.prev_day_cdl_all.empty:
            # Calculate yesterday's date
            yesterday = datetime.now().date() - timedelta(days=1)
            recent_cdls =  self.prev_day_cdl_all[self.prev_day_cdl_all['date_time'].dt.date > yesterday]
            if not recent_cdls.empty:
                self.prev_day_cdl_all = recent_cdls
        self.prev_day_cdl_all = pd.concat([self.prev_day_cdl_all,self.tick_data.loc[self.tick_data.groupby('instrument_token')['date_time'].idxmax()]])
        self.prev_day_cdl_all = self.prev_day_cdl_all[self.prev_day_cdl_all.instrument_token.isin(np.r_[self.cum_table['Ref_stock_tkn'].unique(),self.cum_table['Index_tkn'].unique()])]
        self.prev_day_cdl_all = self.prev_day_cdl_all.loc[self.prev_day_cdl_all.groupby('instrument_token')['date_time'].idxmax()]
        #write to db
        try:
            if not eval(x).empty and not DEBUG:
                AlgoInfo.create_or_update(
                    broker=self.client.broker,
                    tablename=x[5:],
                    tabledata=orjson.dumps(eval(x).to_json(orient='records'),default=self.json_datetime_converter).decode('utf-8')
                )
        except Exception as e:
            general_logger.info('%s' % e, exc_info=True)

    def post_trde(self):
        '''Perform post trading activities'''
        general_logger.info('Entered post trade function')
        # self.prev_day_cdl_all = pd.DataFrame([])
        # self.delta_tick_data_raw = ProcessedTickStore.get_with_time(seconds=5000)
        # self.delta_tick_data = pd.DataFrame(orjson.loads(item["data"]) for item in self.delta_tick_data_raw)
        # self.delta_tick_data['date_time'] = pd.to_datetime(self.delta_tick_data['current_time'], format='ISO8601',errors='coerce')
        # self.delta_tick_data.reset_index(drop=True, inplace=True)
        # self.prev_day_cdl_all = self.delta_tick_data.loc[self.delta_tick_data.groupby('instrument_token')['date_time'].idxmax()]
        if not self.open_positions.empty:
            for k in self.open_positions['instrument_token']:
                if self.cum_table[self.cum_table['instrument_token'] == k]['exchange'].values[0] != 'NFO':
                    # nearest_price = der_30_cdl[['close','open']].head(1).min(axis = 1).values[0]
                    nearest_price = self.open_positions[self.open_positions['instrument_token']==k]['last_price'].values[0]
                    if not self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].empty:
                        if nearest_price > self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].values[0] and not DEBUG:
                            self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'] = nearest_price
                            self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'date_time'] = timezone.make_naive(timezone.now())
                            self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token', keep='last')
                            trade_logger.info('post_trde sl update attempted for ' + self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'tradingsymbol'].values[0] + ' from ' + str(self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].values[0]) + ' to ' + str(nearest_price))
                            trade_logger.info('nearest price: ' + str(nearest_price))
                            trade_logger.info('existing price: ' + str(self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].values[0]))
                        else:
                            trade_logger.info('post_trde sl no  update attempted for ' + self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'tradingsymbol'].values[0])
                    else:
                        trade_logger.info('tick data empty for ' + self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'tradingsymbol'].values[0])
        else:
            trade_logger.info('open position empty')
        self.update_algo_info_table()
        self.clear_tables()
        self.post_trade_done = True
        return self.post_trade_done

    def pre_trde(self):
        '''Perform pre trading activities--- currently disabled'''
        general_logger.info('Entered pre trade function')

        if not self.open_positions.empty:
            for k in self.open_positions['instrument_token']:
                # if self.cum_table[self.cum_table['instrument_token'] == k]['cap'].values[0] != 'weekely_options':
                if self.cum_table[self.cum_table['instrument_token'] == k]['exchange'].values[0] != 'NFO':
                    nearest_price = self.open_positions[self.open_positions['instrument_token']==k]['last_price'].values[0]
                    if not self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].empty:
                        if nearest_price > self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].values[0] and not DEBUG:
                            self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'] = nearest_price
                            self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'date_time'] = timezone.make_naive(timezone.now())
                            self.stop_loss_info = self.stop_loss_info.drop_duplicates(subset='instrument_token', keep='last')
                            trade_logger.info('pre_trde sl update attempted for ' + self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'tradingsymbol'].values[0] + ' from ' + str(self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].values[0]) + ' to ' + str(nearest_price))
                            trade_logger.info('nearest price: ' + str(nearest_price))
                            trade_logger.info('existing price: ' + str(self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'buy_price'].values[0]))
                        else:
                            trade_logger.info('pre_trde sl no  update attempted for ' + self.stop_loss_info.loc[self.stop_loss_info['instrument_token'] == k, 'tradingsymbol'].values[0])
                    else:
                        trade_logger.info(str(self.cum_table[self.cum_table['instrument_token'] == k]['tradingsymbol'].values[0])+' not present' )
        else:
            trade_logger.info('open position empty')
        self.update_algo_info_table()
        self.clear_tables()
        self.pre_trade_done = True
        return self.pre_trade_done



    # @timeit
    def trde(self):
        '''
        This function performs the operations like getting data from the database, denoising it, creating buy and sell list and finally passing it to buy and sell loops
        This function returns  position and orders dataframe
        '''

        # if not self.delta_tick_data.empty:
        #     delta_tick_time = int(abs(pd.Timedelta((pd.Timestamp.now() - self.delta_tick_data['date_time'].max())).seconds))
        #     delta_tick_time = int(10)
        #     logger.info('delta_tick_time : '+str(delta_tick_time))
        # elif DEBUG and not self.delta_tick_data.empty:
        #     self.scan_window = 1 #days
        #     delta_tick_time = int(30) #seconds
        #     logger.info('delta_tick_time : ' + str(delta_tick_time))
            # delta_tick_time = 40  # converting minutes to seconds
        # else:
        #     self.scan_window = 1  # seconds
        #     self.tick_data_raw = ProcessedTickStore.get_with_time(days=self.scan_window)
        #     # self.tick_data_raw = cx.read_sql(self.db_conn,"SELECT data FROM kalai_processedtickstore WHERE timestamp >= NOW() - INTERVAL '1 DAYS'")
        #     if len(self.tick_data_raw)>0:
        #         # self.tick_data = pd.json_normalize(self.tick_data_raw.apply(lambda row: orjson.loads(orjson.loads(row['data'])), axis=1))
        #         self.tick_data = pd.DataFrame.from_records([orjson.loads(item) for item in (list(map(lambda x: x["data"], self.tick_data_raw)))])
        #         self.tick_data['date_time'] = pd.to_datetime(self.tick_data['exchange_timestamp'], format='ISO8601',
        #                                                      errors='coerce')
        #     else:
        #         self.tick_data = pd.DataFrame([])
        #     delta_tick_time = int(10)
        #     logger.info('delta_tick_time : ' + str(delta_tick_time))
        #     logger.info('scan window : ' + str(self.scan_window))

        if DEBUG:
            general_logger.info('data query sent from local')
            self.delta_tick_data_raw = ProcessedTickStore.get_with_time(seconds=1000000)
            if len(self.delta_tick_data_raw)>0:
                self.delta_tick_data = pd.DataFrame(orjson.loads(item["data"]) for item in self.delta_tick_data_raw)
                # self.delta_tick_data = pd.json_normalize([orjson.loads(item) for item in (list(map(lambda x: x["data"], self.delta_tick_data_raw)))])
                # self.delta_tick_data['date_time'] = pd.to_datetime(self.delta_tick_data['exchange_timestamp'], format='ISO8601',errors='coerce')#ass
                self.delta_tick_data['date_time'] = pd.to_datetime(self.delta_tick_data['current_time'],format='ISO8601', errors='coerce')
                self.delta_tick_data = self.delta_tick_data.groupby(['date_time', 'instrument_token']).tail(1)
            else:
                self.delta_tick_data = pd.DataFrame([])
            # self.delta_tick_data = pd.json_normalize(TickStore.get_with_time(days=3, fields=['instrument_token','last_price', 'date_time','oi'],instrument_tokens=None))
            # self.delta_tick_data['date_time'] = pd.to_datetime(self.delta_tick_data['date_time'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            self.delta_tick_data_raw = fetch_ticker_data_from_redis(broker_name="zerodha")
            # logger.info(str(self.delta_tick_data_raw))
            general_logger.info('redis data query length is : '+ str(len(self.delta_tick_data_raw)))
            # self.delta_tick_data_raw = TickStore.get_with_time(seconds=delta_tick_time)

            if len(self.delta_tick_data_raw)>0:
                self.delta_tick_data = pd.json_normalize(self.delta_tick_data_raw)
                # self.delta_tick_data = pd.DataFrame.from_records([orjson.loads(item) for item in (list(map(lambda x: x["data"], self.delta_tick_data_raw)))])
                # self.delta_tick_data['date_time'] = pd.to_datetime(self.delta_tick_data['exchange_timestamp'], format='ISO8601',errors='coerce')#ass
                self.delta_tick_data['date_time'] = pd.to_datetime(self.delta_tick_data['current_time'],format='ISO8601', errors='coerce')  # ass
                now = timezone.make_naive(timezone.now())
                analyse_start = pd.to_datetime(now.replace(hour=8, minute=59, second=0, microsecond=0))
                self.delta_tick_data = self.delta_tick_data.sort_values(by='date_time', ascending=True)
                self.delta_tick_data = self.delta_tick_data[(self.delta_tick_data['date_time'] > analyse_start) & (self.delta_tick_data['date_time'] <= now)]
                self.delta_tick_data = self.delta_tick_data.groupby(['date_time', 'instrument_token']).tail(1)
                # general_logger.info(self.delta_tick_data)
                # general_logger.info('delta_tick_data max_time_after_clip: %s' % str(self.delta_tick_data['date_time'].max()))
                # general_logger.info('delta_tick_data min_time_after_clip: %s' % str(self.delta_tick_data['date_time'].min()))
                # general_logger.info('length %s' % len(self.delta_tick_data))
            else:
                self.delta_tick_data = pd.DataFrame([])
            # self.delta_tick_data = pd.json_normalize(TickStore.get_with_time(seconds=delta_tick_time, fields=['instrument_token', 'last_price', 'date_time','oi'],instrument_tokens=None))

        # self.tick_data = self.tick_data.append(self.delta_tick_data)

        # self.tick_data = self.tick_data[~self.tick_data.index.duplicated(keep='first')]
        general_logger.info('data retrieved ')
        self.tick_data = pd.concat([self.tick_data, self.delta_tick_data], ignore_index=True)
        if  not self.tick_data.empty:
            self.data_ready = True
            # if not DEBUG:
                # self.tick_data = self.tick_data[self.tick_data['date_time'] >= (self.tick_data['date_time'].max() - pd.Timedelta(seconds=int(self.scan_window * 1.1)))]  # remove old data
                # self.delta_tick_data = self.delta_tick_data[self.delta_tick_data['date_time'] >= (self.delta_tick_data['date_time'].max() - pd.Timedelta(seconds=int(400)))]
                # logger.info('delta_tick_data max_time_after_clip: %s'% str(self.delta_tick_data['date_time'].max()))
                # logger.info('delta_tick_data min_time_after_clip: %s'% str(self.delta_tick_data['date_time'].min()))
                # logger.info('length %s' % len(self.delta_tick_data))
                # self.delta_tick_data = self.delta_tick_data.set_index('date_time')
                # general_logger.info(self.tick_data.loc[0])
                # self.tick_data = self.tick_data.between_time('3:45', '18:55')

            # else:
                # self.delta_tick_data['date_time'] = pd.to_datetime(self.delta_tick_data['exchange_timestamp'],format='ISO8601',errors='coerce')
                # self.delta_tick_data = self.delta_tick_data[self.delta_tick_data['date_time'] >= (self.delta_tick_data['date_time'].max() - pd.Timedelta(seconds=int(4000)))]
                #
                # logger.info('delta_tick_data max_time: %s', str(self.delta_tick_data['date_time'].max()))
                # logger.info('delta_tick_data min_time: %s', str(self.delta_tick_data['date_time'].min()))
                # self.delta_tick_data = self.delta_tick_data.set_index('date_time')
                # self.tick_data = self.tick_data.between_time('9:15', '15:30')
            # logger.info('delta_tick_data max_time_after_clip: %s'% str(self.delta_tick_data['date_time'].max()))
            # logger.info('delta_tick_data min_time_after_clip: %s'% str(self.delta_tick_data['date_time'].min()))
            # logger.info('length %s' % len(self.delta_tick_data))
            # self.delta_tick_data = self.delta_tick_data.sort_values(by='date_time', ascending=True)
            # self.delta_tick_data.reset_index(drop=False, inplace=True)
            self.tick_data = self.tick_data.sort_values(by='date_time', ascending=True)
            self.tick_data =self.tick_data[self.tick_data['mode']=='full']

            if DEBUG:
                now = timezone.make_naive(timezone.now())
                analyse_start = pd.to_datetime(now.replace(hour=8, minute=59, second=0, microsecond=0))
                analyse_end = pd.to_datetime(now.replace(hour=16, minute=00, second=10, microsecond=28))
                self.tick_data = self.tick_data.sort_values(by='date_time', ascending=True)
                self.tick_data = self.tick_data[(self.tick_data['date_time'] > analyse_start) & (self.tick_data['date_time'] <= now)]
            self.strike_detect(self.tick_data)
            # self.pos_day_frame, self.pos_net_frame = self.client.pos_data()  # update position after every order
            # self.open_positions = self.open_position_update(self.pos_day_frame, self.pos_net_frame)  # uncomment
            # self.hold_frame = self.current_holdings(self.client.holdings())
            # self.order_status = self.order_status_update()
            self.updated_list = self.token_list_update()
            self.final_tkns = self.updated_list['instrument_token'].dropna()
            self.final_ref_tokens =list(set(self.updated_list['instrument_token'].dropna()).intersection(self.cum_table['Ref_stock_tkn'].values))
            self.insert_instrument_token(self.final_tkns.values.tolist())
            self.stock_info_table = pd.DataFrame([])
            for self.tkn in self.final_ref_tokens:
                if self.tkn in self.cum_table['Ref_stock_tkn'].values:
                    try:
                        self.session_ref_data = self.tick_data[self.tick_data['instrument_token'] == self.tkn]
                        self.session_ref_data.reset_index(drop=True, inplace=True)
                        self.session_ref_data = self.session_ref_data.set_index('date_time')

                    except:
                        self.session_ref_data = pd.DataFrame([])

                    if not self.session_ref_data.empty and any(self.cum_table[self.cum_table['instrument_token'] == self.tkn]['segment'].values == 'CDS-FUT') and self.tkn != -1:  # this will change if indices were used instead of the futures
                        if not self.cum_table[self.cum_table['instrument_token'] == self.tkn].empty:
                            div_val = self.cum_table[self.cum_table['instrument_token'] == self.tkn]['tick_size'].values[0]
                            self.session_ref_data['last_price'] = self.session_ref_data['last_price'].div(div_val)
                        else:
                            self.session_ref_data = pd.DataFrame([])
                if not self.session_ref_data.empty:
                    # self.osc_det(self.tick_data,self.tkn)
                    self.stock_info_table = pd.concat([self.stock_info_table,self.mom(int(self.tkn))])
                    self.stock_info_table.reset_index(drop=True, inplace=True)
            # if not self.stock_info_table.empty:
            #     self.stock_info_table = self.stock_info_table.astype({'instrument_token': 'int', 'buy_signal_CE': 'int', 'buy_signal_PE': 'int', 'PE_jump': 'int', 'CE_jump': 'int','exchange': 'str', 'symbol': 'str'}, errors='ignore')
            self.buy_stock_cap, self.sell_stock_table = self.derivative_analysis(self.stock_info_table,self.tick_data)
                # put jump function here
                # self.buy_stock_cap, self.sell_stock_table = self.jump(self.buy_stock_cap_init, self.sell_stock_table_init,self.tick_data)

            # if (not self.sell_stock_table.empty) or (not self.buy_stock_cap.empty):
            self.buy_sell_loop()
            # self.strike_update() #not needed now
            self.update_order_list()
            self.pos_day_frame, self.pos_net_frame = self.client.pos_data()  # update position after every order
            self.open_positions = self.open_position_update(self.pos_day_frame, self.pos_net_frame)  # uncomment
            self.order_status = self.order_status_update()
            self.slu()
            self.update_algo_info_table()
            self.order_pending_chk()
            if self.prev_cdl_save_time() :
                general_logger.info('Entered prev cdl save')
                self.prev_cdl_save()
            if not DEBUG:
                self.write_db_candle() # to add candle stick data in algoinfo db
            self.tick_data = self.tick_data[self.tick_data['date_time'] >= (self.tick_data['date_time'].max() - pd.Timedelta(seconds=int(180)))]
            general_logger.info('tick_data length: %s', len(self.tick_data))
                # general_logger.info('tick_data max_time: %s', str(self.tick_data['date_time'].max()))
                # general_logger.info('tick_data min_time: %s', str(self.tick_data['date_time'].min()))


                # if self.post_trade_session() or self.pre_trade_session():
                # if self.post_trade_session() :
                # if not self.open_positions.empty:
                # general_logger.info('stop loss table updated')
                # self.stoploss_update(self.open_positions['instrument_token'].values)




if __name__ == "__main__":
    # from plotly.subplots import make_subplots
    # import plotly.graph_objects as go
    algo = TradeAlgo(disco_bro=ZerodhaUtility())
    algo.trde()


    # reference_sysmols = pd.read_excel('my_algo/data/token.xlsx', sheet_name='Stock_list')
    def roll_avg(df, window):
        init_mean = df.mean()
        mov_avg = np.convolve(np.r_[np.repeat(init_mean, window * 2), df], np.ones(window), 'valid') / window
        mov_avg = mov_avg[np.isnan(mov_avg) != True]
        return mov_avg

        # raw_log = pd.read_csv('my_algo/data/trade.log', header=None, names=['log'])
        #
        # raw_log['log'] = raw_log['log'].astype(str)
        #
        # order_df = pd.concat(
        #     (raw_log[raw_log['log'].str.contains('Sell')], raw_log[raw_log['log'].str.contains('Buy')])).sort_index(
        #     ascending=True)
        # order_df['trial'] = order_df['log'].str[26:]
        # order_df.index = order_df.index.set_names(['time'])
        # order_df = order_df.reset_index()
        # order_df[['date', 'time_sec']] = order_df['time'].str.split(" ", n=1, expand=True)
        # symbol,intrument_token,price,buy_signal,counter
        # order_df[['symbol', 'intrument_token','price',  'b/s', 'counter']] = order_df['trial'].str.split('/', expand=True)

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            specs=[[{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]])

        fig.add_trace(go.Scatter(x=resampled_olhc['date'], y=exp_short['norm'], name='exp_short_norm'),
                      secondary_y=False, row=1, col=1)
        fig.update_yaxes(title_text="exp data", row=1, col=1)

        fig.add_trace(go.Scatter(x=resampled_olhc['date'], y=exp_long['norm'], name='exp_long_norm'), secondary_y=False,
                      row=1, col=1)
        # fig.add_trace(go.Scatter(x=ren_frame['date'], y=buy_signal, name='buy_signal'),secondary_y=True, row=1, col=1)
        fig.update_yaxes(title_text="moving average", row=1, col=1)

        # fig.add_trace(go.Scatter( x=resampled_olhc['date'],y=mov_diff['close'],name = 'diff'),
        #               secondary_y=True,row=1, col=1)
        # fig.update_yaxes(title_text="resampled", row=1, col=1)
        fig.add_trace(go.Scatter(x=resampled_olhc['date'], y=resampled_olhc['norm'], name='resampled'),
                      secondary_y=False, row=1, col=1)
        fig.update_yaxes(title_text="resampled", row=1, col=1)

        # fig.add_trace(go.Scatter(x=ren_frame['date'],y=macd,name = 'macd'),row=2, col=1)
        # fig.add_trace(go.Scatter(x=ren_frame['date'],y=signal,name = 'signal'),row=2, col=1)
        # fig.add_trace(go.Scatter(x=ren_frame['date'],y=hist, name='hist'), row=2, col=1)
        fig.add_trace(go.Scatter(x=resampled_olhc['date'], y=buy_signal, name='buy_signal'), row=2, col=1)
        fig.update_yaxes(title_text="macd_ren", row=2, col=1)

        # fig.add_trace(go.Scatter(x=resampled_olhc['date'], y=np.linspace(1,e,e), name='buy_signal'), row=3, col=1)
        fig.add_trace(go.Scatter(x=resampled_olhc['date'], y=diff_long_short_1[0].values, name='diff_long_short_1'),
                      secondary_y=True, row=3, col=1)
        fig.add_trace(go.Scatter(x=resampled_olhc['date'], y=diff_long_short_2[0].values, name='diff_long_short_2'),
                      secondary_y=False, row=3, col=1)

        fig.update_layout(title_text=symbol)
        fig.show()

        return resampled_olhc

    # ren_analysis(1500, reference_sysmols, 0.5, 'PETRONET')
    # analyse_stocks = ['LUPIN','NATIONALUM','DIVISLAB','VEDL','GODREJCP', 'EMAMILTD','DRREDDY','MGL','HAVELLS','APLAPOLLO','DABUR']
    # analyse_stocks = [ 'GSPL','JINDALSTEL','NATIONALUM']
    # #
    # for rr in range(len(analyse_stocks)):
    #     ren_analysis(1500, reference_sysmols, 0.5, analyse_stocks[rr])


# https://stackoverflow.com/questions/46075960/live-updating-only-the-data-in-dash-plotly -- for dashboard
# exm_buy = self.client.placeOrder('IDEA',1,'BUY','NSE')
# ordr_frame = pd.DataFrame(self.client.order_trades(exm_buy))
# ordr_frame.to_excel('my_algo/data/order_trades.xlsx')
# only_order = self.client.orders()
# only_order.to_excel('my_algo/data/all_orders.xlsx')
# hold_frame = self.client.holdings()
# hold_frame.to_excel('my_algo/data/holdings.xlsx')
# pos_day, pos_net = self.client.pos_data()
# pos_day.to_excel('my_algo/data/pos_day.xlsx')
# pos_net.to_excel('my_algo/data/pos_net.xlsx')


# https://stackoverflow.com/questions/22783778/initialize-list-to-a-variable-in-a-dictionary-inside-a-loop
# https://stackoverflow.com/questions/26367812/appending-to-list-in-python-dictionary
# https://stackoverflow.com/questions/59483010/add-a-list-to-a-dictionary-key-within-a-for-loop-with-an-undefined-key
# https://kite.trade/forum/discussion/comment/36996/#Comment_36996 --> order margin example
# https://github.com/zerodha/pykiteconnect/blob/master/examples/order_margins.py#L33
# order_param_single = [{
#     "exchange": "NSE",
#     "tradingsymbol": "SBIN",
#     "transaction_type": "BUY",
#     "variety": "regular",
#     "product": "MIS",
#     "order_type": "MARKET",
#     "quantity": 1
# }]
# order_param_single = pd.DataFrame(order_param_single)
# self.client.get_margin(order_param_single)
def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper

# @run_once
# def my_function(foo, bar):
#    return foo+bar
