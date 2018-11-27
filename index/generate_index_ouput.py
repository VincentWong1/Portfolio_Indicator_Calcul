# !usr/bin/env python
# -*- coding:utf-8 -*-

"""Generating Ouput of Portfolio Indexs Calculation Moudle"""

from __future__ import unicode_literals, division

import pandas as pd
import numpy as np
from common.base.base_class import BaseClass

import warnings
import sys
warnings.filterwarnings('ignore')
reload(sys); sys.setdefaultencoding('utf8')

__author__ = "WiGi"
__mtime__ = "June 24, 2018"


class OutputGenerate(BaseClass):

    def __init__(self):
        BaseClass.__init__(self)
        self.__switcher1 = {'Ann_Ret_h': 'ZB001',
                            'Ann_Vol_h': 'ZB002',
                            'Sharpe_ratio_h': 'ZB003',
                            'MDrawdown_h': 'ZB004',
                            'Ann_Ret_y': 'ZB005',
                            'Ann_Vol_y': 'ZB006',
                            'Sharpe_ratio_y': 'ZB007',
                            'MDrawdown_y': 'ZB008'}

        self.__switcher2 = {'Ann_Ret_h': u'全历史年化收益率',
                            'Ann_Vol_h': u'全历史年化波动率',
                            'Sharpe_ratio_h': u'全历史夏普比率',
                            'MDrawdown_h': u'全历史最大回撤',
                            'Ann_Ret_y': u'近一年年化收益率',
                            'Ann_Vol_y': u'近一年年化波动率',
                            'Sharpe_ratio_y': u'近一年年化夏普比率',
                            'MDrawdown_y': u'近一年年化最大回撤'}
        self._date = None

    def flatten_ouput(self, allportfolio_record_df_index):
        allstack_record = []
        allportfolio_record_df = allportfolio_record_df_index.toPandas()
        allportfolio_record_df.columns = ['portfolio_id',
                                          'portfolio_name',
                                          'TradingDate',
                                          'Ann_Ret_h',
                                          'Ann_Vol_h',
                                          'Sharpe_ratio_h',
                                          'MDrawdown_h',
                                          'Ann_Ret_y',
                                          'Ann_Vol_y',
                                          'Sharpe_ratio_y',
                                          'MDrawdown_y']
        #append_date = datetime.date.today()+datetime.timedelta(days=-1)
        #allportfolio_record_df = allportfolio_record_df[allportfolio_record_df.TradingDate == append_date]
        for i in range(len(allportfolio_record_df)):
            arecord = allportfolio_record_df.iloc[i]
            com_id = arecord[0]
            com_name = arecord[1]
            trading_date = arecord.TradingDate.strftime(format="%Y-%m-%d")
            stack_record_b = [com_id, com_name, trading_date]
            stack_record_f_tmp = arecord[["Ann_Ret_h", "Ann_Vol_h", "Sharpe_ratio_h", "MDrawdown_h",
                                          "Ann_Ret_y", "Ann_Vol_y", "Sharpe_ratio_y", "MDrawdown_y"]]
            for index_name in stack_record_f_tmp.index:
                quota_code = self.__switcher1.get(index_name, np.nan)
                quota_name = self.__switcher2.get(index_name, np.nan)
                quota_val = float(stack_record_f_tmp[index_name])
                stack_record_f = [quota_code, quota_name, quota_val]
                stack_record = stack_record_f + stack_record_b
                allstack_record.append(stack_record)

        try:
            allstack_record_df = pd.DataFrame(data=allstack_record,
                                              columns=["quota_code",
                                                       "quota_name",
                                                       "quota_val",
                                                       "com_id",
                                                       "com_name",
                                                       "trading_date"])
        except ValueError:
            print "No Valid Data to be appended"
            allstack_record_df = pd.DataFrame()

        allstack_record = self.spark.createDataFrame(allstack_record_df.values.tolist(),
                                                        list(allstack_record_df.columns))
        return allstack_record

if __name__ == '__main__':
    pass