# !usr/bin/env python
# -*- coding:utf-8 -*-
"""Calculate Indicators of Porfolios in history and last 1 year
"""

from __future__ import unicode_literals, division

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import numpy as np

from common.base.base_class import BaseClass
from index import constants as ctnt

import sys
import warnings
warnings.filterwarnings('ignore')
reload(sys)
sys.setdefaultencoding('utf8')

__author__ = 'WiGi'
__mtime__ = 'Oct 31, 2018'

class GenerateIndexValue(BaseClass):

	def __init__(self, constant=ctnt):
		BaseClass.__init__(self)
		self.__constants = constant

	@property
	def risk_free(self):
		"""
        :return:
        """
		risk_free = self.operator.get_table(self.__constants.RISK_FREE_RATE_ADJUST_TABLE) \
			.select(F.to_date('effective_date').alias('effective_date'),
					(F.regexp_replace('dbir_after', '%', '').cast(DoubleType()) / 100).alias('rfrate'))
		return risk_free

	@property
	def portfolio_acret(self):
		"""
        :return:
        """
		pf_acret = self.operator.get_table(self.__constants.PORTFOLIO_ACCUM_RET_TABLE) \
			.select('com_id',
					'com_name',
					F.to_date('trading_date').alias('trading_date'),
					(F.col('accum_ret_h').cast(DoubleType()) + 1.0).alias('accum_ret_h')) \
			.filter(F.col('ifbenchmark') == False)
		# pf_acret.show(5)
		return pf_acret

	@property
	def portfolio_tradingdays(self):
		tradingdays = self.operator.get_table(self.__constants.SECURITY_TRADING_DAY_TABLE) \
			.filter(F.col('isopen') == '1') \
			.select(F.to_date('calendardate').alias('trading_date'))
		return tradingdays

	@property
	def risk_free_rateline(self):
		portfolio_tradingdays = self.portfolio_tradingdays
		risk_free = self.risk_free
		risk_free_rates = portfolio_tradingdays.join(risk_free,
													 [portfolio_tradingdays.trading_date ==
													  risk_free.effective_date], 'outer')
		rf = risk_free_rates.withColumn('effective_date', F.coalesce(risk_free_rates.trading_date,
																	 risk_free_rates.effective_date)) \
			.withColumn('rfrate', F.round(F.last(F.col('rfrate'), True).over(self.__constants.w_order), 4)) \
			.filter(F.col('trading_date').isNotNull()) \
			.filter(F.col('rfrate').isNotNull()) \
			.drop('effective_date').cache()
		# rf.show(5)
		return rf

	@property
	def cal_indexs(self):
		portfolio_acret = self.portfolio_acret
		risk_free_rateline = self.risk_free_rateline
		basedata = portfolio_acret.join(risk_free_rateline,
										'trading_date') \
			.drop(risk_free_rateline.trading_date)# .dropDuplicates(['trading_date', 'com_id'])

		basedata = basedata.withColumn('order_nm_desc', F.row_number().over(self.__constants.w_h_desc)) \
			.withColumn('order_nm', F.row_number().over(self.__constants.w_h)) \
			.withColumn('date_diff',
						F.datediff(F.col('trading_date'), F.min(F.col('trading_date')).over(self.__constants.w_h))) \
			.cache()

		basedata = basedata.withColumn('init_arh',
									   F.first(F.col('accum_ret_h')).over(self.__constants.w_h)) \
			.withColumn('annl_ret_h',
						F.when(basedata.date_diff >= 30,
							   F.round(F.pow(F.col('accum_ret_h') / F.col('init_arh'),
											 242.0 / F.col('order_nm')) - 1.0, 6)) \
						.otherwise(None)) \
			.withColumn('pre1y_arh',
						F.first(F.col('accum_ret_h')).over(self.__constants.w_y)) \
			.withColumn('pre1y_odn',
						F.first(F.col('order_nm')).over(self.__constants.w_y)) \
			.withColumn('annl_ret_1y',
						F.when((basedata.date_diff >= 30) & (basedata.date_diff < 365),
							   F.round(F.pow(F.col('accum_ret_h') / F.col('pre1y_arh'),
											 242.0 / (F.col('order_nm') - F.col('pre1y_odn') + 1)) - 1.0, 6)) \
						.when(basedata.date_diff >= 365,
							  F.round(F.pow(F.col('accum_ret_h') / F.col('pre1y_arh'),
											242.0 / 242.0) - 1.0, 6))
						.otherwise(None))

		basedata = basedata.withColumn('lograte',
									   F.log(basedata.accum_ret_h / F.lag(basedata.accum_ret_h, 1, default=1.0) \
											 .over(self.__constants.w_unbnd))) \
			.withColumn('annl_std_1y',
						F.when(basedata.date_diff >= 30,
							   F.round(F.stddev(F.col('lograte')).over(self.__constants.w_y) * np.sqrt(242.0), 6)) \
						.otherwise(None)
						) \
			.withColumn('annl_std_h',
						F.when(basedata.date_diff >= 30,
							   F.round(F.stddev(F.col('lograte')).over(self.__constants.w_h) * np.sqrt(242.0), 6)) \
						.otherwise(None)
						) \
			.withColumn('drawdown',
						F.col('accum_ret_h') / F.max(F.col('accum_ret_h')).over(self.__constants.w_h) - 1.0) \
			.withColumn('max_drawdown_h',
						F.round(F.min(F.col('drawdown')).over(self.__constants.w_h), 6)) \
			.withColumn('sharp_ratio_1y',
						F.round((F.col('annl_ret_1y') - F.col('rfrate')) / F.col('annl_std_1y'), 6)) \
			.withColumn('sharp_ratio_h',
						F.round((F.col('annl_ret_h') - F.col('rfrate')) / F.col('annl_std_h'), 6)
						).filter(basedata.order_nm_desc <= 242)

		basedata = basedata.withColumn('drawdown_1y',
									   F.col('accum_ret_h') / F.max(F.col('accum_ret_h')).over(self.__constants.w_h) - 1.0) \
			.withColumn('max_drawdown_1y',
						F.round(F.min(F.col('drawdown_1y')).over(self.__constants.w_h), 6))

		init_result = basedata.select('com_id',
									  'com_name',
									  'trading_date',
									  'annl_ret_h',
									  'annl_std_h',
									  'sharp_ratio_h',
									  'max_drawdown_h',
									  'annl_ret_1y',
									  'annl_std_1y',
									  'sharp_ratio_1y',
									  'max_drawdown_1y')
		return init_result

if __name__ == '__main__':
	gen_index_data = GenerateIndexValue()
	result = gen_index_data.cal_indexs
	#result = result.filter(result.com_id == '859').sort(result.trading_date.desc())#(F.desc('trading_date'))
	result.show(20)