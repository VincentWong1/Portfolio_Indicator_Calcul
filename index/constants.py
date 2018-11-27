# !usr/bin/env python
# -*- coding:utf-8 -*-
"""Save Instant Variates
"""
from __future__ import unicode_literals
from pyspark.sql import Window
from pyspark.sql import functions as F

__author__ = 'WiGi'
__mtime__ = 'Nov 2, 2018'

PORTFOLIO_ACCUM_RET_TABLE = 'fbidm.fnd_portfolio_accum_ret'
RISK_FREE_RATE_ADJUST_TABLE = 'fdm_sor.sor_fbicics_t_eastmoney_yhll'
SECURITY_TRADING_DAY_TABLE = 'fdm_dpa.dpa_zt_tradingday_day'
INTERNAL_WORKDAYS = 'fdm_sor.sor_bps_working_day_config'

w_order = Window.orderBy('effective_date') \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

w_h = Window.partitionBy('com_id').orderBy('trading_date') \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

w_y = Window.partitionBy('com_id').orderBy('trading_date') \
    .rowsBetween(-242, Window.currentRow)

w_unbnd = Window.partitionBy('com_id').orderBy('trading_date')

w_h_desc = Window.partitionBy('com_id').orderBy(F.desc('trading_date')) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
