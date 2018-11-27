# !usr/bin/env python
# -*- coding:utf-8 -*-

"""Extract last record into table"""

from __future__ import unicode_literals, division

import pandas as pd
from pyspark.sql import functions as F

from index.generate_index_ouput import OutputGenerate
from index.gen_indicator import GenerateIndexValue
from common.base.spark_operator import operator
from Controller_Tool import exe_switch

import warnings
import sys
warnings.filterwarnings('ignore')
reload(sys); sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    if exe_switch():
        gen_index_data = GenerateIndexValue()
        init_result = gen_index_data.cal_indexs
        init_result = init_result.orderBy(['com_id','trading_date'])
        extract_filter = init_result.groupBy('com_id').agg(F.max('trading_date').alias('last_date'),
                                                           F.last('annl_ret_h').alias('last_anlret_rcd')) \
            .filter(F.col('last_anlret_rcd').isNotNull())
        result = extract_filter.join(init_result, [extract_filter.last_date == init_result.trading_date,
                                                   extract_filter.com_id == init_result.com_id]) \
            .drop('last_date') \
            .drop(extract_filter.com_id)\
            .drop(extract_filter.last_anlret_rcd)
        load_data = OutputGenerate()
        add_record = load_data.flatten_ouput(result)
        add_record.show()

        operator.save(
            'fbidm.fnd_portfolio_index',
            add_record.withColumn('updatetime', F.current_timestamp())\
                .select('quota_code',
                        'quota_name',
                        'quota_val',
                        'com_id',
                        'com_name',
                        'trading_date',
                        'updatetime'),
            overwrite=True)
    else:
        print 'Yesterday is not trading date!'
