# !usr/bin/env python
# -*- coding:utf-8 -*-
"""functions for Machine Learning Platform of IDC
"""
import pandas as pd
import subprocess

__author__ = 'WiGi'
__mtime__ = 'Nov 2, 2018'

class DataPrep(object):
    def __init__(self, table_name, spark):
        self.__tn = table_name
        self.__spark_instance = spark

    # Grasping Data Content
    def cleaning_init_data(self, input_data):
        """:param input_data equals to output from subprocess.getstatusoutput($HQL_shell_cmd)
           :return cleaning_data_list is listtype data which has been cleaned up irrelevant info
        """
        row_data = [item_l1.split('\t') for item_l1 in input_data.split('\n')]
        #right_len = len(row_data[-1])
        cleaning_data_list = [item_l2 for item_l2 in row_data if len(item_l2) != 1]
        return cleaning_data_list

    # Grasping Data Struct
    @property
    def data_columns(self):
        status, output = subprocess.getstatusoutput("hive -S -e 'desc %s'" % self.__tn)
        """from apply import apply
        funct = lambda x: x.rstrip()
        apply(funct, [item[:2] for item in cleaning_init_data(output)])
        """
        refined_data = pd.DataFrame(self.cleaning_init_data(output)).iloc[:, :2]
        column_list = refined_data.iloc[:, 0].apply(lambda x: x.rstrip()).tolist()
        return column_list

    @property
    def init_data(self):
        status, output = subprocess.getstatusoutput("hive -S -e 'select * from "+self.__tn+"'")
        init_data = self.__spark_instance.createDataFrame(self.cleaning_init_data(output), self.data_columns)
        return init_data
