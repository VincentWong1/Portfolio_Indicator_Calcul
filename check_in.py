
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