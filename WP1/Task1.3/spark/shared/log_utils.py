
import logging

from pyspark.sql import DataFrame


class LogUtils(object):

    def __init__(self) -> None:
        """Initialize class
        
        :return: None
        :rtype: None
        """
        self._logger = logging.getLogger(__name__)

    def describe_df(self, df: DataFrame, df_name: str) -> None:
        """Log basic info about a data frame
        
        :param df: Data frame to print
        :type df: DataFrame
        :param df_name: Descriptive name
        :type df_name: str
        :return: None
        :rtype: None
        """
        top_x = 5
        logger = logging.getLogger(__name__)
        logger.info(f'DataFrame: {df_name}')
        logger.info('Data schema:')
        df.printSchema()
        logger.info(f'Number of rows: {df.count()}'.format(df.count()))
        logger.info(f'First {top_x} records')
        df.show(n=top_x)
