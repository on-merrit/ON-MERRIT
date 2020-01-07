from pyspark.sql import DataFrame, SparkSession
from typing import List


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import unicodedata
import re




__author__ = 'bikash gyawali'
__email__ = 'bikash.gyawali@open.ac.uk'


def array_to_string(my_list):
    out = ""
    if my_list:  # list is not empty
        for elem in my_list:
            if elem is not None:
                out = out + "," + str(elem)
        out = out.strip(",")
    return out

array_to_string_udf = udf(array_to_string,StringType())