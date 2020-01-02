from pyspark.sql import DataFrame, SparkSession
from typing import List


from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import unicodedata
import re




__author__ = 'bikash gyawali'
__email__ = 'bikash.gyawali@open.ac.uk'



def mag_normalisation_institution_names(institution_name):
        '''
        An approximation function to estimate the way MAG normalises university names
        :param institution_name: The input name of the institution to normalise as per MAG.
        E.g. usage is in https://repl.it/repls/NiceMonthlyFibonacci
        :type str
        :return: normalised names as in the field "normalizedname" of "affiliations" table in MAG.
        :type str
        '''

        # Replace non-ascii by ascii. See https://stackoverflow.com/a/3704793/530399
        # norm_uname = unicodedata.normalize('NFKD', university_name).encode('ascii', 'ignore')
        # https://stackoverflow.com/a/14785625/530399 Python 3 replaced unicode with str
        norm_uname = unicodedata.normalize('NFKD', institution_name).encode("ascii", "ignore").decode("ascii")
        norm_uname = norm_uname.lower()

        # Replace everything inside brackets by nothing.
        # E.g.: University of Natural Resources and Life Sciences, Vienna (Universität für Bodenkultur, Wien) in THE list is represented as "university of natural resources and life sciences vienna" in the normalizedname filed of MAG
        norm_uname = re.sub(r'\([^)]*\)', '', norm_uname)

        # Only preserve the a-z characters and replace the rest by space. Get rid of commas and so on. Also replaces multiple spaces by one.
        # Eg: University of Natural Resources and Life Sciences, Vienna is represented as "university of natural resources and life sciences vienna" in the normalizedname filed of MAG
        norm_uname = re.sub(r'[^\x61-\x7A]+',' ', norm_uname)
        norm_uname = norm_uname.strip()

        return norm_uname


def get_mag_field_ids(
        fields_of_study_df: DataFrame, field_names: List[str], ss: SparkSession
) -> DataFrame:
        """Get a dataframe mapping list of fieldnames to their field IDs in MAG.

        :param fields_of_study_df: df representing fieldsofstudy table of MAG
        :type fields_of_study_df: DataFrame
        :param field_names: list of field names to get IDs for
        :type field_names: List[str]
        :param ss: reference to sparksession object
        :type ss: SparkSession
        :return: Dataframe of field IDs matched to field names
        :rtype: DataFrame
        """
        fieldnames_df = ss.createDataFrame([[x.lower()] for x in set(field_names)], schema=['normalizedname'])
        return fields_of_study_df.join(fieldnames_df, fields_of_study_df['normalizedname'] == fieldnames_df['normalizedname']).select('fieldofstudyid').distinct()