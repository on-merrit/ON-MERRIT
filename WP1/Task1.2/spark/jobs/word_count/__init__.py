
import sys
import logging
from typing import List
from os.path import join

from pyspark import SparkContext, SQLContext

from shared.log_utils import LogUtils


def analyze(sc, cfg):
    """
    Run job 
    :param sc: SparkContext
    :param cfg: app configuration
    :return: None
    """
    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))
    logger.info('Counting words...')

    # needs to be initialized (even if not used) to be able 
    # to work with DataFrames
    sql_sc = SQLContext(sc)
    
    core_dir = cfg['hdfs']['core_dir']

    text_01 = (
        f'CORE’s mission is to aggregate all open access research outputs from '
        f'repositories and journals worldwide and make them available to the '
        f'public. In this way CORE facilitates free unrestricted access to '
        f'research for all.'
    )
    text_02 = (
        f'CORE harvests research papers from data providers from all over the' 
        f'world including institutional and subject repositories, open access '
        f'and hybrid journal publishers.'
    )
    text_03 = (
        f'CORE currently contains 135,539,113 open access articles, from '
        f'thousands and over tens of thousands of journals, collected from '
        f'5,969 data providers around the world.'
    )
    text_04 = (
        f'CORE will supply data for the UK REF 2021 Open Access Policy Audit '
        f'to Research England. We provide advice and support to UK HEIs with '
        f'exposing their outputs data to CORE.'
    )

    texts = [text_01, text_02, text_03, text_04]
    words = sc.parallelize(texts).flatMap(lambda text: text.split())
    words = words.map(lambda word: (word, 1))
    counts = words.reduceByKey(lambda a, b: a + b)
    ordered = counts.sortBy(lambda pair: pair[1], ascending=False)
    ordered = ordered.toDF(['word', 'count'])
    LogUtils().describe_df(ordered, 'word_count')
    output_path = join(core_dir, 'test_job_output')
    logger.info(f'Storing results in {output_path}')
    ordered.coalesce(1).write.csv(output_path, mode='overwrite')
