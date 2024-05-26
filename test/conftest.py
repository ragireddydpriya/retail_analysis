import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    ''' Get spark Session'''
    spark_session = get_spark_session('LOCAL')
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    '''Gives Expected Results'''
    results_schema = 'state string,count int'
    return spark.read \
    .format('csv') \
    .option('header',True) \
    .schema(results_schema) \
    .load('test/data/state_aggregate.csv')
