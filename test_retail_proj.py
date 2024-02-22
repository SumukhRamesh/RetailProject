import pytest
from lib.Utilis import get_spark_session
from lib.datareader import read_customers, read_orders
from lib.datamanipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.configreader import get_app_config


def test_read_customers_df(spark):
    customersCount = read_customers(spark,'LOCAL').count()
    assert customersCount == 12435

def test_read_orders_df(spark):
    ordersCount = read_orders(spark,'LOCAL').count()
    assert ordersCount == 68883


@pytest.mark.transformation
def test_filter_closed_orders(spark):
    ordersDF = read_orders(spark,'LOCAL')
    filteredOrdersCount = filter_closed_orders(ordersDF).count()
    assert filteredOrdersCount == 7556

def test_read_app_config():
    configDict = get_app_config('LOCAL')
    assert configDict["orders.file.path"] == "data/orders.csv"

@pytest.mark.transformation
def test_count_orders_state(spark, expected_results):
    customersDF = read_customers(spark, "LOCAL")
    actualResults = count_orders_state(customersDF)
    assert actualResults.collect() == expected_results.collect()

@pytest.mark.latest()
def test_check_closed_count(spark):
    ordersDF = read_orders(spark,'LOCAL')
    filteredOrdersCount = filter_orders_generic(ordersDF, "CLOSED").count()
    assert filteredOrdersCount == 7556


@pytest.mark.latest()
def test_check_pendingpayment_count(spark):
    ordersDF = read_orders(spark,'LOCAL')
    filteredOrdersCount = filter_orders_generic(ordersDF, "PENDING_PAYMENT").count()
    assert filteredOrdersCount == 15030


@pytest.mark.latest()
def test_check_complete_count(spark):
    ordersDF = read_orders(spark,'LOCAL')
    filteredOrdersCount = filter_orders_generic(ordersDF, "COMPLETE").count()
    assert filteredOrdersCount == 22899

@pytest.mark.parametrize(
        "status, count",
        [("CLOSED", 7556),
         ("PENDING_PAYMENT", 15030),
         ("COMPLETE", 22899)
         ]

)

def test_check_count(spark, status, count):
    ordersDF = read_orders(spark,'LOCAL')
    filteredOrdersCount = filter_orders_generic(ordersDF, status).count()
    assert filteredOrdersCount == count
