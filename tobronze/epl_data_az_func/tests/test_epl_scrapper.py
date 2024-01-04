import logging
import pytest
import sys
import requests
import pandas as pd
sys.path.append("..")
from tobronze.EPLScrapper import EPLScrapper

# Get logger
logger = logging.getLogger('__epl_scrapper__')
logger.setLevel(logging.INFO)

@pytest.fixture
def epl_scrapper_object():
    mockPrefixUrl = "https://mock.uk/"
    mockUrl = "https://mock.uk/englandm.ph"
    mockRegExpr = r'[a-zA-Z]'
    mockLogger = logging.getLogger('__mock__')
    mockLogger.setLevel(logging.INFO)
    return EPLScrapper(mockPrefixUrl, mockUrl, mockRegExpr, mockLogger)

def test_should_create_scrapper(epl_scrapper_object):
    assert epl_scrapper_object is not None

def test_prefix_url_non_empty_string(epl_scrapper_object):
    prefixUrl = epl_scrapper_object.prefixUrl
    assert isinstance(prefixUrl, str), "prefixUrl should be a string"
    assert prefixUrl, "prefixUrl should not be empty"

def test_url_exists_non_empty_string(epl_scrapper_object):
    url = epl_scrapper_object.url
    assert isinstance(url, str), "URL should be a string"
    assert url, "URL should not be empty"

def test_regular_expression_non_empty_string(epl_scrapper_object):
    regularExpression = epl_scrapper_object.regularExpression
    assert isinstance(regularExpression, str), "regularExpression should be a string"
    assert regularExpression, "regularExpression should not be empty"

def test_successful_page_request(requests_mock, epl_scrapper_object):
    testUrl = "https://football-data.co.uk/englandm.php"
    expectedResponse = 200

    requests_mock.get(testUrl, json=expectedResponse)
    # requests_mock is going to intercept all the get requests
    # made at this URL and return the expected response 200
    result = epl_scrapper_object.request_page(testUrl)
    result = int(result.status_code)
    
    assert result == expectedResponse

def test_unsuccessful_page_request_exeption(mocker, epl_scrapper_object):
    testUrl = "htts://"
    mocker.patch('tobronze.EPLScrapper.requests.get', side_effect=requests.exceptions.RequestException())
    result = epl_scrapper_object.request_page(testUrl)
    
    assert result is None

def test_successful_download_csv_(mocker, epl_scrapper_object):
    # Mock successful dataframe conversion
    mocker.patch('tobronze.EPLScrapper.pd.read_csv', return_value=pd.DataFrame())

    dataLink = 'dailyResult.csv'
    result = epl_scrapper_object.download_csv(dataLink)

    assert isinstance(result, pd.DataFrame)

def test_parser_error_download_csv(mocker, epl_scrapper_object):
    # Mock successful dataframe conversion
    mocker.patch('tobronze.EPLScrapper.pd.read_csv', side_effect=pd.errors.ParserError)

    dataLink = 'dailyResult.csv'
    result = epl_scrapper_object.download_csv(dataLink)

    assert result is None

def test_empty_data_error_download_csv(mocker, epl_scrapper_object):
    # Mock successful dataframe conversion
    mocker.patch('tobronze.EPLScrapper.pd.read_csv', side_effect=pd.errors.EmptyDataError)

    dataLink = 'dailyResult.csv'
    result = epl_scrapper_object.download_csv(dataLink)

    assert result is None

@pytest.mark.skip(reason="no done yet")
def test_successful_conversion_to_parquet(mocker):
    pass

@pytest.mark.skip(reason="no done yet")
def test_unsuccessful_conversion_to_parquet(mocker):
    pass

@pytest.mark.skip(reason="no done yet")
def test_successful_save_to_data_lake(mocker):
    pass

@pytest.mark.skip(reason="no done yet")
def test_unsuccessful_save_to_data_lake(mocker):
    pass
