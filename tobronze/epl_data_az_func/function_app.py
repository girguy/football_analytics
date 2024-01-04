import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import HttpResponseError, ServiceRequestError
from azure.core.exceptions import ResourceNotFoundError
from azure.core.exceptions import ClientAuthenticationError, AzureError
import pandas as pd
from io import StringIO
from datetime import date, datetime, time
from EPLScrapper import EPLScrapper
import logging
import re


def create_blob_client_with_con_str(con_str):
    """
    Creates a blob service client using a given Azure connection string.

    Args:
    connection_string (str): The Azure Blob Storage connection string.

    Returns:
    BlobServiceClient: A client object to interact with the Azure Blob Storage.
    """
    con_str = re.sub(r'%2B', '+', con_str)
    blob_service_client = BlobServiceClient.from_connection_string(con_str)
    return blob_service_client


def check_blob_exist(con_str, container_name, blob_name):
    """
    Checks if a specific blob exists in Azure Blob Storage.

    This function creates a BlobClient object using the provided connection
    string, container name, and blob name. It then checks if the specified
    blob exists in the Azure Blob Storage.

    Args:
    con_str (str): The Azure Blob Storage connection string.
                   The function adjusts the connection string to replace
                   encoded plus signs with actual plus signs.

    container_name (str): The name of the container in which to check for the
                          blob.
    blob_name (str): The name of the blob to check for existence.

    Returns:
    bool: True if the blob exists, False otherwise.
    """
    con_str = re.sub(r'%2B', '+', con_str)
    blob = BlobClient.from_connection_string(
        conn_str=con_str,
        container_name=container_name,
        blob_name=blob_name
    )
    exists = blob.exists()
    return exists


def get_blob_from_container(container_client, con_str, container_name,
                            blob_name):
    """
    Retrieves a blob from a container and returns it as a DataFrame.

    This function checks if the specified blob exists in the container.
    If it exists, the blob is downloaded and read into a pandas DataFrame.
    If the blob does not exist, the function returns False.

    Args:
    container_client (ContainerClient): The client object for accessing the
                                        container.
    connection_string (str): The Azure Blob Storage connection string.
    container_name (str): The name of the container from which to
                          retrieve the blob.
    blob_name (str): The name of the blob to retrieve.

    Returns:
    pandas.DataFrame or bool: A DataFrame containing the blob data if it
                              exists, otherwise False.
    """
    blob_exists = check_blob_exist(con_str, container_name, blob_name)
    if blob_exists:
        downloaded_blob = container_client.download_blob(
            blob_name,
            encoding='utf8'
        )
        return pd.read_csv(
            StringIO(downloaded_blob.readall()),
            low_memory=False
        )
    else:
        return blob_exists


def get_season_year():
    """
    Determines the current season year based on today's date.

    This function calculates the current season year for sports leagues like
    the EPL, where the season typically starts in one calendar year and ends
    in the next. The season year is determined based on whether the current
    date falls between August 1st of the current year and January 1st of the
    following year.

    Returns:
    int: The season year. If the current date is between August and December,
         it returns the next calendar year, otherwise the current year.
    """
    today = date.today()
    today_date = today.strftime("%d_%m_%Y")  # dd_mm_YY
    year = int(re.split("_", today_date)[2])

    lower_limit_date = datetime(year, 8, 1, 0, 0)
    upper_limit_date = datetime(year + 1, 1, 1, 0, 0)

    today = datetime.combine(today, time(0, 0))

    if (today > lower_limit_date) and (today < upper_limit_date):
        return year+1
    else:
        return year


def extract_req_args(req):
    """
    Extracts and returns specific arguments from an HTTP request.

    This function retrieves several parameters from an HTTP request object.
    These parameters are typically used in operations related to Azure Blob
    Storage and web scraping tasks. The function simplifies the extraction
    process by consolidating these parameter retrievals into a single function.

    Args:
    req (HttpRequest): The HTTP request object from which to extract
                       the parameters.

    Returns:
    tuple: A tuple containing the extracted parameters in the following order:
           - connection_string (str): The Azure Blob Storage connection string.
           - container_name (str): The name of the container in
                                   Azure Blob Storage.
           - league (str): The name of the sports league
                           (e.g., 'EPL' for English Premier League).
           - prefix_url (str): The prefix URL used for web scraping tasks.
           - year (str): The year or season for which the data is
                         being retrieved.
           - url (str): The specific URL used for web scraping tasks.
    """
    # Extracting parameters from the HTTP request
    connection_string = req.params.get('connection_string')
    container_name = req.params.get('container_name')
    league = req.params.get('league')
    prefix_url = req.params.get('prefix_url')
    year = req.params.get('year')
    url = req.params.get('url')
    return connection_string, container_name, league, prefix_url, year, url


app = func.FunctionApp()


@app.route(route="HttpGetEplData", auth_level=func.AuthLevel.ANONYMOUS)
def HttpGetEplData(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HttpGetEplData Azure function is processing a GET request.')

    logger = logging.getLogger('__epl_scrapper__')
    logger.setLevel(logging.INFO)

    con_str, container_name, league, prefix_url, \
        year, url = extract_req_args(req)

    reg_expr = r'[a-zA-Z]+[0-9]+/[0-9]+/[a-zA-Z]0\.[a-zA-Z]+'
    scrapper = EPLScrapper(prefix_url, url, reg_expr, logger)
    dataLink = scrapper.scrapper()
    downloaded_file = scrapper.download_csv(dataLink)

    downloaded_file_number_of_rows = downloaded_file.shape[0]

    blob_name = '/' + league + '/' + year + '/' + league + '_' + year + '.csv'

    try:
        # Create a blob client
        blob_service_client = create_blob_client_with_con_str(con_str)
        logger.info("Successfully created blob client\n")
        container_client = blob_service_client.get_container_client(
            container=container_name
        )
        logger.info(f"Successfully got container client for {container_name} container.")

        logger.info(f"Check if blob {blob_name} exists in container {container_name}.")
        actual_blob = get_blob_from_container(
            container_client,
            con_str,
            container_name,
            blob_name
        )

        if isinstance(actual_blob, pd.DataFrame):
            logger.info("Blob exists.\n")
            actual_blob_number_of_rows = actual_blob.shape[0]
            if downloaded_file_number_of_rows > actual_blob_number_of_rows:
                logger.info("File has been updated.")
                # Upload the Parquet file
                csv_blob = downloaded_file.to_csv(encoding='utf-8')
                container_client.upload_blob(
                    name=blob_name,
                    data=csv_blob,
                    overwrite=True,
                    encoding='utf-8'
                )
                logger.info(f"Successfully uploaded {blob_name} to {container_name} container !")
                logger.info("This HTTP triggered function executed successfully !")
                return func.HttpResponse("1", status_code=200)
            else:
                logger.info("Existing blob do not need to be updated.")
                logger.info("This HTTP triggered function executed successfully !")
                return func.HttpResponse("0", status_code=200)
        else:
            logger.info(f"No blob in container {container_name} for year {year}.")
            csv_blob = downloaded_file.to_csv(encoding='utf-8')
            container_client.upload_blob(
                name=blob_name,
                data=csv_blob,
                overwrite=True,
                encoding='utf-8'
            )
            logger.info(f"Successfully uploaded {blob_name} to {container_name} container !")
            logger.info("This HTTP triggered function executed successfully !")
            return func.HttpResponse("1", status_code=200)

    except ClientAuthenticationError as e:
        logger.error(f"Failure in client authentication: {e}")
        return func.HttpResponse("0", status_code=401)

    except ResourceNotFoundError as e:
        logger.error(f"Specified container or blob does not exist. : {e}")
        return func.HttpResponse("0", status_code=404)

    except HttpResponseError as e:
        logger.error(f"Http error: {e}")
        return func.HttpResponse("0", status_code=404)

    except ServiceRequestError as e:
        logger.error(f"Issue with the request sent to the Azure service: {e}")
        return func.HttpResponse("0", status_code=400)

    except AzureError as e:
        logger.error(f"Azure error: {e}")
        return func.HttpResponse("0", status_code=400)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return func.HttpResponse("0", status_code=400)
