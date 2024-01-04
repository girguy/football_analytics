import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import date


class EPLScrapper():
    def __init__(self, prefix_url, url, regular_expression, logger):
        """
        Initializes the EPLScrapper class.

        Args:
        prefix_url (str): The prefix URL used in constructing complete URLs
                          for scraping.
        url (str): The specific URL to scrape data from.
        regular_expression (str): A regular expression pattern for identifying
                                  specific links in the scraped HTML.
        logger (Logger): A logging object to log messages and errors.

        The class is designed to scrape data from a given URL, particularly for
        EPL (English Premier League) related data.
        """
        self._prefix_url = prefix_url
        self._url = url
        self._regular_expression = regular_expression
        self._logger = logger

    @property
    def prefix_url(self):
        """Returns the prefix URL."""
        return self._prefix_url

    @property
    def url(self):
        """Returns the scraping URL."""
        return self._url

    @property
    def regular_expression(self):
        """Returns the regular expression used for link identification."""
        return self._regular_expression

    def request_page(self, url):
        """
        Requests a web page using the given URL.

        Args:
        url (str): The URL to request.

        Returns:
        Response: The response object from the requests.get call, or None if
                  there was an exception.
        """
        try:
            return requests.get(url)
        except requests.exceptions.RequestException as e:
            self._logger.error(f"Request to {self._url} failed: {e}")
            return None

    def scrapper(self):
        """
        Scrapes the web page for a specific link based on the
        regular expression.

        This method navigates to the URL, searches for links that match the
        regular expression, and returns the first matching link.

        Returns:
        str: The first link that matches the regular expression, or None if no
             match is found or an error occurs.
        """
        try:
            page = self.request_page(self._url)
            page.raise_for_status()  # Raise an HTTPError for bad requests
            soup = BeautifulSoup(page.content, "html.parser")

            data_link = None
            for link in soup.find_all('a', href=True):
                if re.match(self._regular_expression, link['href']):
                    data_link = link['href']
                    self._logger.info(f"Successfully extracted the dataset link {data_link} from {self._url}.")
                    break

            if data_link is None:
                self._logger.error(f"The link of the dataset could not be extracted from {self._url}.")
                return data_link
            else:
                return data_link

        except Exception as e:
            self._logger.error(f"Error during scraping from {self._url}: {e}")
            return None

    def download_csv(self, data_link):
        """
        Downloads a CSV file from the constructed URL and converts it into a
        pandas DataFrame.

        Args:
        data_link (str): The specific part of the URL where the CSV file
                         is located.

        Returns:
        DataFrame: A pandas DataFrame containing the data from the CSV file,
                   or None if an error occurs.
        """
        try:
            df = pd.read_csv(self._prefix_url + data_link)
            self._logger.info(f"Successfully download the EPL dataset of {date.today()}.")
            return df

        except pd.errors.ParserError as e:
            self._logger.error(f"Error converting {data_link} to pandas dataframe: {e}")
            return None
        except pd.errors.EmptyDataError as e:
            self._logger.error(f"Error converting {data_link} to pandas dataframe: {e}")
            return None
        except Exception as e:
            self._logger.error(f"Unexpected error while processing {data_link}: {e}")
            return None
