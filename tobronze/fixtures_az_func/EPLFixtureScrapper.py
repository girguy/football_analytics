import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import date


class EPLFixtureScrapper():
    def __init__(self, prefix_url, url, regular_expression, logger):
        """
        Initializes the FixtureScrapper class.

        Args:
        prefix_url (str): The prefix URL used in constructing complete URLs
                          for scraping.
        url (str): The specific URL to scrape data from.
        regular_expression (str): A regular expression pattern for identifying
                                  specific links in the scraped HTML.
        logger (Logger): A logging object to log messages and errors.

        The class is designed to scrape data from a given URL, particularly for
        EPL (English Premier League) fixtures for the ongoing season.
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
            req = requests.get(url)
            self._logger.info(f"Request to {self._url} succeeded")
            return req
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
            # Find the <script> tag containing 'window.location'
            script_tag = soup.find(
                'script', text=re.compile(r'window.location')
                )
            if script_tag:
                # Extract the line containing 'window.location'
                location_line = re.search(
                    self._regular_expression,
                    script_tag.string
                    )

                if location_line:
                    data_link = location_line.group(1)
                    return data_link
                else:
                    self._logger.info(
                        "data link line not found in the script tag."
                        )
                    return data_link
            else:
                self._logger.info(
                    "Script tag containing window.location not found."
                    )
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
            self._logger.info(f"{self._prefix_url}{data_link}")
            df = pd.read_csv(self._prefix_url + data_link)
            self._logger.info(f"Successfully download the EPL fixtures of {date.today()}.\n")
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
