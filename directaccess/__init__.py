import os
import csv
import base64
import logging
from warnings import warn

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class DAAuthException(Exception):
    pass


class DAQueryException(Exception):
    pass


class DADatasetException(Exception):
    pass


class BaseAPI(object):
    url = 'https://di-api.drillinginfo.com'

    def __init__(self, api_key, retries, backoff_factor, **kwargs):
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor

        if 'logger' in kwargs and isinstance(kwargs['logger'], logging.Logger):
            self.logger = kwargs['logger']
        else:
            self.logger = logging.getLogger('direct-access-py')
            self.logger.setLevel(kwargs.pop('log_level', logging.INFO))
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s %(module)s %(levelname)-8s %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        if not self.api_key:
            raise DAAuthException('API KEY is required')

        self.session = requests.Session()
        self.session.verify = kwargs.pop('verify', True)
        self.session.proxies = kwargs.pop('proxies', {})
        self.session.headers.update({
            'X-API-KEY': self.api_key,
            'User-Agent': 'direct-access-py'
        })
        retries = Retry(
            total=self.retries,
            backoff_factor=self.backoff_factor,
            method_whitelist=frozenset(['GET', 'POST', 'HEAD']),
            status_forcelist=[403, 500, 502, 503, 504]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def query(self, dataset, **options):
        raise NotImplementedError

    def to_csv(self, query, path, log_progress=True, **kwargs):
        """
        Write query results to CSV.

        :param query: DirectAccessV1 or DirectAccessV2 query object
        :param path: filesystem path for created CSV
        :type path: str
        :param log_progress: whether to log progress
        :type log_progress: bool
        """
        with open(path, mode='w', newline='') as f:
            writer = csv.writer(f, **kwargs)
            count = None
            for i, row in enumerate(query, start=1):
                count = i
                if i == 1:
                    writer.writerow(row.keys())
                writer.writerow(row.values())

                if log_progress and i % 100000 == 0:
                    self.logger.info('Wrote {count} records to file {path}'.format(
                        count=count, path=path
                    ))
            self.logger.info('Completed writing CSV file to {path}. Final count {count}'.format(
                path=path, count=count
            ))
        return


class DirectAccessV1(BaseAPI):
    def __init__(self, api_key, retries=5, backoff_factor=1, **kwargs):
        super(DirectAccessV1, self).__init__(api_key, retries, backoff_factor, **kwargs)
        self.url = self.url + '/v1/direct-access'
        warn('DEPRECATION: Direct Access Version 1 will reach the end of its life in July, 2020. '
             'Please upgrade your application as Version 1 will be inaccessible after that date. '
             'A future version of this module will drop support for Version 1.')

    def query(self, dataset, **options):
        """
        Query Direct Access V1 dataset

        Accepts a dataset name and a variable number of keyword arguments that correspond to the fields specified in
        the 'Request Parameters' section for each dataset in the Direct Access documentation.

        This method only supports the JSON output provided by the API and yields dicts for each record.

        :param dataset: a valid dataset name. See the Direct Access documentation for valid values
        :param options: query parameters as keyword arguments
        :return:
        """
        url = self.url + '/' + dataset

        if 'page' not in options:
            options['page'] = 1
        while True:
            request = self.session.get(url, params=options)
            try:
                response = request.json()
            except ValueError:
                msg = 'Query Error: {}'.format(request.content.decode())
                self.logger.error(msg)
                raise DAQueryException(msg)
            if not len(response):
                break
            options['page'] = options['page'] + 1
            for record in response:
                yield record


class DirectAccessV2(BaseAPI):
    def __init__(self, client_id, client_secret, api_key, retries=5, backoff_factor=1, links=None, access_token=None,
                 **kwargs):
        super(DirectAccessV2, self).__init__(api_key, retries, backoff_factor, **kwargs)
        self.client_id = client_id
        self.client_secret = client_secret
        self.links = links
        self.access_token = access_token
        if not self.client_id and not self.client_secret:
            raise DAAuthException('CLIENT ID and CLIENT SECRET are required')

        self.url = self.url + '/v2/direct-access'

        if self.access_token:
            self.session.headers['Authorization'] = 'bearer {}'.format(self.access_token)
        else:
            self.access_token = self.get_access_token()['access_token']
            self.logger.debug('Access token acquired: {}'.format(self.access_token))

        self.session.hooks['response'].append(self._refresh_token_hook)

    def _refresh_token_hook(self, response, *args, **kwargs):
        """
        Check responses for 401, refresh access token if found and resend request

        :param response: a requests Response object
        :param args:
        :param kwargs:
        :return: the resent request back to calling function
        """
        if not response.ok:
            if response.status_code == 401:
                self.logger.warning('Access token expired. Acquiring a new one...')
                self.get_access_token()
                request = response.request
                request.headers['Authorization'] = self.session.headers['Authorization']
                return self.session.send(request)

    def get_access_token(self):
        """
        Get an access token from /tokens endpoint. Automatically sets the Authorization header on the class instance's
        session. Raises DAAuthException on error

        :return: token response as dict
        """
        url = self.url + '/tokens'
        self.session.headers['Authorization'] = 'Basic {}'.format(
            base64.b64encode(':'.join([self.client_id, self.client_secret]).encode()).decode()
        )
        self.session.headers['Content-Type'] = 'application/x-www-form-urlencoded'

        payload = {'grant_type': 'client_credentials'}
        response = self.session.post(url, params=payload)

        if not response.ok:
            raise DAAuthException('Error getting token. Code: {} Message: {}'.format(
                response.status_code, response.content)
            )

        self.access_token = response.json()['access_token']
        self.session.headers['Authorization'] = 'bearer {}'.format(self.access_token)
        return response.json()

    def ddl(self, dataset, database):
        """
        Get DDL statement for dataset. Must provide exactly one of mssql or pg for database argument.
        mssql is Microsoft SQL Server, pg is PostgreSQL

        :param dataset: a valid dataset name. See the Direct Access documentation for valid values
        :param database: one of mssql or pg.
        :return: a DDL statement from the Direct Access service as str
        """
        url = self.url + '/' + dataset
        if database != 'pg' and database != 'mssql':
            raise DAQueryException('Valid values for ddl database parameter are "pg" or "mssql"')
        self.logger.debug('Retrieving DDL for dataset: ' + dataset)
        response = self.session.get(url, params=dict(ddl=database))
        return response.text

    def docs(self, dataset):
        """
        Get docs for dataset

        :param dataset: a valid dataset name. See the Direct Access documentation for valid values
        :return: docs response for dataset as list[dict]
        """
        url = self.url + '/' + dataset
        self.logger.debug('Retrieving docs for dataset: ' + dataset)
        response = self.session.get(url, params=dict(docs=True))
        if response.status_code == 501:
            self.logger.warning('docs and example params are not yet supported on dataset {dataset}'.format(
                dataset=dataset
            ))
            return
        return response.json()

    def count(self, dataset, **options):
        """
        Get the count of records given a dataset and query options

        :param dataset: a valid dataset name. See the Direct Access documentation for valid values
        :param options: query parameters as keyword arguments
        :return: record count as int
        """
        url = self.url + '/' + dataset
        response = self.session.head(url, params=options)
        count = response.headers.get('X-Query-Record-Count')
        return int(count)

    def query(self, dataset, **options):
        """
        Query Direct Access V2 dataset

        Accepts a dataset name and a variable number of keyword arguments that correspond to the fields specified in
        the 'Request Parameters' section for each dataset in the Direct Access documentation.

        This method only supports the JSON output provided by the API and yields dicts for each record.

        :param dataset: a valid dataset name. See the Direct Access documentation for valid values
        :param options: query parameters as keyword arguments
        :return: query response as generator
        """
        url = self.url + '/' + dataset

        while True:
            if self.links:
                response = self.session.get(self.url + self.links['next']['url'])
            else:
                response = self.session.get(url, params=options)

            if not response.ok:
                if response.status_code == 404:
                    raise DADatasetException('Invalid dataset provided: ' + dataset)
                else:
                    raise DAQueryException('Non-200 response: {} {}'.format(
                        response.status_code, response.content.decode())
                    )

            records = response.json()

            if not len(records):
                self.links = None
                break

            if 'next' in response.links:
                self.links = response.links

            for record in records:
                yield record
