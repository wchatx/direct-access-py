import os
import re
import time
import json
import base64
import logging
from uuid import uuid4
from warnings import warn
from shutil import rmtree
from tempfile import mkdtemp
from collections import OrderedDict

import requests
import unicodecsv as csv
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

        if kwargs.get('logger'):
            self.logger = kwargs.pop('logger').getChild('directaccess')
        else:
            logging.basicConfig(
                level=kwargs.pop('log_level', logging.INFO),
                format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S'
            )
            self.logger = logging.getLogger('directaccess')

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
            status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def query(self, dataset, **options):
        raise NotImplementedError

    def to_csv(self, query, path, log_progress=True, **kwargs):
        """
        Write query results to CSV. Optional keyword arguments are
        provided to the csv writer object, allowing control over
        delimiters, quoting, etc. The default is comma-separated
        with csv.QUOTE_MINIMAL

        ::

            d2 = DirectAccessV2(client_id, client_secret, api_key)
            query = d2.query('rigs', deleteddate='null', pagesize=1500)
            # Write tab-separated file
            d2.to_csv(query, '/path/to/rigs.csv', delimiter='\\t')

        :param query: DirectAccessV1 or DirectAccessV2 query object
        :param path: relative or absolute filesystem path for created CSV
        :type path: str
        :param log_progress: whether to log progress. if True, log a message with current written count
        :type log_progress: bool
        :return: the newly created CSV file path
        """
        with open(path, mode='wb') as f:
            writer = csv.writer(f, **kwargs)
            count = None
            for i, row in enumerate(query, start=1):
                row = OrderedDict(sorted(row.items(), key=lambda t: t[0]))
                count = i
                if count == 1:
                    writer.writerow(row.keys())
                writer.writerow(row.values())

                if log_progress and i % 100000 == 0:
                    self.logger.info('Wrote {count} records to file {path}'.format(
                        count=count, path=path
                    ))
            self.logger.info('Completed writing CSV file to {path}. Final count {count}'.format(
                path=path, count=count
            ))
        return path


class DirectAccessV1(BaseAPI):
    """Client for Enverus Drillinginfo Developer API Version 1"""

    def __init__(self, api_key, retries=5, backoff_factor=1, **kwargs):
        """
        Enverus Drillinginfo Developer API Version 1 client

        API documentation and credentials can be found at: https://app.drillinginfo.com/direct/#/api/overview

        :param api_key: api key credential.
        :type api_key: str
        :param retries: the number of attempts when retrying failed requests with status codes of 500, 502, 503 or 504
        :type retries: int
        :param backoff_factor: the factor to use when exponentially backing off prior to retrying a failed request
        :type backoff_factor: int
        :param kwargs:
        """
        super(DirectAccessV1, self).__init__(api_key, retries, backoff_factor, **kwargs)
        self.url = self.url + '/v1/direct-access'
        if not self.api_key:
            raise DAAuthException('API KEY is required')
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
                raise DAQueryException('Query Error: {}'.format(request.text))
            if not len(response):
                break
            options['page'] = options['page'] + 1
            for record in response:
                yield record


class DirectAccessV2(BaseAPI):
    """Client for Enverus Drillinginfo Developer API Version 2"""

    def __init__(self, client_id, client_secret, api_key, retries=5, backoff_factor=1, links=None, access_token=None,
                 **kwargs):
        """
        Enverus Drillinginfo Developer API Version 2 client

        API documentation and credentials can be found at: https://app.drillinginfo.com/direct/#/api/overview

        :param client_id: client id credential.
        :type client_id: str
        :param client_secret: client secret credential.
        :type client_secret: str
        :param api_key: api key credential.
        :type api_key: str
        :param retries: the number of attempts when retrying failed requests with status codes of 500, 502, 503 or 504
        :type retries: int
        :param backoff_factor: the factor to use when exponentially backing off prior to retrying a failed request
        :type backoff_factor: int
        :param links: a dictionary of prev and next links as provided by the python-requests Session.
        See https://requests.readthedocs.io/en/master/user/advanced/#link-headers
        :type dict
        :param access_token: an optional, pregenerated access token. If provided, the class instance will not
        automatically try to request a new access token.
        :type: access_token: str
        :param kwargs:
        """
        super(DirectAccessV2, self).__init__(api_key, retries, backoff_factor, **kwargs)
        self.client_id = client_id
        self.client_secret = client_secret
        self.links = links
        self.access_token = access_token
        self.url = self.url + '/v2/direct-access'

        if self.access_token:
            self.session.headers['Authorization'] = 'bearer {}'.format(self.access_token)
        else:
            self.access_token = self.get_access_token()['access_token']

        self.session.hooks['response'].append(self._check_response)

    def _check_response(self, response, *args, **kwargs):
        """
        Check responses for errors.

        If the API returns 400, there was a problem with the provided parameters. Raise DAQueryException
        If the API returns 401, refresh access token if found and resend request.
        If the API returns 404, an invalid dataset name was provided. Raise DADatasetException

        5xx errors are handled by the session's Retry configuration

        :param response: a requests Response object
        :type response: requests.Response
        :param args:
        :param kwargs:
        :return:
        """
        if not response.ok:
            self.logger.debug('Response status code: ' + str(response.status_code))
            self.logger.debug('Response text: ' + response.text)
            if response.status_code == 400:
                raise DAQueryException(response.text)
            if response.status_code == 401:
                self.logger.warning('Access token expired. Acquiring a new one...')
                self.get_access_token()
                request = response.request
                request.headers['Authorization'] = self.session.headers['Authorization']
                return self.session.send(request)
            if response.status_code == 404:
                raise DADatasetException('Invalid dataset name provided')

    def get_access_token(self):
        """
        Get an access token from /tokens endpoint. Automatically sets the Authorization header on the class instance's
        session. Raises DAAuthException on error

        :return: token response as dict
        """
        url = self.url + '/tokens'
        if not self.api_key or not self.client_id or not self.client_secret:
            raise DAAuthException('API_KEY, CLIENT_ID and CLIENT_SECRET are required to generate an access token')
        self.session.headers['Authorization'] = 'Basic {}'.format(
            base64.b64encode(':'.join([self.client_id, self.client_secret]).encode()).decode()
        )
        self.session.headers['Content-Type'] = 'application/x-www-form-urlencoded'

        payload = {'grant_type': 'client_credentials'}
        retries = self.retries
        while True:
            response = self.session.post(url, params=payload)

            if not response.ok:
                if not retries:
                    raise DAAuthException('Error getting token. Code: {} Message: {}'.format(
                        response.status_code, response.text)
                    )
                if response.status_code == 403:
                    self.logger.warning('Throttled token request. Waiting 60 seconds...')
                    retries -= 1
                    time.sleep(60)
                    continue

            break

        self.logger.debug('Token response: ' + json.dumps(response.json(), indent=2))
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
        self.logger.debug('Retrieving DDL for dataset: ' + dataset)
        response = self.session.get(url, params=dict(ddl=database))
        return response.text

    def docs(self, dataset):
        """
        Get docs for dataset

        :param dataset: a valid dataset name. See the Direct Access documentation for valid values
        :return: docs response for dataset as list[dict] or None if ?docs is not supported on the dataset
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

    def to_dataframe(self, dataset, converters=None, log_progress=True, **options):
        """
        Write query results to a pandas Dataframe with properly set dtypes and index columns.

        This works by requesting the DDL for `dataset` and manipulating the text to build a list of dtypes, date columns
        and the index column(s). It then makes a query request for `dataset` to ensure we know the exact fields
        to expect, (ie, if `fields` was a provided query parameter and the result will have fewer fields than the DDL).

        For endpoints with composite primary keys, a pandas MultiIndex is created.

        This method is potentially fragile. The API's `docs` feature is preferable but not yet available on all
        endpoints.

        Query results are written to a temporary CSV file and then read into the dataframe. The CSV is removed
        afterwards.

        pandas version 0.24.0 or higher is required for use of the Int64 dtype allowing integers with NaN values. It is
        not possible to coerce missing values for columns of dtype bool and so these are set to `object` dtype.

        ::

            d2 = DirectAccessV2(client_id, client_secret, api_key)
            # Create a Texas permits dataframe, removing commas from Survey names and replacing the state
            # abbreviation with the complete name.
            df = d2.to_dataframe(
                dataset='permits',
                deleteddate='null',
                pagesize=100000,
                stateprovince='TX',
                converters={
                    'StateProvince': lambda x: 'TEXAS',
                    'Survey': lambda x: x.replace(',', '')
                }
            )
            df.head(10)

        :param dataset: a valid dataset name. See the Direct Access documentation for valid values
        :type dataset: str
        :param converters: Dict of functions for converting values in certain columns.
        Keys can either be integers or column labels.
        :type converters: dict
        :param log_progress: whether to log progress. if True, log a message with current written count
        :type log_progress: bool
        :param options: query parameters as keyword arguments
        :return: pandas dataframe
        """
        try:
            import pandas
        except ImportError:
            raise Exception('pandas not installed. This method requires pandas >= 0.24.0')

        ddl = self.ddl(dataset, database='mssql')
        try:
            index_col = re.findall(r'PRIMARY KEY \(([a-z0-9,]*)\)', ddl)[0].split(',')
        except IndexError:
            index_col = None
        self.logger.debug('index_col: {}'.format(index_col))
        ddl = {x.split(' ')[0]: x.split(' ')[1][:-1] for x in ddl.split('\n')[1:] if x and 'CONSTRAINT' not in x}

        date_cols = [k for k, v in ddl.items() if v == 'DATETIME']
        self.logger.debug('date columns:\n{}'.format(json.dumps(date_cols, indent=2)))

        pagesize = options.pop('pagesize')
        try:
            filter_ = OrderedDict(
                sorted(next(self.query(dataset, pagesize=1, **options)).items(), key=lambda x: x[0])
            ).keys()
            self.logger.debug(
                'Fields retrieved from query response: {}'.format(json.dumps(list(filter_), indent=2, default=str))
            )
        except StopIteration:
            raise Exception('No results returned from query')
        self.links = None
        if pagesize:
            options['pagesize'] = pagesize

        try:
            index_col = [x for x in filter_ if x.upper() in [y.upper() for y in index_col]]
            if index_col and len(index_col) == 1:
                index_col = index_col[0]
        except (IndexError, TypeError) as e:
            self.logger.warning('Could not discover index col(s): {}'.format(e))
            index_col = None
        self.logger.debug('index_col: {}'.format(index_col))

        dtypes_mapping = {
            'TEXT': 'object',
            'NUMERIC': 'float64',
            'DATETIME': 'object',
            'INT': 'Int64',
            'VARCHAR(5)': 'object'
        }
        dtypes = {k: dtypes_mapping[v] for k, v in ddl.items() if k in filter_}
        self.logger.debug('dtypes:\n{}'.format(json.dumps(dtypes, indent=2)))

        t = mkdtemp()
        self.logger.debug('Created temporary directory: ' + t)

        query = self.query(dataset, **options)
        try:
            chunks = pandas.read_csv(
                filepath_or_buffer=self.to_csv(
                    query, os.path.join(t, '{}.csv'.format(uuid4().hex)), delimiter='|', log_progress=log_progress
                ),
                sep='|',
                dtype=dtypes,
                index_col=index_col,
                parse_dates=date_cols,
                chunksize=options.get('pagesize', 100000),
                converters=converters
            )
            df = pandas.concat(chunks)
        finally:
            rmtree(t)
            self.logger.debug('Removed temporary directory')
        return df

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
                raise DAQueryException('Non-200 response: {} {}'.format(
                    response.status_code, response.text)
                )

            records = response.json()

            if not len(records):
                self.links = None
                break

            if 'next' in response.links:
                self.links = response.links

            for record in records:
                yield record
