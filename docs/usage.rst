
Usage Guide
===========
This module is a thin wrapper around Enverus Drillinginfo's Developer API (formerly known as Direct Access).
It handles authentication and token management, pagination and network-related
error handling/retries. It also provides a simple, convenient method to write
results to CSV.

``direct-access-py`` is built and tested on Python 3.6 but should work on Python 2.7 and up.


Installation
############

The easiest way to install ``direct-access-py`` is from the `Python Package Index
<https://pypi.python.org/pypi/directaccess/>`_ using ``pip``:

.. code-block:: bash

   $ pip install directaccess

To install it manually, simply download the repository from Github:

.. code-block:: bash

   $ git clone https://github.com/wchatx/direct-access-py.git
   $ cd directaccess/
   $ python setup.py install

Notes
#####
The ``directaccess`` module only supports the JSON format from the API. The ``query`` method
returns a generator of API responses as dictionaries.

Version 2 of the API uses "soft deletes". Records marked as deleted will have a populated
``DeletedDate`` field. If these records are not important for your workflow, you should always
provide ``deleteddate='null'`` as a keyword argument to the V2 ``query`` method

It is also important to note that your API credentials should be treated like any other password.
Take care to not check them into public code repositories or expose them outside of your organization.

If you find a problem with this module, have a feature request or just need a little help getting started,
please `open an issue <https://github.com/wchatx/direct-access-py/issues/new>`_! If you're having
trouble with the Enverus Drillinginfo Developer API, you should `contact support
<mailto:support@drillinginfo.com>`_.

Quick Start
###########

Direct Access Version 1
***********************
For version 1 of the API, create an instance of the DirectAccessV1 class and provide it your API key

.. code-block:: python

  from directaccess import DirectAccessV1

  d1 = DirectAccessV1(api_key='your-api-key')

.. warning::

  Direct Access Version 1 will reach the end of its life in July, 2020.
  Please upgrade your application as Version 1 will be inaccessible after that date.
  A future version of this module will drop support for Version 1.

Provide the query method the dataset as the first argument and any query parameters as keyword arguments.
See valid dataset names and query params in the Direct Access documentation.
The query method returns a generator of API responses as dicts.

.. code-block:: python

  for row in d1.query('legal-leases', county_parish='Reeves', state_province='TX'):
      print(row)

Direct Access Version 2
***********************
For version 2 of the API, create an instance of the DirectAccessV2 class, providing it your API key, client id and client secret.
The returned access token will be available as an attribute on the instance (``d2.access_token``) and the Authorization
header is set automatically

.. code-block:: python

  from directaccess import DirectAccessV2

  d2 = DirectAccessV2(
      api_key='your-api-key',
      client_id='your-client-id',
      client_secret='your-client-secret',
  )


Like with the V1 class, provide the query method the dataset and query params. All query parameters must match the valid
parameters found in the Direct Access documentation and be passed as keyword arguments.

.. code-block:: python

	for row in d2.query('well-origins', county='REEVES', pagesize=10000):
	    print(row)


Version 2 Concepts
##################

Filter Functions
****************
Direct Access version 2 supports filter functions. These can be passed as strings on the keyword arguments.

Some common filters are greater than (``gt()``), less than (``lt()``), ``null``, not null (``not(null)``) and
between (``btw()``).
See the Direct Access documentation for a list of all available filters.

.. code-block:: python

	# Get well records updated after 2018-08-01 and without deleted dates
	for row in d2.query('well-origins', updateddate='gt(2018-08-01)', deleteddate='null'):
	    print(row)

	# Get permit records with approved dates between 2018-03-01 and 2018-06-01
	for row in d2.query('permits', approveddate='btw(2018-03-01,2018-06-01)'):
	    print(row)

Fields keyword
**************
You can use the ``fields`` keyword to limit the returned fields in your queries.
This has the benefit of limiting the API responses to only those fields needed for your
workflow and will significantly improve the speed of your queries.

.. code-block:: python

	for row in d2.query('rigs', fields='DrillType,LeaseName,PermitDepth'):
	    print(row)

Escaping
********
When making requests containing certain characters like commas, use a backslash to escape them.

.. code-block:: python

	# Escaping the comma before LLC
	for row in d2.query('producing-entities', curropername='PERCUSSION PETROLEUM OPERATING\, LLC'):
	    print(row)
