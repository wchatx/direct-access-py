

def in_(items):
    """
    Helper for providing values to the API's `in()` filter function. Returns a comma separated string from
    an input iterable.

    The API currently supports GET requests to dataset endpoints. When providing a large list of values to the API's
    `in()` filter function, it's necessary to chunk up the values to avoid URLs larger than 2048 characters. The
    `query` method of this module handles the chunking transparently; this helper function simply stringifies
    the input items.

    ::

        d2 = DirectAccessV2(client_id, client_secret, api_key)
        # Query well-origins
        well_origins_query = d2.query(
            dataset='well-origins',
            deleteddate='null',
            pagesize=100000
        )
        # Get all UIDs for well-origins
        uid_parent_ids = [x['UID'] for x in well_origins_query]
        # Provide the UIDs to wellbores endpoint
        wellbores_query = d2.query(
            dataset='wellbores',
            deleteddate='null',
            pagesize=100000,
            uidparent=in_(uid_parent_ids)
        )

    :param items: list of values to provide to in() filter function
    :return:
    """
    return ','.join([str(x) for x in items])
