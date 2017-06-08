import requests
from collections import OrderedDict
from .utils import check_response


def select_all(host, user, pswd, cert=None, format=None):
    """Get all data from the MDCS server

    Inputs:
        host - string, URL of MDCS instance
        user - string, username of desired account on MDCS server
        pswd - string, password of desired account on MDCS server
        cert - string, path to authentication certificate
        format - string, format of data (can be xml or json)
    Output:
        lists where each entry is a dictionary describing entries that match the query.
        dictionaries each have the keys:
            title - title of the entry
            schema - ID of the schema that describes the entry
            content - content of the entry in either xml or json format
            _id - ID number of the entry
    """
    url = host.strip("/") + "/rest/explore/select/all"
    params = dict()
    if format:
        params['dataformat'] = format
    r = requests.get(url, params=params, auth=(user, pswd), verify=cert)
    return check_response(r)


def select(host, user, pswd, cert=None, format=None, ID=None, template=None, title=None):
    """Get all data that fits a certain simple query

    Inputs:
        host - string, URL of MDCS instance
        user - string, username of desired account on MDCS server
        pswd - string, password of desired account on MDCS server
        cert - string, path to authentication certificate
        format - string, format of data (can be xml or json)
        template - string, ID of the schema for particular data
        ID - string, ID of entry to be retrieved
        title - string, title of data to be retrieved
    Output:
        list of all entries in the database
        dictionaries each have the keys:
            title - title of the entry
            schema - ID of the schema that describes the entry
            content - content of the entry in either xml or json format
            _id - ID number of the entry
    """
    url = host.strip("/") + "/rest/explore/select"
    params = dict()
    if format:
        params['dataformat'] = format
    if ID:
        params['id'] = ID
    if template:
        params['schema'] = template
    if title:
        params['title'] = title
    r = requests.get(url, params=params, auth=(user, pswd), verify=cert)
    return check_response(r)


def delete(ID, host, user, pswd, cert=None):
    """Delete an entry

    Input:
        ID - string, ID of object to be deleted
        host - string, URL of MDCS instance
        user - string, username of desired account on MDCS server
        pswd - string, password of desired account on MDCS server
        cert - string, path to authentication certificate
    Output:
        response from MDCS
    """

    url = host.strip("/") + "/rest/explore/delete"
    params = dict()
    params['id'] = ID
    r = requests.delete(url, params=params, auth=(user, pswd), verify=cert)
    return check_response(r)


def query(host, user, pswd, cert=None, format=None, query=None, repositories=None):
    """Query by example.

    Input:
        host - string, URL of MDCS instance
        user - string, username of desired account on MDCS server
        pswd - string, password of desired account on MDCS server
        cert - string, path to authentication certificate
        format - string, format of data (can be xml or json)
        respositories - string, lists of names of other repositories to be 
    Output:
        lists where each entry is a dictionary describing entries that match the query.
        dictionaries each have the keys:
            title - title of the entry
            schema - ID of the schema that describes the entry
            content - content of the entry in either xml or json format
            _id - ID number of the entry
    """
    url = host.strip("/") + "/rest/explore/query-by-example"
    data = dict()
    if format:
        data['dataformat'] = format
    if query:
        data['query'] = query
    if repositories:
        data['repositories'] = repositories
    r = requests.post(url, data=data, auth=(user, pswd), verify=cert)
    return check_response(r)
