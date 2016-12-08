#! /usr/bin/env python
import requests
from .utils import check_response

def upload(name,host,user,pswd,cert=None):
    url = host.strip("/") + "/rest/blob"
    files = {'blob':open(name, 'rb')}
    r = requests.post(url, files=files, auth=(user, pswd), verify=cert)
    result = check_response(r)
    return result['handle']