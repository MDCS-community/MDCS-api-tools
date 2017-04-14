#! /usr/bin/env python
import os
import requests
from .templates import current_id
from .utils import check_response

def curate(content, file_title, template_id, host, user, pswd, cert=None):

    if os.path.isfile(content):
        with open(content, 'r') as f: 
            content = f.read()
    data=dict()
    data['content']=[content]
    data['schema']=[template_id]
    data['title']=[file_title]
    
    url = host.strip("/") +  "/rest/curate"
    r = requests.post(url, data=data, auth=(user, pswd), verify=cert)
    return check_response(r)

def curate_as(file_name,file_title,host,user,pswd,cert=None,filename=None,template_title=None,content=None):
    template_id = current_id(host,user,pswd,cert=cert,filename=filename,title=template_title)
    return curate(file_name,file_title,template_id,host,user,pswd,cert=cert,content=content)