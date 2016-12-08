#! /usr/bin/env python
import requests
from .utils import check_response
import os

def add(filename,title,host,user,pswd,cert=None,version=None,dependencies=None):
    url = host.strip("/") + "/rest/types/add"
    
    with open(filename, 'r') as f: 
        xsd_data = f.read()
    
    data=dict()
    data['content'] = [xsd_data]
    data['filename'] = [os.path.basename(filename)]
    data['title'] = [title]
    if version: data['typeVersion'] = [version]
    if dependencies: data['dependencies[]'] = dependencies
    
    r = requests.post(url, data=data, auth=(user, pswd), verify=cert)
    return check_response(r)

def select(host,user,pswd,cert=None,ID=None,filename=None,title=None,version=None,typeVersion=None,Hash=None):
    url = host.strip("/") + "/rest/types/select"
    params = dict()
    if ID: params['id']=ID
    if filename: params['filename']=filename
    if title: params['title']=title
    if version: params['version']=version
    if typeVersion: params['typeVersion']=typeVersion
    if Hash: params['hash']=Hash
    r = requests.get(url, params=params, auth=(user, pswd), verify=cert)
    return check_response(r)

def delete(ID,host,user,pswd,cert=None,next=None):
    url = host.strip("/") + "/rest/types/delete"
    params = dict()
    params['id']=ID
    if next: params['next']=next
    r = requests.delete(url, params=params, auth=(user, pswd), verify=cert)
    return check_response(r)

def restore(ID,host,user,pswd,cert=None):
    url = host.strip("/") + "/rest/types/restore"
    params = dict()
    params['id']=ID
    r = requests.get(url, params=params, auth=(user, pswd), verify=cert)
    return check_response(r)

def select_all(host,user,pswd,cert=None):
    url = host.strip("/") + "/rest/types/select/all"
    r = requests.get(url, auth=(user, pswd), verify=cert)
    return check_response(r)

def versions_select_all(host,user,pswd,cert=None):
    url = host.strip("/") + "/rest/types/versions/select/all"
    r = requests.get(url, auth=(user, pswd), verify=cert)
    return check_response(r)

def current_id(host,user,pswd,cert=None,filename=None,title=None):
    types = select_all(host,user,pswd,cert)
    versions = versions_select_all(host,user,pswd,cert)
    
    name_matches = list()
    for t in types:
        if title == t['title']: name_matches.append(t['id'])
        if title == t['filename']: name_matches.append(t['id'])
        
    current_versions = list()
    for v in versions:
        current_versions.append(v['current'])
    
    current_list = list(set(name_matches).intersection(current_versions))
    if len(current_list) == 1:
        return current_list[0]
    else:
        return None

def select_current(host,user,pswd,cert=None):
    types = select_all(host,user,pswd,cert)
    versions = versions_select_all(host,user,pswd,cert)
    
    name_matches = list()
    for t in types:
        name_matches.append(t['id'])
        
    current_versions = list()
    for v in versions:
        current_versions.append(v['current'])
        
    current_list = list(set(name_matches).intersection(current_versions))
    
    current_types = list()
    for t in types:
        if t['id'] in current_list:
            current_types.append(t)
    
    return current_types