#Standard Python libraries
from __future__ import print_function
import os
import sys
import getpass

#External packages
import pandas as pd
import requests

#Relative imports
from . import blob, curate, explore, exporter, repository, saved_queries, templates, types, users, utils

class MDCS(object):
    """
    Class wrapper around the MDCS-api Python tools
    """
    
    def __init__(self, host=None, user=None, pswd=None, cert=None, records_fetch=True):
        """
        Class initializer. Stores MDCS access information and calls refresh() 
        to build local DataFrames of MDCS records, types and templates.
        
        Keyword arguments:
        host -- string url to the instance of the MDCS to access
        user -- string username for accessing the host or 
                tuple of strings for the username and password.
        pswd -- string password or file location containing only the password.
                if not given in user or here, then a prompt will ask for it. 
        cert -- string path to certification information file.
        records_fetch -- indicates if the records DataFrame should be built
                         automatically. For large databases, it may be 
                         beneficial to not build immediately and call 
                         build_records() directly after initializing.
                         Default is True.        
        """
        #Set access information
        self.host = host
        self.cert = cert
        if user is not None:
            if isinstance(user, (list, tuple)):
                assert pswd is None, 'Password cannot be given both in user and pswd'
                self.user = user
            else:
                self.user = (user, pswd)
        
        #Set default records search parameters
        self.__records_fetch =    records_fetch
        self.__records_format =   None    
        self.__records_id =       None
        self.__records_template = None
        self.__records_title =    None
        self.__records =          None
        self.__templates =        None
        self.__xsd_types =        None
        
        #Access MDCS instance and build DataFrames
        if host is not None and user is not None:
            self.refresh()
        
    @property
    def host(self):
        """The host url path"""
        return self.__host
        
    @host.setter
    def host(self, value):
        self.__host = value
        
    @property
    def user(self):
        """The username"""
        return self.__user
        
    @user.setter
    def user(self, value):
        
        if isinstance(value, (list, tuple)):
            assert len(value) == 2, 'Invalid user term'
            self.__user = value[0]
            self.__setpswd(value[1])
        else:
            self.__user = value
            self.__setpswd()
        
    @property
    def cert(self):
        """The certification file path"""
        return self.__cert
        
    @cert.setter
    def cert(self, value):
        if value is None:
            self.__cert = value
        elif os.path.isfile(value):
            self.__cert = os.path.abspath(value)
        else:
            raise ValueError('Certification file not found!')
    
    def __setpswd(self, value=None):
        url = self.host.strip("/") + "/rest/explore/select"
        
        #With no password given, check if one is needed, then prompt for one 
        if value is None:
            r = requests.get(url, auth=(self.user, None), verify=self.cert)
            if r.status_code == 401:
                i = 0
                print('Enter password for ' + self.user + ' on ' + self.host + ':')
                while i < 5:
                    pswd = getpass.getpass(prompt='password:')    
                    r = requests.get(url, auth=(self.user, pswd), verify=self.cert)
                    if r.status_code == 400:
                        self.__pswd = pswd
                        break
                    print('Invalid password')
                    i += 1
                if i == 5:
                    raise ValueError('Out of tries!')
        
        #If a file name is given, read contents as the password
        elif os.path.isfile(value):
            with open(value) as f:
                value = f.read().strip()
            self.__setpswd(value)   

        #Otherwise, set password to what is given
        else:
            pswd = str(value)
            r = requests.get(url, auth=(self.user, pswd), verify=self.cert)
            if r.status_code == 400:
                self.__pswd = pswd
            else:
                raise ValueError('Invalid username/password')
    
    def refresh(self, old=False):
        """
        (Re)build local DataFrames of xml_types, templates and records by accessing host. 
        Records is only built if it has been built before.
        """
        if old:
            self.__xsd_types = pd.DataFrame(types.select_all(    self.host, self.user, self.__pswd, cert=self.cert))
            self.__templates = pd.DataFrame(templates.select_all(self.host, self.user, self.__pswd, cert=self.cert))
        else:
            self.__xsd_types = pd.DataFrame(types.select_current(    self.host, self.user, self.__pswd, cert=self.cert))
            self.__templates = pd.DataFrame(templates.select_current(self.host, self.user, self.__pswd, cert=self.cert))
    
        if self.__records_fetch:
            self.build_records(format =   self.__records_format, 
                               id =       self.__records_id, 
                               template = self.__records_template, 
                               title =    self.__records_title)
    
    @property
    def templates(self):
        """Returns a copy of the templates DataFrame"""
        if self.__templates is None:
            self.refresh()
            
        return self.__templates.copy()
    
    @property    
    def xsd_types(self):
        """Returns a copy of the xsd_types DataFrame"""
        if self.__xsd_types is None:
            self.refresh()
        
        return self.__xsd_types.copy()
    
    @property 
    def records(self):
        """
        Returns a copy of the records DataFrame. 
        Builds it from the MDCS instance if it doesn't already exist.
        """
        if self.__records is None:
            self.build_records(format =   self.__records_format, 
                               id =       self.__records_id, 
                               template = self.__records_template, 
                               title =    self.__records_title)
            
        return self.__records.copy()
    
    def build_records(self, format=None, id=None, template=None, title=None):
        """
        Builds the records DataFrame. If called directly, can use the keyword search limiters.
        Values for limiters are stored and used until this function is directly called again.
        
        Keyword arguments:
        format -- string, format of data (can be xml or json)
        template -- string, ID of the schema for particular data
        ID -- string, ID of entry to be retrieved
        title -- string, title of data to be retrieved
        """
        
        if template is not None:
            if not isinstance(template, pd.Series):
                template = self.get_template(template)
            template = template.id
        self.__records_format =   format    
        self.__records_id =       id
        self.__records_template = template
        self.__records_title =    title
        self.__records_fetch =    True
        
        if format is None and id is None and template is None and title is None:
            self.__records = pd.DataFrame(explore.select_all(self.host,self.user,self.__pswd,cert=self.cert))
        else:
            self.__records = pd.DataFrame(explore.select(self.host,self.user,self.__pswd,cert=self.cert,
                                          format=format, ID=id, template=template, title=title))
                                         
    def get_template(self, *args, **kwargs):
        """
        Convienience function for retrieving a specific template.
        
        Arguments:
        One optional argument is allowed. If given, a limiting search is 
        performed using the template's title and filename (if not 
        specified as keywords).
        
        Keyword arguments:
        These are all optional. If any are given, the returned template must
        have matching corresponding values for the terms.
        filename -- name of the file associated with the template
        title -- name assigned to the template
        version -- version assigned to the template
        
        Returns the Series of the matching template. Issues an error if 
        exactly one matching template is not found.        
        """
    
        #Fetch optional term from args
        if   len(args) == 0: term = None
        elif len(args) == 1: term = args[0]
        else: raise ValueError('Only one non-keyword argument allowed')
        
        #Get copy of templates
        t = self.templates
        
        #Limit by specified keywords
        if 'filename' in kwargs:
            t = t[t.filename == kwargs['filename']]
        if 'title' in kwargs:
            t = t[t.title == kwargs['title']]
        if 'version' in kwargs:
            t = t[t.version == kwargs['version']]
        
        #Limit by unspecified term
        if term is not None:
            if   'filename' in kwargs and 'title' in kwargs:
                t = t[t.id == term]
            elif 'filename' in kwargs and 'title' not in kwargs: 
                t = t[(t.title == term) | (t.id == term)]
            elif 'filename' not in kwargs and 'title' in kwargs:
                t = t[(t.filename == term) | (t.id == term)]
            else:
                t = t[(t.filename == term) | (t.title == term) | (t.id == term)]
         
        if   len(t) == 1:
            return t.iloc[0]
        elif len(t) == 0:
            raise ValueError('No matching templates found')
        else:
            raise ValueError(str(len(t)) + ' matching templates found')
            
    def get_xsd_type(self, *args, **kwargs):
        """
        Convienience function for retrieving a specific xsd_type.
        
        Arguments:
        One optional argument is allowed. If given, a limiting search is 
        performed using the xsd_type's title and filename (if not 
        specified as keywords).
        
        Keyword arguments:
        These are all optional. If any are given, the returned xsd_type must
        have matching corresponding values for the terms.
        filename -- name of the file associated with the xsd_type
        title -- name assigned to the xsd_type
        version -- version assigned to the xsd_type
        
        Returns the Series of the matching xsd_type. Issues an error if 
        exactly one matching xsd_type is not found.        
        """
    
        #Fetch optional term from args
        if   len(args) == 0: term = None
        elif len(args) == 1: term = args[0]
        else: raise ValueError('Only one non-keyword argument allowed')
        
        #Get copy of xsd_types
        t = self.xsd_types
        
        #Limit by specified keywords
        if 'filename' in kwargs:
            t = t[t.filename == kwargs['filename']]
        if 'title' in kwargs:
            t = t[t.title == kwargs['title']]
        if 'version' in kwargs:
            t = t[t.version == kwargs['version']]
        
        #Limit by unspecified term
        if term is not None:
            if   'filename' in kwargs and 'title' in kwargs:
                t = t[t.id == term]
            elif 'filename' in kwargs and 'title' not in kwargs: 
                t = t[(t.title == term) | (t.id == term)]
            elif 'filename' not in kwargs and 'title' in kwargs:
                t = t[(t.filename == term) | (t.id == term)]
            else:
                t = t[(t.filename == term) | (t.title == term) | (t.id == term)]
         
        if   len(t) == 1:
            return t.iloc[0]
        elif len(t) == 0:
            raise ValueError('No matching xsd_types found')
        else:
            raise ValueError(str(len(t)) + ' matching xsd_types found')
    
    def get_record(self, title=None, template=None):
        """
        Convienience function for retrieving a specific record.
        
        Keyword arguments:
        title -- title (or _id) assigned to the record
        template -- schema assigned to the record. 
        
        Returns the Series of the matching record. Issues an error if 
        exactly one matching record is not found.       
        """
    
        #Get copy of records
        r = self.records
        
        #Limit by specified keywords
        if title is not None:
            r = r[(r.title == title) | (r._id == title)]
        if template is not None:
            template = self.get_template(template)
            r = r[r.schema == template.id]
         
        if   len(r) == 1:
            return r.iloc[0]
        elif len(r) == 0:
            raise ValueError('No matching records found')
        else:
            raise ValueError(str(len(r)) + ' matching records found')    
    
    def add_template(self, filename, title=None, version=None, dependencies=None, refresh=True):
        """Add template to the MDCS instance"""        
        
        #Get absolute path
        filename = os.path.abspath(filename)
        
        #Set title to match file's base name if title not given
        if title is None: 
            title = os.path.splitext(os.path.basename(filename))[0]
        
        #Get ids for any xsd_type dependencies
        if dependencies is not None:
            dep_ids = []
            for d in utils.as_list(dependencies):
                if not isinstance(d, pd.Series):
                    d = self.get_xsd_type(d)
                dep_ids.append(d.id)
            dependencies = ','.join(dep_ids)
        
        #Check if title matches an existing template
        t = self.templates
        if version is None:
            check = len(t[t.title == title])
        else:
            check = len(t[(t.title == title) & (t.version == version)])
            
        if check > 0:
            print('Matching template title and version already in curator')
        else:
            #Add template and refresh
            print("uploading " + title)
            templates.add(filename, title, self.host, self.user, self.__pswd, cert=self.cert, version=version, dependencies=dependencies)
            if refresh: self.refresh()

    def add_xsd_type(self, filename, title=None, version=None, dependencies=None, refresh=True):
        """Add xsd_type to the MDCS instance"""        
        
        #Get absolute path
        filename = os.path.abspath(filename)
        
        #Set title to match file's base name if title not given
        if title is None: 
            title = os.path.splitext(os.path.basename(filename))[0]
        
        #Get ids for any xsd_type dependencies
        if dependencies is not None:
            dep_ids = []
            for d in utils.as_list(dependencies):
                if not isinstance(d, pd.Series):
                    d = self.get_xsd_type(d)
                dep_ids.append(d.id)
            dependencies = ','.join(dep_ids)
        
        #Check if title matches an existing xsd_type
        t = self.xsd_types
        if version is None:
            check = len(t[t.title == title])
        else:
            check = len(t[(t.title == title) & (t.version == version)])
            
        if check > 0:
            print('Matching xsd_type title and version already in curator')
        else:
            #Add xsd_type and refresh 
            print('uploading ' + title)
            types.add(filename, title, self.host, self.user, self.__pswd, cert=self.cert, version=version, dependencies=dependencies)
            if refresh: self.refresh()        
        
    def add_record(self, content, title, template, refresh=True):
        """Add a record to the MDCS instance"""

        if not isinstance(template, pd.Series):
            template = self.get_template(template)
        curate(content, title, template.id, self.host, self.user, self.__pswd, cert=self.cert)
        
        if refresh: self.refresh()
    
    def delete_template(self, template, next=None, refresh=True):
        """Deletes a template"""
        
        if not isinstance(template, pd.Series):
            template = self.get_template(template)
        if next is not None: 
            if not isinstance(next, pd.Series):
                next = self.get_template(next)
        templates.delete(template.id, self.host, self.user, self.__pswd, cert=self.cert, next=next.id)
        
        if refresh: self.refresh()
    
    def restore_template(self, template, refresh=True):
        """Restores a template"""
        
        if not isinstance(template, pd.Series):
            template = self.get_template(template)
        template.restore(template.id, self.host, self.user, self.__pswd, cert=self.cert)
        
        if refresh: self.refresh()

    def delete_xsd_type(self, xsd_type, next=None, refresh=True):
        """Deletes an xsd_type"""
        
        if not isinstance(xsd_type, pd.Series):
            xsd_type = self.get_xsd_type(xsd_type)
        if next is not None: 
            if not isinstance(next, pd.Series):
                next = get_xsd_type(next)
        types.delete(xsd_type.id, self.host, self.user, self.__pswd, cert=self.cert, next=next.id)
        
        if refresh: self.refresh()
    
    def restore_xsd_type(self, xsd_type, refresh=True):
        """Restores an xsd_type"""
        
        if not isinstance(xsd_type, pd.Series):
            xsd_type = self.get_xsd_type(xsd_type)
        types.restore(xsd_type.id, self.host, self.user, self.__pswd, cert=self.cert)
        
        if refresh: self.refresh()
        
    def delete_record(self, record, refresh=True):
        """Deletes a record"""
        
        if not isinstance(record, pd.Series):
            record = self.get_record(record)
        explore.delete(record._id, self.host, self.user, self.__pswd, cert=self.cert)      

        if refresh: self.refresh()        

    def update_record(self, record, content, refresh=True):
        """Deletes an existing record and saves new content to that title and template"""
        
        if not isinstance(record, pd.Series):
            record = self.get_record(record)
        
        explore.delete(record._id, self.host, self.user, self.__pswd, cert=self.cert)
        curate(content, record.title, record.schema, self.host, self.user, self.__pswd, cert=self.cert)
        
        if refresh: self.refresh()    
        
    def add_file(self, filename):
        return blob.upload(filename, self.host, self.user, self.__pswd, cert=self.cert)
    
    def get_file(self, url):
        return blob.download(url, self.user, self.__pswd, cert=self.cert)
        
        
        