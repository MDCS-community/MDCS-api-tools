#! /usr/bin/env python
from collections import OrderedDict
import sys

def check_response(r):
    try:
        r_content = r.json(object_pairs_hook=OrderedDict)
    except:
        r_content = r.text
    if str(r.status_code)[0] is not "2":
        print "Error: ",r.status_code
        print r.text
        sys.exit(0)
    else:
        return r_content
        
def iter_as_list(term):
    """Iterate over list representation of term"""
    if isinstance(term, (str, unicode)):
        yield term
    else:
        try:
            for t in term:
                yield t
        except:
            yield term
            
def as_list(term):
    """Return list representation of term"""
    return [t for t in iter_as_list(term)]