from collections import OrderedDict
import sys


def check_response(r):
    try:
        r_content = r.json(object_pairs_hook=OrderedDict)
    except:
        r_content = r.text
    if str(r.status_code)[0] is not "2":
        print("Error: {}".format(r.status_code))
        print(r.text)
        sys.exit(0)
    else:
        return r_content
