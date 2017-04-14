# MDCS-api-tools

This contains a collection of tools for interacting with an instance of the 
Materials Database Curation System, MDCS. In particular, it contains Python 
scripts that allow for a user to interact with an MDCS instance outside of 
the web interface. Depending on your user preference, there are three distinct
styles of interaction to choose from:

1. The mdcs-api file provides access through a command line interface. 

2. The mdcs Python package contains an MDCS class that provides a simple and 
convenient means of interacting with a database for Python programmers.

3. The mdcs package also contains the basic convenience functions that wrap
around the REST web api commands used by the curator. Both #1 and #2 use 
these simple functions.