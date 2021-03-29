#!/usr/bin/env python
# Copyright (c) 2002 Joao Prado Maia. See the LICENSE file for more information.

import time
import sys
import os

#
# The following configuration settings should be pretty self-explanatory, but
# please let me know if this is not complete or if more information / examples
# are needed.
#

# what is the maximum number of concurrent connections that should be allowed
max_connections = 20

#
# GENERAL PATH INFORMATION
#

# full path for where Papercut will store the log file
log_path = "/var/log/papercut/"
# the actual log filename
log_file = log_path + "papercut.log"


#
# HOSTNAME / PORT OF THE SERVER
#

# hostname that Papercut will bind against
nntp_hostname = 'myhost.com'

# port for nntp server, usually 119, but use 563 for an SSL server
nntp_port = 119

# type of server ('read-only' or 'read-write')
server_type = 'read-write'


#
# NNTP AUTHENTICATION SUPPORT
#

# does the server need authentication ? ('yes' or 'no')
nntp_auth = 'yes'

# backend that Papercut will use to authenticate the users
auth_backend = 'phpbb_mysql_users'

#
# CACHE SYSTEM
#

# the cache system may need a lot of diskspace ('yes' or 'no')
nntp_cache = 'no'

# cache expire (in seconds)
nntp_cache_expire = 60 * 60 * 3

# path to where the cached files should be kept
nntp_cache_path = '/var/cache/papercut/'


#
# STORAGE MODULE
#

# backend that Papercut will use to get (and store) the actual articles content
storage_backend = "phpbb_mysql"

# database connection variables
dbhost = "localhost"
dbname = "phpbb"
dbuser = "phpbb_user"
dbpass = "phpbb_password"


#
# PHPBB STORAGE MODULE OPTIONS
#

# the prefix for the phpBB3 tables
phpbb_table_prefix = "phpbb_"


# check for the appropriate options
if nntp_auth == 'yes' and auth_backend == '':
    sys.exit("Please configure the 'nntp_auth' and 'auth_backend' options correctly")

# helper function to log information
def logEvent(msg):
    f = open(log_file, "a")
    f.write("[%s] %s\n" % (time.strftime("%a %b %d %H:%M:%S %Y", time.gmtime()), msg))
    f.close()
