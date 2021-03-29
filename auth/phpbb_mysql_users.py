#!/usr/bin/env python
# Copyright (c) 2002 Joao Prado Maia. See the LICENSE file for more information.
# $Id: phpbb_mysql_users.py,v 1.4 2003/09/19 03:11:51 jpm Exp $
import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*', DeprecationWarning, 'MySQLdb')
import MySQLdb
import settings
from hashlib import md5
import phpass

class Papercut_Auth:
    """
    Authentication backend interface for the phpBB web message board software (http://www.phpbb.com)

    This backend module tries to authenticate the users against the phpbb_users table.

    Many thanks to Chip McClure <vhm3 AT gigguardian.com> for the work on this file.
    """

    def is_valid_user(self, username, password):
        self.conn = MySQLdb.connect(host=settings.dbhost, db=settings.dbname, user=settings.dbuser, passwd=settings.dbpass)
        self.cursor = self.conn.cursor()

        stmt = """
                SELECT
                    user_password
                FROM
                    %susers
                WHERE
                    username='%s'
                """ % (settings.phpbb_table_prefix, username)
        num_rows = self.cursor.execute(stmt)
        retcode=0

        if num_rows == 0 or num_rows is None:
            settings.logEvent('Error - Authentication failed for username \'%s\' (user not found)' % (username))
        else:
            db_password = self.cursor.fetchone()[0]
            if db_password != phpass.crypt_private(password, db_password, '$H$'):
                settings.logEvent('Error - Authentication failed for username \'%s\' (incorrect password)' % (username))
            else:
                retcode=1

        self.cursor.close()
        self.conn.close()

        return retcode

