#!/usr/bin/env python
# Copyright (c) 2002, 2003, 2004 Joao Prado Maia. See the LICENSE file for more information.
# $Id: phpbb_mysql.py,v 1.20 2004/08/01 01:51:48 jpm Exp $
import warnings
warnings.filterwarnings('ignore', '.*the sets module is deprecated.*', DeprecationWarning, 'MySQLdb')
import MySQLdb
import time
#from email.header import decode_header (is totally different returning list)
from mimify import mime_decode_header
import re
import settings
from hashlib import md5
#import mime
import strutil
import random
import base64
import phpass
import email

# patch by Andreas Wegmann <Andreas.Wegmann@VSA.de> to fix the handling of unusual encodings of messages
q_quote_multiline = re.compile("=\?(.*?)\?[qQ]\?(.*?)\?=.*?=\?\\1\?[qQ]\?(.*?)\?=", re.M | re.S)

# we don't need to compile the regexps everytime..
doubleline_regexp = re.compile("^\.\.", re.M)
singleline_regexp = re.compile("^\.", re.M)
from_regexp = re.compile("^From:(.*)<(.*)>", re.M)
subject_regexp = re.compile("^Subject:(.*)", re.M)
references_regexp = re.compile("^References:(.*)<(.*)>", re.M)
lines_regexp = re.compile("^Lines:(.*)", re.M)

class Papercut_Storage:
    """
    Storage Backend interface for the Phorum web message board software (http://phorum.org)

    This is the interface for Phorum running on a MySQL database. For more information
    on the structure of the 'storage' package, please refer to the __init__.py
    available on the 'storage' sub-directory.
    """

    def __init__(self):
        self.connect()

    def __del__(self):
        self.cursor.close()
        self.conn.close();

    def get_message_body(self, headers):
        """Parses and returns the most appropriate message body possible.

        The function tries to extract the plaintext version of a MIME based
        message, and if it is not available then it returns the html version.
        """
        text= ""
        html= ""
        msg = email.message_from_string(headers)
        for part in msg.walk():
            if part.get_content_type()== 'text/plain':
                text=text+part.get_payload(decode=1)
            elif part.get_content_type()=='text/html':
                html=html+part.get_payload(decode=1)
        if text=="":
            if html=="":
                if not msg.is_multipart():
                    text=msg.get_payload()
            else:
                text=html
        return text

    def connect(self):
        self.conn = MySQLdb.connect(host=settings.dbhost, db=settings.dbname, user=settings.dbuser, passwd=settings.dbpass)
        self.cursor = self.conn.cursor()

    def query(self, sql):
        try:
            self.cursor.execute(sql)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect()
            self.cursor.execute(sql)
        return self.cursor.rowcount

    def quote_string(self, text):
        """Quotes strings the MySQL way."""
        return self.conn.escape_string(text)

    def make_bbcode_uid(self):
        random.seed()
        hash=md5()
        hash.update(str(random.random()))
        return base64.encodestring(hash.digest())[0:5]

    def encode_ip(self, dotquad_ip):
        t = dotquad_ip.split('.')
        return '%02x%02x%02x%02x' % (int(t[0]), int(t[1]), int(t[2]), int(t[3]))

    def group_exists(self, group_name):
        stmt = """
                SELECT
                    COUNT(*) AS total
                FROM
                    %sforums
                WHERE
                    LOWER(nntp_group_name)=LOWER('%s')""" % (settings.phpbb_table_prefix, group_name)
        self.query(stmt)
        return self.cursor.fetchone()[0]

    def article_exists(self, group_name, style, range):
        forum_id = self.get_forum(group_name)
        stmt = """
                SELECT
                    COUNT(*) AS total
                FROM
                    %sposts
                WHERE
                    (forum_id=%s OR forum_id=0)""" % (settings.phpbb_table_prefix, forum_id)
        if style == 'range':
            stmt = "%s AND post_id > %s" % (stmt, range[0])
            if len(range) == 2:
                stmt = "%s AND post_id < %s" % (stmt, range[1])
        else:
            stmt = "%s AND post_id = %s" % (stmt, range[0])
        self.query(stmt)
        return self.cursor.fetchone()[0]

    def get_first_article(self, group_name):
        forum_id = self.get_forum(group_name)
        stmt = """
                SELECT
                    IF(MIN(post_id) IS NULL, 0, MIN(post_id)) AS first_article
                FROM
                    %sposts
                WHERE
                    (forum_id=%s or forum_id=0)""" % (settings.phpbb_table_prefix, forum_id)
        num_rows = self.query(stmt)
        return self.cursor.fetchone()[0]

    def get_group_stats(self, group_name):
        total, max, min = self.get_forum_stats(self.get_forum(group_name))
        return (total, min, max, group_name)

    def get_forum_stats(self, forum_id):
        stmt = """
                SELECT
                   COUNT(post_id) AS total,
                   IF(MAX(post_id) IS NULL, 0, MAX(post_id)) AS maximum,
                   IF(MIN(post_id) IS NULL, 0, MIN(post_id)) AS minimum
                FROM
                    %sposts
                WHERE
                    (forum_id=%s OR forum_id=0)""" % (settings.phpbb_table_prefix, forum_id)
        num_rows = self.query(stmt)
        return self.cursor.fetchone()

    def get_forum(self, group_name):
        stmt = """
                SELECT
                    forum_id
                FROM
                    %sforums
                WHERE
                    nntp_group_name='%s'""" % (settings.phpbb_table_prefix, self.quote_string(group_name))
        self.query(stmt)
        return self.cursor.fetchone()[0]

    def get_message_id(self, msg_num, group):
        return '<%s@%s>' % (msg_num, group)

    def ip_allowed(self, user_ip):
        prefix = settings.phpbb_table_prefix
        stmt = """
                SELECT ban_id
                FROM %sbanlist
                WHERE ban_ip='%s' and ban_exclude=0
               """ % (prefix, user_ip)
        if self.query(stmt)==0:
            return 1
        return 0

    def get_poster_color(self, user_id):
        color=''
        prefix = settings.phpbb_table_prefix
        stmt = """
               SELECT group_id
               FROM %susers AS u
               WHERE u.user_id=%s
               UNION SELECT group_id
               FROM %suser_group as ug
               WHERE ug.user_id=%s
               """ % (prefix, user_id, prefix, user_id)
        if self.query(stmt)==0:
            return color

        result = self.cursor.fetchall()
        for row in result:
            if row[0]==5:
                color='AA0000'
            elif (color=='') and (row[0]==4):
                color='00AA00'

        return color

    def check_permission(self, forum_id, user_id, permission):
        prefix = settings.phpbb_table_prefix
        stmt = """
                SELECT DISTINCT f.nntp_group_name
                FROM %sforums AS f
                INNER JOIN %susers AS u
                ON u.user_id=%s
                INNER JOIN %suser_group as mg
                ON mg.user_id=u.user_id
                LEFT OUTER JOIN %sacl_users AS aa
                ON f.forum_id=aa.forum_id AND aa.user_id=u.user_id AND (aa.auth_option_id=0 OR aa.auth_setting=1)
                LEFT OUTER JOIN %sacl_groups AS ug
                ON f.forum_id=ug.forum_id AND (mg.group_id=ug.group_id OR ug.group_id=u.group_id) AND (ug.auth_option_id=0 OR ug.auth_setting=1)
                LEFT OUTER JOIN %sacl_roles_data as ard
                ON ard.auth_setting=1 AND
                  ((aa.auth_option_id=0 AND aa.auth_role_id=ard.role_id) OR
                  (ISNULL(aa.auth_setting) AND ug.auth_option_id=0 AND ug.auth_role_id=ard.role_id))
                INNER JOIN %sacl_options as ao
                ON ao.auth_option='%s' AND
                 (aa.auth_option_id=ao.auth_option_id OR ard.auth_option_id=ao.auth_option_id OR
                  (ISNULL(aa.auth_setting) AND ug.auth_option_id=ao.auth_option_id))
                WHERE f.forum_id=%s
               """ % (prefix, prefix, user_id, prefix, prefix, prefix, prefix, prefix, permission, forum_id)
        if self.query(stmt)==0:
            return 0
        return 1

    def get_NEWGROUPS(self, ts, group='%'):
        # since phpBB doesn't record when each forum was created, we have no way of knowing this...
        return None

    def get_NEWNEWS(self, ts, group='*'):
        stmt = """
                SELECT
                    nntp_group_name,
                    forum_id
                FROM
                    %sforums
                WHERE
                    nntp_group_name LIKE '%s'
                ORDER BY
                    nntp_group_name ASC""" % (settings.phpbb_table_prefix, group.replace('*', '%'))
        self.query(stmt)
        result = list(self.cursor.fetchall())
        articles = []
        for group_name, forum_id in result:
            stmt = """
                    SELECT
                        post_id
                    FROM
                        %sposts
                    WHERE
                        (forum_id=%s OR forum_id=0) AND
                        post_time >= %s""" % (settings.phpbb_table_prefix, forum_id, ts)
            num_rows = self.query(stmt)
            if num_rows == 0:
                continue
            ids = list(self.cursor.fetchall())
            for id in ids:
                articles.append("<%s@%s>" % (id, group_name))
        if len(articles) == 0:
            return ''
        else:
            return "\r\n".join(articles)

    def get_GROUP(self, group_name):
        forum_id = self.get_forum(group_name)
        result = self.get_forum_stats(forum_id)
        return (result[0], result[2], result[1])

    def get_LIST(self, username=""):
        # If the username is supplied, then find what he is allowed to see
        if len(username) > 0:
            stmt = """
                   SELECT DISTINCT f.nntp_group_name, f.forum_id, u.user_id
                   FROM %sforums AS f
                   INNER JOIN %susers AS u
                   ON u.username_clean='%s'
                   INNER JOIN %suser_group as mg
                   ON mg.user_id=u.user_id
                   LEFT OUTER JOIN %sacl_users AS aa
                   ON f.forum_id=aa.forum_id AND aa.user_id=u.user_id AND (aa.auth_option_id=0 OR aa.auth_setting=1)
                   LEFT OUTER JOIN %sacl_groups AS ug
                   ON f.forum_id=ug.forum_id AND (mg.group_id=ug.group_id OR ug.group_id=u.group_id) AND (ug.auth_option_id=0 OR ug.auth_setting=1)
                   LEFT OUTER JOIN %sacl_roles_data as ard
                   ON ard.auth_setting=1 AND
                     ((aa.auth_option_id=0 AND aa.auth_role_id=ard.role_id) OR
                     (ISNULL(aa.auth_setting) AND ug.auth_option_id=0 AND ug.auth_role_id=ard.role_id))
                   INNER JOIN %sacl_options as ao
                   ON ao.auth_option='f_list' AND
                    (aa.auth_option_id=ao.auth_option_id OR ard.auth_option_id=ao.auth_option_id OR
                     (ISNULL(aa.auth_setting) AND ug.auth_option_id=ao.auth_option_id))
                   WHERE LENGTH(f.nntp_group_name) > 0
                   ORDER BY f.nntp_group_name ASC
                   """ % (settings.phpbb_table_prefix, settings.phpbb_table_prefix, username.lower().strip(), settings.phpbb_table_prefix, settings.phpbb_table_prefix, settings.phpbb_table_prefix, settings.phpbb_table_prefix, settings.phpbb_table_prefix)
        else:
            stmt = """
                   SELECT
                       nntp_group_name,
                       forum_id
                   FROM
                       %sforums
                   WHERE
                       LENGTH(nntp_group_name) > 0
                   ORDER BY
                       nntp_group_name ASC""" % (settings.phpbb_table_prefix)
        self.query(stmt)
        result = list(self.cursor.fetchall())
        if len(result) == 0:
            return ""
        else:
            lists = []
            for group_name, forum_id, user_id in result:
                total, maximum, minimum = self.get_forum_stats(forum_id)
                if settings.server_type == 'read-only':
                    lists.append("%s %s %s n" % (group_name, maximum, minimum))
                elif self.check_permission(forum_id, user_id, 'f_post')!=0:
                    lists.append("%s %s %s y" % (group_name, maximum, minimum))
                else:
                    lists.append("%s %s %s n" % (group_name, maximum, minimum))
            return "\r\n".join(lists)

    def get_STAT(self, group_name, id):
        forum_id = self.get_forum(group_name)
        stmt = """
                SELECT
                    post_id
                FROM
                    %sposts
                WHERE
                    (forum_id=%s OR forum_id=0) AND
                    post_id=%s""" % (settings.phpbb_table_prefix, forum_id, id)
        return self.query(stmt)

    def get_ARTICLE(self, group_name, id):
        forum_id = self.get_forum(group_name)
        prefix = settings.phpbb_table_prefix
        stmt = """
                SELECT
                    A.post_id,
                    C.username,
                    C.user_email,
                    CASE WHEN A.post_subject = '' THEN CONCAT('Re: ', E.topic_title) ELSE A.post_subject END,
                    A.post_time,
                    A.post_text,
                    A.topic_id,
                    A.post_username,
                    MIN(D.post_id),
                    A.poster_ip
                FROM
                    %sposts A
                INNER JOIN
                    %sposts D
                ON
                    D.topic_id=A.topic_id
                INNER JOIN
                    %stopics E
                ON
                    A.topic_id = E.topic_id
                LEFT JOIN
                    %susers C
                ON
                    A.poster_id=C.user_id
                WHERE
                    (A.forum_id=%s OR A.forum_id=0) AND
                    A.post_id=%s
                GROUP BY
                    D.topic_id""" % (prefix, prefix, prefix, prefix, forum_id, id)
        num_rows = self.query(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchone())
        # check if there is a registered user
        if result[7] == '':
            if len(result[2]) == 0:
                author = result[1]
            else:
                #author = "%s <%s>" % (result[1], result[2])
                author = result[1]
        else:
            author = result[7]
        formatted_time = strutil.get_formatted_time(time.localtime(result[4]))
        headers = []
        headers.append("Path: %s" % (settings.nntp_hostname))
        headers.append("From: %s" % (author))
        headers.append("Newsgroups: %s" % (group_name))
        headers.append("Date: %s" % (formatted_time))
        headers.append("Subject: %s" % (result[3]))
        headers.append("Message-ID: <%s@%s>" % (result[0], group_name))
        headers.append("Xref: %s %s:%s" % (settings.nntp_hostname, group_name, result[0]))
        if result[9].strip()!='':
            headers.append("NNTP-Posting-Host: %s" % (result[9].strip()))
        if result[8] != result[0]:
            headers.append("References: <%s@%s>" % (result[8], group_name))
            headers.append("In-Reply-To: <%s@%s>" % (result[8], group_name))
        return ("\r\n".join(headers), strutil.format_body(result[5]))

    def get_LAST(self, group_name, current_id):
        forum_id = self.get_forum(group_name)
        stmt = """
                SELECT
                    post_id
                FROM
                    %sposts
                WHERE
                    post_id < %s AND
                    (forum_id=%s OR forum_id=0)
                ORDER BY
                    post_id DESC
                LIMIT 0, 1""" % (settings.phpbb_table_prefix, current_id, forum_id)
        num_rows = self.query(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_NEXT(self, group_name, current_id):
        forum_id = self.get_forum(group_name)
        stmt = """
                SELECT
                    post_id
                FROM
                    %sposts
                WHERE
                    (forum_id=%s or forum_id=0) AND
                    post_id > %s
                ORDER BY
                    post_id ASC
                LIMIT 0, 1""" % (settings.phpbb_table_prefix, forum_id, current_id)
        num_rows = self.query(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_HEAD(self, group_name, id):
        forum_id = self.get_forum(group_name)
        prefix = settings.phpbb_table_prefix
        stmt = """
                SELECT
                    A.post_id,
                    C.username,
                    C.user_email,
                    CASE WHEN A.post_subject = '' THEN CONCAT('Re: ', E.topic_title) ELSE A.post_subject END,
                    A.post_time,
                    A.topic_id,
                    A.post_username,
                    MIN(D.post_id)
                FROM
                    %sposts A
                INNER JOIN
                    %stopics E
                ON
                    A.topic_id = E.topic_id
                INNER JOIN
                    %sposts D
                ON
                    D.topic_id=A.topic_id
                LEFT JOIN
                    %susers C
                ON
                    A.poster_id=C.user_id
                WHERE
                    (A.forum_id=%s OR A.forum_id=0) AND
                    A.post_id=%s
                GROUP BY
                    D.topic_id""" % (prefix, prefix, prefix, prefix, forum_id, id)
        num_rows = self.query(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchone())
        # check if there is a registered user
        if len(result[6]) == 0 or result[6] == '':
            if len(result[2]) == 0:
                author = result[1]
            else:
                #author = "%s <%s>" % (result[1], result[2])
                author = result[1]
        else:
            author = result[6]
        formatted_time = strutil.get_formatted_time(time.localtime(result[4]))
        headers = []
        headers.append("Path: %s" % (settings.nntp_hostname))
        headers.append("From: %s" % (author))
        headers.append("Newsgroups: %s" % (group_name))
        headers.append("Date: %s" % (formatted_time))
        headers.append("Subject: %s" % (result[3]))
        headers.append("Message-ID: <%s@%s>" % (result[0], group_name))
        headers.append("Xref: %s %s:%s" % (settings.nntp_hostname, group_name, result[0]))
        # because topics are all related in forums we can only reference the first topic
        if result[7] != result[0]:
            headers.append("References: <%s@%s>" % (result[7], group_name))
            headers.append("In-Reply-To: <%s@%s>" % (result[7], group_name))
        return "\r\n".join(headers)

    def get_BODY(self, group_name, id):
        forum_id = self.get_forum(group_name)
        prefix = settings.phpbb_table_prefix
        stmt = """
                SELECT
                    A.post_text
                FROM
                    %sposts A
                WHERE
                    (A.forum_id=%s OR A.forum_id=0) AND
                    A.post_id=%s""" % (prefix, forum_id, id)
        num_rows = self.query(stmt)
        if num_rows == 0:
            return None
        else:
            return strutil.format_body(self.cursor.fetchone()[0])

    def get_XOVER(self, group_name, start_id, end_id='ggg'):
        forum_id = self.get_forum(group_name)
        prefix = settings.phpbb_table_prefix
        #print "xover startid=%s endid=%s\r\n" % (start_id, end_id)
        stmt = """
                SELECT
                    A.post_id,
                    A.topic_id,
                    C.username,
                    C.user_email,
                    CASE WHEN A.post_subject = '' THEN CONCAT('Re: ', D.topic_title) ELSE A.post_subject END,
                    A.post_time,
                    A.post_text,
                    A.post_username,
                    E.MinPostID
                FROM
                    %sposts A
                LEFT JOIN
                    (select topic_id, MIN(post_id) as MinPostID from %sposts group by topic_id) E
                ON
                    E.topic_id=A.topic_id
                LEFT JOIN
                    %susers C
                ON
                    A.poster_id=C.user_id
                LEFT JOIN
                    %stopics D
                ON
                    A.topic_id = D.topic_id
                WHERE
                    (A.forum_id=%s OR A.forum_id=0) AND
                    A.post_id >= %s""" % (prefix, prefix, prefix, prefix, forum_id, start_id)
        if end_id != 'ggg':
            stmt = "%s AND A.post_id <= %s" % (stmt, end_id)
        self.query(stmt)
        result = list(self.cursor.fetchall())
        overviews = []
        for row in result:
            if row[7] == '':
                if row[3] == '':
                    author = row[2]
                else:
                    #author = "%s <%s>" % (row[2], row[3])
                    author = row[2]
            else:
                author = row[7]
            formatted_time = strutil.get_formatted_time(time.localtime(row[5]))
            message_id = "<%s@%s>" % (row[0], group_name)
            line_count = len(row[6].split('\n'))
            xref = 'Xref: %s %s:%s' % (settings.nntp_hostname, group_name, row[0])
            if row[8] != row[0]:
                reference = "<%s@%s>" % (row[8], group_name)
            else:
                reference = ""
            # message_number <tab> subject <tab> author <tab> date <tab> message_id <tab> reference <tab> bytes <tab> lines <tab> xref
            overviews.append("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (row[0], row[4], author, formatted_time, message_id, reference, len(strutil.format_body(row[6])), line_count, xref))
        return "\r\n".join(overviews)

    def get_XPAT(self, group_name, header, pattern, start_id, end_id='ggg'):
        # XXX: need to actually check for the header values being passed as
        # XXX: not all header names map to column names on the tables
        forum_id = self.get_forum(group_name)
        prefix = settings.phpbb_table_prefix
        stmt = """
                SELECT
                    A.post_id,
                    A.topic_id,
                    C.username,
                    C.user_email,
                    CASE WHEN A.post_subject = '' THEN CONCAT('Re: ', D.topic_title) ELSE A.post_subject END,
                    A.post_time,
                    A.post_text,
                    A.post_username
                FROM
                    %sposts A
                LEFT JOIN
                    %susers C
                ON
                    A.poster_id=C.user_id
                LEFT JOIN
                    %stopics D
                ON
                    A.topic_id = D.topic_id
                WHERE
                    (A.forum_id=%s OR A.forum_id=0) AND
                    %s REGEXP '%s' AND
                    A.post_id >= %s""" % (prefix, prefix, prefix, forum_id, header, strutil.format_wildcards(pattern), start_id)
        if end_id != 'ggg':
            stmt = "%s AND A.post_id <= %s" % (stmt, end_id)
        num_rows = self.query(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchall())
        hdrs = []
        for row in result:
            if header.upper() == 'SUBJECT':
                hdrs.append('%s %s' % (row[0], row[4]))
            elif header.upper() == 'FROM':
                if row[7] == '':
                    if row[3] == '':
                        author = row[2]
                    else:
                        #author = "%s <%s>" % (row[2], row[3])
                        author = row[2]
                else:
                    author = row[7]
                # XXX: totally broken with empty values for the email address
                hdrs.append('%s %s' % (row[0], author))
            elif header.upper() == 'DATE':
                hdrs.append('%s %s' % (row[0], strutil.get_formatted_time(time.localtime(result[5]))))
            elif header.upper() == 'MESSAGE-ID':
                hdrs.append('%s <%s@%s>' % (row[0], row[0], group_name))
            elif (header.upper() == 'REFERENCES') and (row[1] != 0):
                hdrs.append('%s <%s@%s>' % (row[0], row[1], group_name))
            elif header.upper() == 'BYTES':
                hdrs.append('%s %s' % (row[0], len(row[6])))
            elif header.upper() == 'LINES':
                hdrs.append('%s %s' % (row[0], len(row[6].split('\n'))))
            elif header.upper() == 'XREF':
                hdrs.append('%s %s %s:%s' % (row[0], settings.nntp_hostname, group_name, row[0]))
        if len(hdrs) == 0:
            return ""
        else:
            return "\r\n".join(hdrs)

    def get_LISTGROUP(self, group_name):
        forum_id = self.get_forum(group_name)
        stmt = """
                SELECT
                    post_id
                FROM
                    %sposts
                WHERE
                    (forum_id=%s OR forum_id=0)
                ORDER BY
                    post_id ASC""" % (settings.phpbb_table_prefix, forum_id)
        self.query(stmt)
        result = list(self.cursor.fetchall())
        return "\r\n".join(["%s" % k for k in result])

    def get_XGTITLE(self, pattern=None):
        stmt = """
                SELECT
                    nntp_group_name,
                    forum_desc
                FROM
                    %sforums
                WHERE
                    LENGTH(nntp_group_name) > 0""" % (settings.phpbb_table_prefix)
        if pattern != None:
            stmt = stmt + """ AND
                    nntp_group_name REGEXP '%s'""" % (strutil.format_wildcards(pattern))
        stmt = stmt + """
                ORDER BY
                    nntp_group_name ASC"""
        self.query(stmt)
        result = list(self.cursor.fetchall())
        return "\r\n".join(["%s %s" % (k, v) for k, v in result])

    def get_XHDR(self, group_name, header, style, range):
        forum_id = self.get_forum(group_name)
        prefix = settings.phpbb_table_prefix
        stmt = """
                SELECT
                    A.post_id,
                    A.topic_id,
                    D.username,
                    D.user_email,
                    CASE WHEN A.post_subject = '' THEN CONCAT('Re: ', C.topic_title) ELSE A.post_subject END,
                    A.post_time,
                    A.post_text,
                    A.post_username
                FROM
                    %sposts A
                LEFT JOIN
                    %stopics C
                ON
                    A.topic_id = C.topic_id
                LEFT JOIN
                    %susers D
                ON
                    A.poster_id=D.user_id
                WHERE
                    (A.forum_id=%s OR A.forum_id=0) AND
                    """ % (prefix, prefix, prefix, forum_id)
        if style == 'range':
            stmt = '%s A.post_id >= %s' % (stmt, range[0])
            if len(range) == 2:
                stmt = '%s AND A.post_id <= %s' % (stmt, range[1])
        else:
            stmt = '%s A.post_id = %s' % (stmt, range[0])
        if self.query(stmt) == 0:
            return None
        result = self.cursor.fetchall()
        hdrs = []
        for row in result:
            if header.upper() == 'SUBJECT':
                hdrs.append('%s %s' % (row[0], row[4]))
            elif header.upper() == 'FROM':
                if row[7] == '':
                    if row[3] == '':
                        author = row[2]
                    else:
                        #author = "%s <%s>" % (row[2], row[3])
                        author = row[2]
                else:
                    author = row[7]
                hdrs.append('%s %s' % (row[0], author))
            elif header.upper() == 'DATE':
                hdrs.append('%s %s' % (row[0], strutil.get_formatted_time(time.localtime(result[5]))))
            elif header.upper() == 'MESSAGE-ID':
                hdrs.append('%s <%s@%s>' % (row[0], row[0], group_name))
            elif (header.upper() == 'REFERENCES') and (row[1] != 0):
                hdrs.append('%s <%s@%s>' % (row[0], row[1], group_name))
            elif header.upper() == 'BYTES':
                hdrs.append('%s %s' % (row[0], len(row[6])))
            elif header.upper() == 'LINES':
                hdrs.append('%s %s' % (row[0], len(row[6].split('\n'))))
            elif header.upper() == 'XREF':
                hdrs.append('%s %s %s:%s' % (row[0], settings.nntp_hostname, group_name, row[0]))
        if len(hdrs) == 0:
            return ""
        else:
            return "\r\n".join(hdrs)

    def do_POST(self, group_name, lines, ip_address, username=''):
        forum_id = self.get_forum(group_name)
        prefix = settings.phpbb_table_prefix
        # patch by Andreas Wegmann <Andreas.Wegmann@VSA.de> to fix the handling of unusual encodings of messages
        lines = mime_decode_header(re.sub(q_quote_multiline, "=?\\1?Q?\\2\\3?=", lines))
        body = self.get_message_body(lines)
        author, email = from_regexp.search(lines, 0).groups()
        subject = subject_regexp.search(lines, 0).groups()[0].strip()
        # get the authentication information now
        if username != '':
            stmt = """
                    SELECT
                        user_id
                    FROM
                        %susers
                    WHERE
                        username_clean='%s'""" % (prefix, self.quote_string(username.lower().strip()))
            num_rows = self.query(stmt)
            if num_rows == 0:
                poster_id = 0
                post_username = username
            else:
                poster_id = self.cursor.fetchone()[0]
                # use name and email provided by news client
                if email!='':
                  post_username = "%s <%s>" % (author, email)
                else:
                  post_username = author
                # post_username = ''
        else:
            poster_id = 0
            if email!='':
              post_username = "%s <%s>" % (author, email)
            else:
              post_username = author

        # check if user can post
        if self.ip_allowed(ip_address)==0:
            return 2

        if self.check_permission(forum_id, poster_id, 'f_post')==0:
            return 2

        postercolor=self.get_poster_color(poster_id)

        replying=lines.find('References') != -1

        if replying:
            # get the 'modifystamp' value from the parent (if any)
            references = references_regexp.search(lines, 0).groups()
            parent_id, void = references[-1].strip().split('@')
            stmt = """
                    SELECT
                        topic_id
                    FROM
                        %sposts
                    WHERE
                        post_id=%s
                    GROUP BY
                        post_id""" % (prefix, self.quote_string(parent_id))
            num_rows = self.query(stmt)
            if num_rows == 0:
                return None
            thread_id = self.cursor.fetchone()[0]

            # check if topic locked
            stmt = """
                    SELECT topic_status
                    FROM %stopics
                    WHERE topic_id=%s AND topic_status=0
                   """ % (prefix, thread_id)
            if self.query(stmt) == 0:
                # create new topic instead
                replying=0

        if not replying:
            # create a new topic
            stmt = """
                    INSERT INTO
                        %stopics
                    (
                        forum_id,
                        topic_title,
                        topic_poster,
                        topic_time,
                        topic_status,
                        topic_type
                    ) VALUES (
                        %s,
                        '%s',
                        %s,
                        UNIX_TIMESTAMP(),
                        0,
                        0
                    )""" % (prefix, forum_id, self.quote_string(subject), poster_id)
            self.query(stmt)
            thread_id = self.cursor.lastrowid

        stmt = """
                INSERT INTO
                    %sposts
                (
                    topic_id,
                    forum_id,
                    poster_id,
                    post_time,
                    poster_ip,
                    post_username,
                    post_subject,
                    post_text,
                    enable_magic_url,
                    enable_sig,
                    bbcode_uid
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    UNIX_TIMESTAMP(),
                    '%s',
                    '%s',
                    '%s',
                    '%s',
                    0,
                    0,
                    '%s'
                )""" % (prefix, thread_id, forum_id, poster_id, ip_address, self.quote_string(post_username), self.quote_string(subject),self.quote_string(body), self.make_bbcode_uid())
        self.query(stmt)
        new_id = self.cursor.lastrowid
        if not new_id:
            if not replying:
                # delete from 'topics' and 'posts' tables before returning...
                stmt = """
                        DELETE FROM
                            %stopics
                        WHERE
                            topic_id=%s""" % (prefix, thread_id)
                self.query(stmt)
            return None
        else:
            if replying:
                # update the total number of posts in the forum
                stmt = """
                        UPDATE
                            %sforums
                        SET
                            forum_posts=forum_posts+1,
                            forum_last_post_id=%s,
                            forum_last_poster_id=%s,
                            forum_last_post_subject='%s',
                            forum_last_post_time=UNIX_TIMESTAMP(),
                            forum_last_poster_name='%s',
                            forum_last_poster_colour='%s'
                        WHERE
                            forum_id=%s
                        """ % (prefix, new_id, poster_id, self.quote_string(subject), self.quote_string(post_username), postercolor, forum_id)
                self.query(stmt)
            else:
                # create the topics posted record
                stmt = """
                        INSERT INTO
                            %stopics_posted
                       (
                            user_id,
                            topic_id,
                            topic_posted
                       ) VALUES (
                            %s,
                            %s,
                            1
                       )""" % (prefix, poster_id, thread_id)
                self.query(stmt)


                # update the total number of topics and posts in the forum
                stmt = """
                        UPDATE
                            %sforums
                        SET
                            forum_topics=forum_topics+1,
                            forum_topics_real=forum_topics_real+1,
                            forum_posts=forum_posts+1,
                            forum_last_post_id=%s,
                            forum_last_poster_id=%s,
                            forum_last_post_subject='%s',
                            forum_last_post_time=UNIX_TIMESTAMP(),
                            forum_last_poster_name='%s',
                            forum_last_poster_colour='%s'
                        WHERE
                            forum_id=%s
                        """ % (prefix, new_id, poster_id, self.quote_string(subject), self.quote_string(post_username), postercolor, forum_id)
                self.query(stmt)
            # update the user's post count, if this is indeed a real user
            if poster_id != -1:
                stmt = """
                        UPDATE
                            %susers
                        SET
                            user_posts=user_posts+1,
                            user_lastpost_time=UNIX_TIMESTAMP()
                        WHERE
                            user_id=%s""" % (prefix, poster_id)
                self.query(stmt)
            # setup last post on the topic thread (Patricio Anguita <pda@ing.puc.cl>)
            if replying:
                incval='1'
            else:
                incval='0'

            stmt = """
                    UPDATE
                        %stopics
                    SET
                        topic_replies=topic_replies+%s,
                        topic_replies_real=topic_replies_real+%s,
                        topic_last_post_id=%s,
                        topic_last_poster_id=%s,
                        topic_last_poster_name='%s',
                        topic_last_poster_colour='%s',
                        topic_last_post_subject='%s',
                        topic_last_post_time=UNIX_TIMESTAMP()
                    WHERE
                        topic_id=%s""" % (prefix, incval, incval, new_id, poster_id, self.quote_string(post_username), postercolor, self.quote_string(subject), thread_id)
            self.query(stmt)
            # if this is the first post on the thread.. (Patricio Anguita <pda@ing.puc.cl>)
            if not replying:
                stmt = """
                        UPDATE
                            %stopics
                        SET
                            topic_first_post_id=%s,
                            topic_first_poster_name='%s',
                            topic_first_poster_colour='%s'
                        WHERE
                            topic_id=%s AND
                            topic_first_post_id=0""" % (prefix, new_id, self.quote_string(post_username), postercolor, thread_id)
                self.query(stmt)
            return 1

    def is_valid_user(self, username, password):
        stmt = """
                SELECT
                    user_password
                FROM
                    %susers
                WHERE
                    username='%s'
                """ % (settings.phpbb_table_prefix, self.quote_string(username))
        num_rows = self.query(stmt)

        if num_rows == 0 or num_rows is None:
            settings.logEvent('Error - Authentication failed for username \'%s\' (user not found)' % (username))
            return 0
        else:
            db_password = self.cursor.fetchone()[0]
            if db_password != phpass.crypt_private(password, db_password, '$H$'):
                settings.logEvent('Error - Authentication failed for username \'%s\' (incorrect password)' % (username))
                return 0
            else:
                return 1
