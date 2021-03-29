#!/usr/bin/env python
# Copyright (c) 2002 Joao Prado Maia. See the LICENSE file for more information.
# $Id: mime.py,v 1.1 2002/02/03 06:15:25 jpm Exp $
import re
import email

def get_body(subpart):
    doubleline_regexp = re.compile("^\.\.", re.M)
    body = []
    found = 0
    raw_headers = subpart.split('\r\n')
    for line in raw_headers:
        if not found and line == '':
            found = 1
            continue
        if found:
            body.append(doubleline_regexp.sub(".", line))
    return "\r\n".join(body)

def get_text_message(msg_string):
    msg = email.message_from_string(msg_string)
    cnt_type = msg.get_content_maintype()
    if cnt_type == 'text':
        # a simple mime based text/plain message (is this even possible?)
        body = get_body(msg_string)
    elif cnt_type == 'multipart':
        # needs to loop thru all parts and get the text version
        #print 'several parts here'
        text_parts = {}
        for part in msg.walk():
            if part.get_main_type() == 'text':
                #print 'text based part'
                #print part.as_string()
                text_parts[part.get_params()[0][0]] = get_body(part.as_string())
        if 'text/plain' in text_parts:
            return text_parts['text/plain']
        elif 'text/html' in text_parts:
            return text_parts['text/html']
    else:
        # not mime based
        body = get_body(msg_string)
    return body
