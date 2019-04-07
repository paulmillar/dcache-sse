#!/usr/bin/env python3
"""test application to demonstrate dCache inotify"""
from sseclient import SSEClient
import requests
import urllib3
import getpass
import argparse
import json

parser = argparse.ArgumentParser(description='Sample dCache SSE consumer')
parser.add_argument('--endpoint',
                    default="https://prometheus.desy.de:3880/api/v1/events",
                    help="The events endpoint.  This should be a URL like 'https://frontend.example.org:3880/api/v1/events'.")
parser.add_argument('--user', metavar="NAME", default=getpass.getuser(),
                    help="The dCache username.  Defaults to the current user's name.")
parser.add_argument('--password', default=None,
                    help="The dCache password.  Defaults to prompting the user.")
parser.add_argument('--inotify', default=None, metavar="PATH",
                    help="Subscribe to events on PATH.")
parser.add_argument('--trust', choices=['path', 'builtin', 'any'],
                    help="Which certificates to trust.", default='builtin')
parser.add_argument('--trust-path', metavar="PATH", default='/etc/grid-security/certificates',
                    help="Trust anchor location if --trust is 'path'.")
args = parser.parse_args()

user = vars(args).get("user")
pw = vars(args).get("password")
if not pw:
    pw = getpass.getpass("Please enter dCache password for user " + user + ": ")

s = requests.Session()
s.auth = (user,pw)

trust = vars(args).get("trust")
if trust == 'any':
    print("Disabling certificate verification: connection is insecure!")
    s.verify = False
    urllib3.disable_warnings()
elif trust == 'path':
    s.verify = vars(args).get("trust-path")

response = s.post(vars(args).get("endpoint") + '/channels')
channel = response.headers['Location']

print("Channel is %s" % channel)


def message(type, sub, event):
    if type == 'inotify':
        mask = event['mask']

        if 'name' in event:
            path = watches[sub] + '/' + event['name']
        else:
            path = watches[sub]

        for flag in mask:
            if flag == 'IN_ISDIR':
                path = path + '/'
            else:
                action = flag

        if action == 'IN_MOVED_FROM':
            mvFrom = path
            cookie = event['cookie']
            if cookie in mvCookie:
                mvTo = mvCookie[cookie]
                del mvCookie[cookie]
            else:
                mvCookie[cookie] = path
        else:
            mvFrom = None

        if action == 'IN_MOVED_TO':
            mvTo = path
            cookie = event['cookie']
            if cookie in mvCookie:
                mvFrom = mvCookie[cookie]
                del mvCookie[cookie]
            else:
                mvCookie[cookie] = path
        else:
            mvTo = None

        if mvFrom and mvTo:
            print("MOVE FROM %s TO %s" % (mvFrom, mvTo))
        else:
            print("%s %s %s" % (action.ljust(17), path, event))

    else:
        print("Unknown event: %s", type)
        print("    Subscription: %s", sub)
        print("    Data: %s", event)


mvCookie = {}
watches = {}

path = vars(args).get("inotify")
if path:
    r = s.post(format(channel) + "/subscriptions/inotify", json={"path" : path})
    watch = r.headers['Location']
    print("Watching %s" % path)
    watches[watch] = path

messages = SSEClient(channel, session=s)

try:
    for msg in messages:
        eventType = msg.event
        data = json.loads(msg.data)
        sub = data["subscription"]
        event = data["event"]
        message(eventType, sub, event)

except KeyboardInterrupt:
    print("Deleting channel")
    s.delete(channel)
