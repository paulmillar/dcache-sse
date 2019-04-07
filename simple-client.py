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
                    default="https://prometheus.desy.de:3880/api/v1",
                    help="The events endpoint.  This should be a URL like 'https://frontend.example.org:3880/api/v1'.")
parser.add_argument('--user', metavar="NAME", default=getpass.getuser(),
                    help="The dCache username.  Defaults to the current user's name.")
parser.add_argument('--recursive', '-r', action='store_const', const='recursive', default='single')
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
r = vars(args).get("recursive")
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

response = s.post(vars(args).get("endpoint") + '/events/channels')
channel = response.headers['Location']

eventCount = 0

def watch(path):
    w = s.post(format(channel) + "/subscriptions/inotify", json={"path" : path})
    watch = w.headers['Location']
    print("Watching %s" % path)
    watches[watch] = path

def recursive_watch(path):
    watch(path)

    r = s.get(vars(args).get("endpoint") + "/namespace" + path + "?children=true")
    dir_list = r.json()
    children = dir_list["children"]
    for item in children:
        if item["fileType"] == "DIR":
            recursive_watch(path + "/" + item["fileName"])



def moveEvent(mvFrom, mvTo):
    print("MOVE FROM %s TO %s" % (mvFrom, mvTo))

def nonMoveEvent(action, path):
    print("%s %s" % (action.ljust(17), path))

def checkMoveEvents():
    pop_list = []
    for cookie, (path, action, removeAt) in mvCookie.items():
        if removeAt <= eventCount:
            pop_list.append(cookie)

    for c in pop_list:
        (path,action,_) = mvCookie.pop(c)
        nonMoveEvent(action, path)

def message(type, sub, event):
    if type == 'inotify':
        mask = event['mask']

        if 'name' in event:
            path = watches[sub] + '/' + event['name']
        else:
            path = watches[sub]

        isDir = False
        for flag in mask:
            if flag == 'IN_ISDIR':
                isDir = True
                path = path + '/'
            else:
                action = flag

        if action == 'IN_CREATE' and isDir and vars(args).get("recursive") == 'recursive':
            path = watches[sub] + '/' + event['name']
            watch(path)

        if action == 'IN_IGNORED' and isDir:
            path = watches[sub] + '/' + event['name']
            watches.pop(path)

        if action == 'IN_MOVED_FROM':
            mvFrom = path
            cookie = event['cookie']
            if cookie in mvCookie:
                (mvTo, _, _) = mvCookie[cookie]
                del mvCookie[cookie]
                moveEvent(mvFrom, mvTo)
            else:
                mvCookie[cookie] = (path,action, eventCount+5)

        elif action == 'IN_MOVED_TO':
            mvTo = path
            cookie = event['cookie']
            if cookie in mvCookie:
                (mvFrom, _, _) = mvCookie[cookie]
                del mvCookie[cookie]
                moveEvent(mvFrom, mvTo)
            else:
                mvCookie[cookie] = (path, action, eventCount+5)
        else:
            checkMoveEvents()
            nonMoveEvent(action, path)

    else:
        print("Unknown event: %s", type)
        print("    Subscription: %s", sub)
        print("    Data: %s", event)


mvCookie = {}
watches = {}

path = vars(args).get("inotify")

if path:
    if vars(args).get("recursive") == 'recursive':
        recursive_watch(path)
    else:
        watch(path)

messages = SSEClient(channel, session=s)

try:
    for msg in messages:
        eventCount = eventCount + 1
        eventType = msg.event
        data = json.loads(msg.data)
        if eventType == "SYSTEM":
            print("SYSTEM: %s" % msg.data)
        else:
            sub = data["subscription"]
            event = data["event"]
            message(eventType, sub, event)

except KeyboardInterrupt:
    print("Deleting channel")
    s.delete(channel)
