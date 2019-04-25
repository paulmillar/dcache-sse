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
parser.add_argument('--auth', metavar="METHOD", choices=['userpw', 'x509'], default="userpw",
                    help="How to authenticate.")
parser.add_argument('--user', metavar="NAME", default=getpass.getuser(),
                    help="The dCache username.  Defaults to the current user's name.")
parser.add_argument('--recursive', '-r', action='store_const', const='recursive', default='single')
parser.add_argument('--password', default=None,
                    help="The dCache password.  Defaults to prompting the user.")
parser.add_argument('--trust', choices=['path', 'builtin', 'any'],
                    help="Which certificates to trust.", default='builtin')
parser.add_argument('--trust-path', metavar="PATH", default='/etc/grid-security/certificates',
                    help="Trust anchor location if --trust is 'path'.")
parser.add_argument('paths', metavar='PATH', nargs='+',
                    help='The paths to watch.')
args = parser.parse_args()

auth = vars(args).get("auth")
user = vars(args).get("user")
pw = vars(args).get("password")
isRecursive = vars(args).get("recursive") == 'recursive'
if auth == 'userpw' and not pw:
    pw = getpass.getpass("Please enter dCache password for user " + user + ": ")

s = requests.Session()
if auth == 'userpw':
    s.auth = (user,pw)
else:
    s.cert = '/tmp/x509up_u1000'

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
    w = s.post(format(channel) + "/subscriptions/inotify",
               json={"path": path, "flags": ["IN_CLOSE_WRITE", "IN_CREATE",
                                             "IN_DELETE", "IN_DELETE_SELF",
                                             "IN_MOVE_SELF", "IN_MOVE"]})
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

def newFileEvent(path):
    print("New file %s" %path)
    
def rmFileEvent(path):
    print("File deleted %s" % path)

def checkMoveEvents():
    pop_list = []
    for cookie, (path, action, removeAt) in mvCookie.items():
        if removeAt <= eventCount:
            pop_list.append(cookie)

    for c in pop_list:
        (path,action,_) = mvCookie.pop(c)
        if action == 'IN_MOVE_FROM':
            rmFileEvent(path)
        else:
            newFileEvent(path)

def inotify(type, sub, event):
    mask = event['mask']

    if 'name' in event:
        path = watches[sub] + '/' + event['name']
    else:
        path = watches[sub]
        if path in paths and isRecursive:
            return

    isDir = False
    for flag in mask:
        if flag == 'IN_ISDIR':
            isDir = True
        else:
            action = flag

    if action == 'IN_CREATE' and isDir and isRecursive:
        watch(path)

    if action == 'IN_IGNORED' and isDir:
        watches.pop(path)

    if isDir:
        path = path + '/'

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
        if not isDir:
            if action == 'IN_CLOSE_WRITE':
                newFileEvent(path)
            elif action == 'IN_DELETE':
                rmFileEvent(path)
            elif action != 'IN_CREATE':
                print("Suppressing %s %s" % (action, path))

mvCookie = {}
watches = {}

def normalise_path(path):
    return path if path == "/" or not path.endswith("/") else path[:-1]

def remove_redundant_paths(paths):
    "Removing any paths that are redundant"
    watches = []
    for raw_path in paths:
        path = normalise_path(raw_path)
        remove_watches = []
        for watch in watches:
            if path.startswith(watch + "/"):
                print("Skipping redundant path: %s" % path)
                break
            elif watch.startswith(path + "/"):
                print("Skipping redundant path: %s" % watch)
                remove_watches.append(watch)
        else:
            watches.append(path)
        for watch in remove_watches:
            watches.remove(watch)
    return watches

base_paths = vars(args).get("paths")

if isRecursive:
    paths = remove_redundant_paths(base_paths)
    for path in paths:
        recursive_watch(path)
else:
    paths = map(normalise_path, base_paths)
    for path in paths:
        watch(path)

messages = SSEClient(channel, session=s)

try:
    for msg in messages:
        eventCount = eventCount + 1
        eventType = msg.event
        data = json.loads(msg.data)
        if eventType == "SYSTEM":
            type = data["type"]
            if type != "NEW_SUBSCRIPTION" and type != "SUBSCRIPTION_CLOSED":
                print("SYSTEM: %s" % msg.data)
        else:
            sub = data["subscription"]
            event = data["event"]
            if eventType == 'inotify':
                inotify(eventType, sub, event)
            else:
                print("Unknown event: %s", type)
                print("    Subscription: %s", sub)
                print("    Data: %s", event)

except KeyboardInterrupt:
    print("Deleting channel")
    s.delete(channel)
