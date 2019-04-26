#!/usr/bin/env python3
"""test application to demonstrate dCache inotify"""
from sseclient import SSEClient
import requests
import urllib3
import getpass
import argparse
import json
import activities

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
parser.add_argument('--x509-trust', choices=['path', 'builtin', 'any'],
                    help="Which certificate authorities to trust when checking a server certificate.",
                    default='builtin')
parser.add_argument('--x509-trust-path', metavar="PATH", default='/etc/grid-security/certificates',
                    help="Trust anchor location if --x509-trust is 'path'.")
parser.add_argument('paths', metavar='PATH', nargs='+',
                    help='The paths to watch.')
parser.add_argument('--activity', metavar="ACTIVITY", choices=['print', 'unarchive'], default="print",
                    help='What to do with the inotify events.')
parser.add_argument('--target-path', metavar="PATH", default=None, help="The path for unarchive activity");
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

trust = vars(args).get("x509-trust")
if trust == 'any':
    print("Disabling certificate verification: connection is insecure!")
    s.verify = False
    urllib3.disable_warnings()
elif trust == 'path':
    s.verify = vars(args).get("x509-trust-path")

def request_channel():
    response = s.post(vars(args).get("endpoint") + '/events/channels')
    response.raise_for_status()
    return response.headers['Location']

eventCount = 0
activity_name = vars(args).get("activity")

if activity_name == 'print':
    activity = activities.PrintActivity()
elif activity_name == 'unarchive':
    target = vars(args).get("target_path")
    if not target:
        raise Exception('Missing --target-path argument')
    activity = activities.UnarchiveActivity(target)
else:
    raise Exception('Unknown activity: ' + activity)

def watch(path):
    "Add a watch and update watches list if successful"
    w = s.post(format(channel) + "/subscriptions/inotify",
               json={"path": path, "flags": ["IN_CLOSE_WRITE", "IN_CREATE",
                                             "IN_DELETE", "IN_DELETE_SELF",
                                             "IN_MOVE_SELF", "IN_MOVE"]})
    w.raise_for_status()
    watch = w.headers['Location']
    print("Watching %s" % path)
    watches[watch] = path

def single_watch(path):
    "Watch a single path (i.e., non-recursive)"
    try:
        watch(path)
    except requests.exceptions.HTTPError as e:
        if w.status_code == 400:
            print("Watch for path %s request failed: %s" % (path, w.json()["errors"][0]["message"]))
        else:
            print("Server rejected watch for path %s: %s" % (path, str(e)))

    except requests.exceptions.RequestException as e:
        print("Failed to watch path %s: %s" % (path, str(e)))


def watch_subdirectories(path):
    try:
        r = s.get(vars(args).get("endpoint") + "/namespace" + path + "?children=true")
        r.raise_for_status()
        dir_list = r.json()
        children = dir_list["children"]

        for item in children:
            if item["fileType"] == "DIR":
                recursive_watch(path + "/" + item["fileName"])

    except requests.exceptions.HTTPError as e:
        if w.status_code == 400:
            print("Directory listing for path %s failed: %s" % (path, w.json()["errors"][0]["message"]))
        else:
            print("Server rejected directory listing for path %s: %s" % (path, str(e)))

    except requests.exceptions.RequestException as e:
        print("Failed to list directory %s: %s" % (path, str(e)))


def recursive_watch(path):
    try:
        watch(path)
        watch_subdirectories(path)

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            print("Watch for path %s request failed: %s" % (path, e.response.json()["errors"][0]["message"]))
        else:
            print("Server rejected watch for path %s: %s" % (path, str(e)))

    except requests.exceptions.RequestException as e:
        print("Failed to watch path %s: %s" % (path, str(e)))



def checkMoveEvents():
    pop_list = []
    for cookie, (path, action, removeAt) in mvCookie.items():
        if removeAt <= eventCount:
            pop_list.append(cookie)

    for c in pop_list:
        (path,action,_) = mvCookie.pop(c)
        if action == 'IN_MOVE_FROM':
            activity.onDeletedFile(path)
        else:
            activity.onNewFile(path)

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
        single_watch(path)

    if action == 'IN_IGNORED' and isDir:
        watches.pop(path)

    if action == 'IN_MOVED_FROM':
        mvFrom = path
        cookie = event['cookie']
        if cookie in mvCookie:
            (mvTo, _, _) = mvCookie[cookie]
            del mvCookie[cookie]
            if isDir:
                activity.onMovedDirectory(mvFrom, mvTo)
            else:
                activity.onMovedFile(mvFrom, mvTo)
        else:
            mvCookie[cookie] = (path,action, eventCount+5)

    elif action == 'IN_MOVED_TO':
        mvTo = path
        cookie = event['cookie']
        if cookie in mvCookie:
            (mvFrom, _, _) = mvCookie[cookie]
            del mvCookie[cookie]
            if isDir:
                activity.onMovedDirectory(mvFrom, mvTo)
            else:
                activity.onMovedFile(mvFrom, mvTo)
        else:
            mvCookie[cookie] = (path, action, eventCount+5)
    else:
        checkMoveEvents()
        if isDir:
            if action == 'IN_CREATE':
                activity.onNewDirectory(path)
            elif action == 'IN_DELETE':
                activity.onDeletedDirectory(path)
            elif action == 'IN_IGNORED' or action == 'IN_DELETE_SELF' or action == 'IN_MOVE_SELF':
                pass
            else:
                print("Suppressing ISDIR %s %s" % (action, path))
        else:
            if action == 'IN_CLOSE_WRITE':
                activity.onNewFile(path)
            elif action == 'IN_DELETE':
                activity.onDeletedFile(path)
            elif action == 'IN_IGNORED' or action == 'IN_DELETE_SELF' or action == 'IN_CREATE':
                pass
            else:
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

channel = request_channel()

try:
    base_paths = vars(args).get("paths")
    if isRecursive:
        paths = remove_redundant_paths(base_paths)
        for path in paths:
            recursive_watch(path)
    else:
        paths = map(normalise_path, base_paths)
        for path in paths:
            single_watch(path)

    if not watches:
        exit("No watches established, exiting...")

    messages = SSEClient(channel, session=s)

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
    print("Interrupting...")

finally:
    print("Deleting channel")
    s.delete(channel)
    activity.close()
