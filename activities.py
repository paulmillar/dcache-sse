from threading import Thread
from urllib.parse import urljoin
import tempfile
import os
import requests
import time
import zipfile

class BaseActivity:
    """The base class that does nothing when presented with events"""

    def onNewFile(self, path):
        pass

    def onDeletedFile(self, path):
        pass

    def onMovedFile(self, fromPath, toPath):
        pass

    def onNewDirectory(self, path):
        pass

    def onDeletedDirectory(self, path):
        pass

    def onMovedDirectory(self, fromPath, toPath):
        pass

    def close(self):
        pass


class PrintActivity(BaseActivity):
    def onNewFile(self, path):
        print("NEW FILE %s" % path)

    def onDeletedFile(self, path):
        print("DELETED FILE %s" % path)

    def onMovedFile(self, fromPath, toPath):
        print("FILE MOVED FROM %s TO %s" % (fromPath, toPath))

    def onNewDirectory(self, path):
        print("NEW DIRECTORY %s/" % path)

    def onDeletedDirectory(self, path):
        print("DELETED DIRECTORY %s/" % path)

    def onMovedDirectory(self, fromPath, toPath):
        print("DIRECTORY MOVED FROM %s/ TO %s/" % (fromPath, toPath))


class UnarchiveActivity(BaseActivity):
    """Extract newly uploaded files to a target directory"""

    def __init__(self, targetPath, configure_session):
        print("Extracting archives into %s" % targetPath)
        ## REVISIT discover webdav door URL
        self.__target_url = urljoin('https://prometheus.desy.de/', targetPath + '/');
        self.__threads = []
        self.__configure_session = configure_session

    def onNewFile(self, path):
        if path.endswith(".zip"):
            print("Extracting files from zip archive: %s" % path)
            thread = Thread(target = self.extract, args = (path,))
            thread.start()
            self.__threads.append(thread)

    def extract(self, path):
        with tempfile.TemporaryDirectory() as tmpdirname:
            with self.__configure_session() as s:

                local_archive = os.path.join(tmpdirname, 'archive.zip')

                # REVISIT discover the webdav door URL
                url = urljoin('https://prometheus.desy.de/', path)
                print("Downloading %s into %s" % (url, local_archive))
                r = s.get(url, allow_redirects=True)
                open(local_archive, 'wb').write(r.content)

                target_dir = os.path.join(tmpdirname, 'contents')
                with zipfile.ZipFile(local_archive, "r") as zip_ref:
                    zip_ref.extractall(target_dir)

                basename = os.path.basename(path) # REVISIT: shouldn't this be OS indepndent?
                target = urljoin(self.__target_url, os.path.splitext(basename)[0] + '/');

                print("basename=%s, target=%s" % (basename, target))

                for r, d, f in os.walk(target_dir):
                    for file in f:
                        abs_path = os.path.join(r, file)
                        upload_url = urljoin(target, file)

                        print("    UPLOADING %s to %s" % (abs_path, upload_url))
                        with open(abs_path, 'rb') as data:
                            s.put(upload_url, data=data)

    def close(self):
        print("Waiting for background tasks to finish")
        for thread in self.__threads:
            thread.join()
