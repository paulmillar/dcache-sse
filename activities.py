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
