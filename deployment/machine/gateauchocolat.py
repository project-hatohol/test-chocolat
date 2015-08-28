class QuickLogger(object):
    def info(self, msg):
        self.log("INFO", msg)

    def log(self, level, msg):
        print "%s: %s" % (level, msg)
