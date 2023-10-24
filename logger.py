class Logger():
    def __init__(self, filename):
        self.filename = filename
        self.file = open(filename, 'a')
    def Log(self, msg):
        self.file.write(msg + "\n")
    def __del__(self):
        self.file.close()