
from flask import current_app
from datetime import datetime
import MySQLdb

class Config(object):

    def __init__(self):
        self.logger = current_app.logger
        self.configuration = current_app.config
        super(Config, self).__init__()

# logging function
    def log(self, message, msgtype):
        t = datetime.now()
        str_time = t.strftime("%Y-%m-%d %H:%M:%S")
        msg = str_time + " | " + message
        if msgtype == 'error':
            self.logger.error(msg)
        else:
            self.logger.info(msg)
# db connection function

    def connect_db(self, db_name):

        db = MySQLdb.connect(host=self.configuration['db_server'], user=self.configuration['db_user'],
                             passwd=self.configuration['db_password'], db=db_name)
        return db
