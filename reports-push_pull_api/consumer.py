# Author: Alonzo Fred Olum
# Description: Service for Pulling and Pushing aggregate data
# Date: 2nd Aug 2016

import json
import urlparse
import requests
from datetime import datetime
from celery_config import celery
from celery import Task
from config import Config

class CustomConsumer(Task):

    # c = Config()

    def __init__(self):

        pass

    def post_data_api(self, body, url):
	body = body.copy()
        print("posting data to push API ... %s" % str(body))

        try:
	    path = urlparse.urlsplit(url)
	    response = ''
	    print( ' the path string is %s' % str(path[2]))

	    if 'push_trx' not in str(path[2]):
            	response = requests.post(url, params=body)
            	print('response as ... %s' % response.status_code)
	    else:
		response = requests.post(url, data=body)
		print('response for immutable body ... %s' % response.status_code)

            	if response.status_code == "200":
                    self.log("status code ok ... request success ... %s" % response.status_code, "info")

            	else:

                    self.log("status code not ok .... request problem ... %s" % response.status_code, "error")
        except requests.exceptions.RequestException as e:

            self.log("an exception occurred during this process ... %s" % e, "error")
        return response.status_code

    # @celery.task()
    def run(self, url, body):

        # pb = Publisher()
        # pb.publish(body)
        output = self.post_data_api(body, url)

        if output == "200":
            self.log("push data successful ... {} ".format(output), "info")
        else:
            self.log("push data  fail ... {} ".format(output), "error")

    # logging function
    def log(self, message, msgtype):
        t = datetime.now()
        str_time = t.strftime("%Y-%m-%d %H:%M:%S")
        msg = str_time + " | " + message
        if msgtype == 'error':
            #self.logger.error(msg)
            pass
        else:
            pass
            #self.logger.info(msg)

            # pickiling and unpickling logger instances
            # def __getstate__(self):
            #   d = dict(self.__dict__)
            #  print str(d)
            # del d['logger']
            # return d

            # def __setstate__(self, d):
            #   self.__dict__.update(d)

# BROKER_URL = 'amqp://{user}:{password}@{host}:{port}//'.format(user="alonzo", password="@l0n30", host="localhost",port="5672")
# celery = Celery(broker=BROKER_URL)
# celery.steps['consumer'].add(CustomConsumer)
