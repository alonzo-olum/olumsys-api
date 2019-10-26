# Author: Alonzo Fred Olum
# Description: Service for Pulling and Pushing aggregate data
# Date: 2nd Aug 2016

import os
import json
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
import MySQLdb
from flask import Flask
from flask_restful import Api
from push_API import PushMsgAPI, PushSubAPI, PushTrxAPI, PushHaiyaAPI
from consumer import CustomConsumer
from werkzeug.datastructures import CombinedMultiDict, MultiDict

# create the application object
app = Flask(__name__)
api = Api(app)

# create url routing for the PUSH API
api.add_resource(PushMsgAPI, "/push_msg")
api.add_resource(PushSubAPI, "/push_sub")
api.add_resource(PushTrxAPI, "/push_trx")
api.add_resource(PushHaiyaAPI, "/push_api")

# initialize the worker instance
c = CustomConsumer()

# logging function
def log(message, msgtype):
    t = datetime.now()
    str_time = t.strftime("%Y-%m-%d %H:%M:%S")
    msg = str_time + " | " + message
    if msgtype == 'error':
        app.logger.error(msg)
    else:
        app.logger.info(msg)

def connect_db(db_name):

    db = MySQLdb.connect(host=app.config['db_server'],
                         user=app.config['db_user'],
                         passwd=app.config['db_password'],
                         db=db_name)
    return db

def set_url(port, route):
    app.config['push_url']=("http://localhost:%d/%s" % (port, route))
    return app.config['push_url']

@app.route('/get_msg/<service>', methods=['GET'])
def get_aggregate_msg(service):
    data = {}
    data_v3 = {}
    app.config['db_name'] = dbname['emali']
    db = connect_db(app.config['db_name'])
    cur = db.cursor()

    query = " select count(*) as delivered ,s.service_name,(select count(*) from outbox o join service s on " \
            "(o.service_id = s.service_id) where s.service_name like '%"+service+"%' and date_created >= " \
            "subdate(curdate(),1)) sent,date_created, a.alert_type from outbox o inner join alert a on (o.alert_id = " \
            "a.alert_id) inner join service s on (o.service_id = s.service_id) where o.status ='DeliveredToTerminal' " \
            "and s.service_name like '%"+service+"%'and date_created >= subdate(curdate(),1)"

    query_v3 = " select count(*)as delivered, s.service_name,(select count(*) from emali_transactional.outboxes o join " \
               "emali_transactional.alerts a on (a.alerts_id = o.alerts_id) join emali_master.services s on " \
               "(a.services_id = s.services_id) where o.outboxes_id > ((select max(outboxes_id) from emali_transactional.outboxes) - 20000000) " \
               "and s.service_name like '%"+service+"%' and o.date_created >= subdate(curdate(), 1))sent, o.date_created, a.alert_type from " \
	       "emali_transactional.outboxes o join emali_transactional.alerts a on(o.alerts_id = a.alerts_id) join emali_master.services s on " \
	       "(a.services_id = s.services_id) where o.outboxes_id > ((select max(outboxes_id) from emali_transactional.outboxes) - 20000000) " \
	       "and o.STATUS = 'DELIVERED' and s.service_name like '%"+service+"%' and o.date_created >= subdate(curdate(), 1)"

    data = process_query(data, cur, query)
    data_v3 = process_query(data_v3, cur, query_v3)

    db.close()

    log("Data Captured for aggregate_messages "+str(data) + " ------ ","info")

    return str(data)

def process_query(data, cur, query):

    try:
        cur.execute(query)

        arr = cur.fetchall()

        for a in arr:
            print ("for each value in {}").format(a[0])
            data['delivered'] = a[0]
            data['serviceID'] = a[1]
            data['sent'] = a[2]
            data['dateCreated'] = a[3]
            data['message_type'] = a[4]
            data['reportDate'] = datetime.today()
            data['dateModified'] = datetime.now()

    except MySQLdb.Error, e:
        print "Data Fetch Error in get_messages .... %s" % str(e)
    finally:
        print data
        log("does the subprocess call occur "+str(data), "info")
        """
        Make Request to PUSH_API && Implement data queuing using AMQ
        """

        url = set_url(5000,"push_msg")
        # invoke the worker class
        #url = "http://localhost:5000/push_msg"
        c.delay(url, data)

    return data

@app.route('/get_sub/<service>', methods=['GET'])
def get_aggregate_subscriptions(service):
    data = {}
    global arr_sub
    app.config['db_name'] = dbname['emali']
    db = connect_db(app.config['db_name'])
    cur = db.cursor()

    try:
        cur.execute("select count(*) as user_count,s.service_name, ps.created,ps.modified from profile_service ps join"
                    " service s on (ps.service_id = s.service_id) where s.service_name like '%"+service+"%'and "
                    "ps.status = 'ACTIVE'")
        arr_sub = cur.fetchall()

    except MySQLdb.Error, e:
        print "Data Fetch Error in get_subscriptions .... %s" % str(e)
    finally:
        """
            Make Request to PUSH_API && Implement data queuing using AMQ
        """
        for a in arr_sub:
            print a[0]
            data['serviceID'] = a[1]
            data['user_count'] = a[0]
            data['aggregate_date'] = datetime.today()
            data['dateCreated'] = a[2]
            data['dateModified'] = a[3]

            url = set_url(5000, "push_sub")
            #url = "http://localhost:5000/push_sub"
            c.delay(url, data)
    db.close()
    log("Data Captured for aggregate_subscriptions "+str(data), "info")
    return str(data)

@app.route('/get_trx/<service>', methods=['GET'])
def get_transactions(service):
    dataset = {}
    mtrxdataset = {}
    atrxdataset = {}
    app.config['db_name'] = dbname['emali']
    db = connect_db(app.config['db_name'])
    cur = db.cursor()

    try:
        cur.execute("select count(*)as services,s.service_name, c.name, c.created, c.modified,s.created,s.modified from"
                    " service s join client c on (s.client_id = c.client_id) where service_name like '%"+service+"%' group by"
                    " s.service_name, c.name, c.created, c.modified, s.created, s.modified limit 1")

        arr = cur.fetchall()
        for a in arr:
            dataset['clientName'] = a[2]
            dataset['cdateCreated'] = a[3]
            dataset['cdateModified'] = a[4]
            dataset['serviceName'] = a[1]
            dataset['sdateCreated'] = a[5]
            dataset['sdateModified'] = a[6]
        
    except MySQLdb.Error, e:
        print "Data Fetch Error in get_trx | services n client .... %s " % str(e)

    try:
        
        cur.execute("select count(*) as trxs,o.status,o.date_created, o.modified from outbox o join alert a on "
                    "(o.alert_id = a.alert_id) join service s on (s.service_id = o.service_id) where  o.date_created >="
                    " curdate() and s.service_name like '%"+service+"%' and msisdn not in (select msisdn from mpesa_"
                    "transaction order by msisdn )")
        trx_arr = cur.fetchall()
        for trx in trx_arr:
	    atrxdataset['atrx_count'] = trx[0]
            atrxdataset['airtime_status'] = trx[1]
            atrxdataset['airtime_trx_type'] = "AIRTIME"
            atrxdataset['adateCreated'] = trx[2]
            atrxdataset['adateModified'] = trx[3]
        
    except MySQLdb.Error, e:
        print "Data Fetch Error in get_trx | airtime aggregate_trx .... %s " % str(e)
    try:
        
        cur.execute("select count(*) as trxs, o.status,o.date_created, o.modified from outbox o join alert a on "
                    "(o.alert_id = a.alert_id) join service s on (s.service_id = o.service_id) where o.date_created >= "
                    "subdate(curdate(),1) and s.service_name like '%"+service+"%'and msisdn in (select msisdn from "
                    "mpesa_transaction order by msisdn)")
        trx_arr = cur.fetchall()

        for trx in trx_arr:
	    mtrxdataset['mtrx_count'] = trx[0]
            mtrxdataset['mpesa_status'] = trx[1]
            mtrxdataset['mpesa_trx_type'] = "MPESA"
            mtrxdataset['mdateCreated'] = trx[2]
            mtrxdataset['mdateModified'] = trx[3]
        
    except MySQLdb.Error, e:
        print "Data Fetch Error in get_trx | mpesa aggregate_trx .... %s " % str(e)
    finally:
	dataarray = dict()
	dataarray.update(dataset)
	dataarray.update(atrxdataset)
	dataarray.update(mtrxdataset)
	#dataarray['client_service'] = dataset
	#dataarray['airtime_trx'] = atrxdataset
	#dataarray['mpesa_trx'] = mtrxdataset
        """
            Make Request to PUSH_API && Implement data queuing using AMQ
        """
        url = set_url(5000, "push_trx")#"http://localhost:5000/push_trx"
        c.delay(url, dataarray)

        db.close()
        log("Data Captured for aggregate_transactions "+str(dataarray), "info")
        return str(dataarray)

    """
    HAIYA DATASETS

    """
@app.route('/haiya_api/', methods = ['GET'])
def get_event_tickets():
    dataset = {}
    global arr_tickets
    app.config['db_name'] = dbname['haiya']
    db = connect_db(app.config['db_name'])
    cur = db.cursor()
    try:
        cur.execute("select c.companyName, e.eventTitle, sum(tS.quantity)as ticket_quantity, tS.ticketName, "
                    "sum(ts.totalCost)as ticket_sales_amount, sum(tS.ticketPrice) as ticket_price_amount, "
                    "sum(ts.quantity) as ticket_quantity_sold, t.createdAt as ticket_created_date, t.updatedAt as "
                    "ticketDateUpdated, ts.createdAt as ticket_sale_date from ticketSale ts inner join ticket t on "
                    "(t.ticketSaleID=ts.ticketSaleID) inner join ticketSetting tS on (tS.ticketSettingID="
                    "t.ticketSettingID) join event e on (tS.eventID=e.eventID) join company c on (c.companyID="
                    "e.companyID) group by tS.ticketName,e.eventTitle, c.companyName")

        arr_tickets = cur.fetchall()

    except MySQLdb.Error, e:
        print "ticket fetch error!"
    finally:
        """
        Make Request to PUSH API && Implement task Queue

        """
        for a in arr_tickets:
            dataset['companyName'] = a[0]
            dataset['eventName'] = a[1]
            dataset['ticketQuantity'] = a[2]
            dataset['ticketType'] = a[3]
            dataset['ticketSaleAmt'] = a[4]
            dataset['ticketPriceAmt'] = a[5]
            dataset['ticketQuantitySold'] = a[6]
            dataset['ticketDateCreated'] = a[7]
            dataset['ticketDateUpdated'] = a[8]
            dataset['ticketSaleDate'] = a[9]
            dataset['reportDate'] = datetime.today()

            print "group By %s" % str(a[4])
            url = set_url(5000, "push_api")  # "http://localhost:5000/push_api"
            c.delay(url, dataset)

    db.close()
    return str(dataset)

if __name__ == '__main__':
    # configurations
    dbname = dict()
    dbname['emali'] = "emali"
    dbname['haiya'] = "haiya"
    dbname['sms_leo'] = "sms_leo"
    service_port = 5000
    service_route = "push_msg"
    app.config['name'] = "RoamTech Reports Pull API"
    app.config['db_server'] ="172.19.3.3"
    app.config['db_user'] = "web_apps"
    app.config['db_password'] = "webt3chN1k"
    #app.config['push_url'] = ("http://localhost:%d/%s" % (service_port, service_route))

    """
    Setup logging
    """
    path = "/var/log/flask/roamtech/"
    app.config['info_logs'] = "info_api.log"
    app.config['error_logs'] = "error_api.log"

    if not os.path.exists(path):
        os.makedirs(path, 0777)

    info_path = path + app.config['info_logs']
    error_path = path + app.config['error_logs']

    """
    Setup the handler
    """
    handlerInfo = RotatingFileHandler(info_path, maxBytes=10000, backupCount=1)
    handlerInfo.setLevel(logging.INFO)
    handlerError = RotatingFileHandler(error_path, maxBytes=10000, backupCount=1)
    handlerError.setLevel(logging.ERROR)
    app.logger.addHandler(handlerInfo)
    app.logger.addHandler(handlerError)

# run
    app.run(port=service_port, debug=True)






