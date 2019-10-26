# Author: Alonzo Fred Olum
# Description: Service for Pulling and Pushing aggregate data
# Date: 2nd Aug 2016

from flask import request
from flask_restful import Resource
from config import Config
from datetime import datetime
import MySQLdb


class PushMsgAPI(Config, Resource):

    def __init__(self):
        super(PushMsgAPI, self).__init__()

    def post(self):

        db_name = "roamtech_reports"
        db = self.connect_db(db_name)
        print ("the values received from post {}").format(request.args.get('serviceID'))
        payload = dict()
        payload['delivered'] = request.args.get('delivered')
        payload['serviceID'] = request.args.get('serviceID')
        payload['sent'] = request.args.get('sent')
        payload['dateCreated'] = request.args.get('dateCreated')
        payload['message_type'] = request.args.get('message_type')
        payload['reportDate'] = request.args.get('reportDate')
        payload['reportDate'] = payload['reportDate'].split(' ')[0]
        payload['dateModified'] = request.args.get('dateModified')

        global serviceCode
        serviceCode = ''
        if "22554" in str(payload['serviceID']):
            serviceCode = "22554"
        elif "22677" in str(payload['serviceID']):
            serviceCode = "22677"
        elif "61026" in str(payload['serviceID']):
            serviceCode = "61026"
        elif "21026" in str(payload['serviceID']):
            serviceCode = "21026"
        elif "22567" in str(payload['serviceID']):
            serviceCode = "22567"
        elif "21735" in str(payload['serviceID']):
            serviceCode = "21735"
        elif "22229" in str(payload['serviceID']):
            serviceCode = "22229"
        elif "21650" in str(payload['serviceID']):
            serviceCode = "60040"
	elif "22088" in str(payload['serviceID']):
	    serviceCode = "22088"
        cur = db.cursor()
        try:
            cur.execute("INSERT into aggregate_messages (delivered, serviceID, sent, msg_type, dateCreated,"
                        "report_date,dateModified) values (%s, (select serviceID from services where serviceCode = '"
                        + serviceCode + "'), %s, %s, %s, %s, %s)", (payload['delivered'], payload['sent'],
                                                                    payload['message_type'], payload['dateCreated'],
                                                                    payload['reportDate'], payload['dateModified']))
            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s ...." % str(e)
        self.log("Inserted payload | aggregate_messages "+str(payload), "info")
        db.close()

class PushSubAPI(Config, Resource):

    def post(self):
        db_name = "roamtech_reports"
        db = self.connect_db(db_name)
        global service_id
        service_id = ''
        payload = dict()
        payload['serviceID'] = request.args.get('serviceID')

        if "22554" in str(payload['serviceID']):
            service_id = "22554"
        elif "22677" in str(payload['serviceID']):
            service_id = "22677"
        elif "61026" in str(payload['serviceID']):
            service_id = "61026"
        elif "21026" in str(payload['serviceID']):
            service_id = "21026"
        elif "22567" in str(payload['serviceID']):
            service_id = "22567"
        elif "21735" in str(payload['serviceID']):
            service_id = "21735"
        elif "22229" in str(payload['serviceID']):
            service_id = "22229"
        elif "21650" in str(payload['serviceID']):
            service_id = "60040"
	elif "22088" in str(payload['serviceID']):
	    service_id = "22088"

        payload['user_count'] = request.args.get('user_count')
        payload['aggregate_date'] = request.args.get('aggregate_date')
        payload['aggregate_date'] = payload['aggregate_date'].split(' ')[0]
        payload['dateCreated'] = request.args.get('dateCreated')
        # payload['dateCreated'] = payload['dateCreated'].replace(' ', '')
        payload['dateModified'] = request.args.get('dateModified')
        print ("subscriber's dateCreated as {}").format(payload['dateCreated'])
        cur = db.cursor()
        try:
            cur.execute("INSERT into aggregate_subscriptions (serviceID, user_count, aggregate_date, dateCreated, dateModified) values ((select serviceID from services where serviceCode = '" + service_id + "'),%s, %s, %s, %s)", (payload['user_count'],
                                                                      payload['aggregate_date'], payload['dateCreated'],
                                                                      payload['dateModified']))
            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)
        self.log("Inserted payload | aggregate_subscriptions "+str(payload), "info")
        db.close()

class PushTrxAPI(Config, Resource):

    def post(self):
        db_name = "roamtech_reports"
	report_date = datetime.today()
	report_date = str(report_date).split(' ')[0]
        global serviceCode
        global serviceCost
        payload = dict()
        """
            client table data
        """
        print "posted payload as %s " % str(request.values.get('clientName'))
        payload['clientName'] = request.values.get('clientName')
        payload['dateCreated'] = request.values.get('cdateCreated')
        payload['dateModified'] = request.values.get('cdateModified')

        db = self.connect_db(db_name)
        cur = db.cursor()
        try:
            cur.execute("INSERT INTO clients ( clientName, dateCreated,dateModified) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE dateCreated ='" + payload['dateCreated'] + "', dateModified ='" + payload['dateModified'] +"'",(payload['clientName'], payload['dateCreated'], payload['dateModified']))
            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)

        """
            services table data
        """
        # clientID as last inserted ID from client table
        payload['serviceName'] = request.values.get('serviceName')
        if "22554" in str(payload['serviceName']):
            serviceCode = "22554"
            serviceCost = "30"
        elif "22677" in str(payload['serviceName']):
            serviceCode = "22677"
            serviceCost = "30"
        payload['sdateCreated'] = request.values.get('sdateCreated')
        payload['sdateModified'] = request.values.get('sdateModified')

        if db is None:
            db = self.connect_db(db_name)

        cur = db.cursor()
        try:
            cur.execute("INSERT INTO services (clientID, serviceName, serviceCode, serviceCost, dateCreated, dateModified) VALUES (last_insert_id(),%s,%s, %s, %s, %s) ON DUPLICATE KEY UPDATE dateCreated = '" + payload['dateCreated'] + "', dateModified ='" + payload['dateModified'] +"'", (payload['serviceName'], serviceCode, serviceCost, payload['sdateCreated'], payload['sdateModified']))
            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)

        """
            airtime trx table data
        """
        # serviceID as last inserted ID from services table
	payload['trx_count'] = request.values.get('atrx_count')
        payload['airtime_trx'] = request.values.get('airtime_trx_type')
        # reportDate is now()
        payload['airtime_status'] = request.values.get('airtime_status')
        payload['statusDescription'] = ""
        payload['adateCreated'] = request.values.get('adateCreated')
        payload['adateModified'] = request.values.get('adateModified')

        db = self.connect_db(db_name)
        cur = db.cursor()
        try:
            cur.execute("INSERT into aggregate_transactions (service_id, transaction_type, report_date, status, statusDescription,dateCreated, dateModified,trx_count) values ((select serviceID from services where serviceCode = " + serviceCode + "),%s, %s, %s, %s, %s, %s, %s)", (payload['airtime_trx'], report_date
, payload['airtime_status'], payload['statusDescription'], payload['adateCreated'], payload['adateModified'], payload['trx_count']))
            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)

        """
            mpesa trx table data
        """
	payload['trx_count'] = request.values.get('mtrx_count')
        payload['mpesa_trx'] = request.values.get('mpesa_trx_type')
        payload['mpesa_status'] = request.values.get('mpesa_status')
        payload['mdateCreated'] = request.values.get('mdateCreated')
        payload['mdateModified'] = request.values.get('mdateModified')

        if db is None:
            db = self.connect_db(db_name)

        cur = db.cursor()
        try:
            cur.execute("INSERT into aggregate_transactions (service_id, transaction_type, report_date, status,statusDescription,dateCreated,dateModified,trx_count) values ((select serviceID from services where serviceCode = " + serviceCode + "),%s, %s, %s, %s, %s, %s, %s)", (payload['mpesa_trx'], report_date, payload['mpesa_status'], payload['statusDescription'], payload['mdateCreated'], payload['mdateModified'], payload['trx_count']))
            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)

        return


class PushHaiyaAPI(Config, Resource):
    def post(self):

        db_name = "roamtech_reports"
        payload = dict()
        payload['companyName'] = request.args.get('companyName')
        payload['eventName'] = request.args.get('eventName')
        payload['ticketQuantity'] = request.args.get('ticketQuantity')
        payload['ticketType'] = request.args.get('ticketType')
        payload['ticketSaleAmt'] = request.args.get('ticketSaleAmt')
        payload['ticketPriceAmt'] = request.args.get('ticketPriceAmt')
        payload['ticketQuantitySold'] = request.args.get('ticketQuantitySold')
        payload['ticketDateCreated'] = request.args.get('ticketDateCreated')
        payload['ticketDateUpdated'] = request.args.get('ticketDateUpdated')
        payload['ticketSaleDate'] = request.args.get('ticketSaleDate')
        payload['reportDate'] = request.args.get('reportDate')
        print "ticket_sales_amount as %s" % str(payload['ticketSaleAmt'])
        db = self.connect_db(db_name)
        cur = db.cursor()

        try:
            cur.execute(''' INSERT INTO aggregate_haiya_ticket_sales(company_name, event_name, tickets_quantity, ticket_type, ticket_sales_amount,
        report_date, date_created, dateModified, ticket_price_amount, ticket_sale_date, ticket_quantity_sold) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (payload['companyName'],
                                                                                     payload['eventName'],
                                                                                     payload['ticketQuantity'],
                                                                                     payload['ticketType'],
                                                                                     payload['ticketSaleAmt'],
                                                                                     payload['reportDate'],
                                                                                     payload['ticketDateCreated'],
                                                                                     payload['ticketDateUpdated'],
                                                                                     payload['ticketPriceAmt'],
                                                                                     payload['ticketSaleDate'],
                                                                                     payload['ticketQuantitySold']
                                                                                     ))

            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)

        self.log("Inserted payload | aggregate_haiya_tickets "+str(payload), "info")
        db.close()
        return

    def abs_connect_db(self, db_name, payload, query):

        db = self.connect_db(db_name)
        cur = db.cursor()
        try:
            cur.execute(query)
            db.commit()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)
            self.log("Inserted payload | aggregate_subscriptions "+str(payload), "info")
            db.close()
        return db

    def get_last_insert_id(self, db, query):

        cur = db.cursor()
        try:
            cur.execute(query)
            last_service_id = cur.fetchone()
        except MySQLdb.Error, e:
            print "DB Insertion fail from %s .... " % str(e)
            db.close()
        return last_service_id






