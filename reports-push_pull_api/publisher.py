from amqplib import client_0_8 as amqp
from config import Config
import json


class Publisher(Config):

    def get_msg_queue_message(self, message):

        message = {"Delivered": message.get('delivered'),
                   "serviceID": message.get('serviceID'),
                   "sent": message.get('sent'),
                   "dateCreated": message.get('dateCreated'),
                   "message_type": message.get('message_type'),
                   "reportDate": message.get('reportDate'),
                   "dateModified": message.get("dateModified")}
        return message

    def get_sub_queue_message(self, message):

        message = {"serviceID": message.get('serviceID'),
                   "user_count": message.get('user_count'),
                   "aggregate_date": message.get('aggregate_date'),
                   "dateCreated": message.get('dateCreated'),
                   "dateModified": message.get('dateModified')}
        return message

    def get_trx_queue_message(self, message):

        message = {"clientName": message.get('clientName'),
                   "cdateCreated": message.get('cdateCreated'),
                   "cdateModified": message.get('cdateModified'),
                   "serviceName": message.get('serviceName'),
                   "sdateCreated": message.get('sdateCreated'),
                   "sdateModified": message.get('sdateModified'),
                   "airtime_status": message.get('airtime_status'),
                   "airtime_trx_type": message.get('airtime_trx_type'),
                   "adateCreated": message.get('adateCreated'),
                   "adateModified": message.get('adateModified'),
                   "mpesa_status": message.get('mpesa_status'),
                   "mpesa_trx_type": message.get('mpesa_trx_type'),
                   "mdateCreated": message.get('mdateCreated'),
                   "mdateModified": message. get('mdateModified')}
        return message

    def publish(self, message):

        self.log("FOUND MESSAGE POSTING TO Q ... %s" % message, "info")
        message = self.get_msg_queue_message(message)
        try:
            conn = amqp.Connection(host="localhost:5672", userid="alonzo", password="@l0n30", virtual_host="/",
                                   insist=False)
        except Exception, e:
            self.log("failure in initiating connection %e" % e, "error")
            return

        self.log("Connection to rabbitMQ initiated ....", "info")
        try:
            ch = conn.channel()
            ch.queue_declare(queue="MSG_QUEUE", durable=True, exclusive=False, auto_delete=False)
            ch.exchange_declare(exchange="MSG_QUEUE", type="direct", durable=True, auto_delete=False)
            ch.queue_bind(queue="MSG_QUEUE", exchange="MSG_QUEUE", routing_key="MSG_QUEUE")
            msg = amqp.Message(json.dumps(message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 2

            ch.basic_publish(exchange="MSG_QUEUE", routing_key="MSG_QUEUE", msg=msg)
            self.log("publish successful ...", "info")
        except Exception, e:
            self.log("Error attempting to publish to Rabbit: %r " % e, "error")
            conn.close()
        else:
            ch.close()
            conn.close()

    def publish_subscriptions(self, message):

        self.log("FOUND SUB MESSAGE POSTING TO Q ... %s" % message, "info")
        message = self.get_sub_queue_message(message)
        try:
            conn = amqp.Connection(host="localhost:5672", userid="alonzo", password="@l0n30", virtual_host="/",
                                   insist=False)
        except Exception, e:
            self.log("failure in initiating connection %e" % e, "error")
            return

        self.log("Connection to rabbitMQ initiated ....", "info")
        try:
            ch = conn.channel()
            ch.queue_declare(queue="SUB_QUEUE", durable=True, exclusive=False, auto_delete=False)
            ch.exchange_declare(exchange="SUB_QUEUE", type="direct", durable=True, auto_delete=False)
            ch.queue_bind(queue="SUB_QUEUE", exchange="SUB_QUEUE", routing_key="SUB_QUEUE")
            msg = amqp.Message(json.dumps(message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 2

            ch.basic_publish(exchange="SUB_QUEUE", routing_key="SUB_QUEUE", msg=msg)
            self.log("publish successful ...", "info")
        except Exception, e:
            self.log("Error attempting to publish to Rabbit: %r " % e, "error")
            conn.close()
        else:
            ch.close()
            conn.close()

    def publish_trx(self, message):

        self.log("FOUND TRX MESSAGE POSTING TO Q ... %s" % message, "info")
        message = self.get_trx_queue_message(message)
        try:
            conn = amqp.Connection(host="localhost:5672", userid="alonzo", password="@l0n30", virtual_host="/",
                                   insist=False)
        except Exception, e:
            self.log("failure in initiating connection %e" % e, "error")
            return

        self.log("Connection to rabbitMQ initiated ....", "info")
        try:
            ch = conn.channel()
            ch.queue_declare(queue="TRX_QUEUE", durable=True, exclusive=False, auto_delete=False)
            ch.exchange_declare(exchange="TRX_QUEUE", type="direct", durable=True, auto_delete=False)
            ch.queue_bind(queue="TRX_QUEUE", exchange="TRX_QUEUE", routing_key="TRX_QUEUE")
            msg = amqp.Message(json.dumps(message))
            msg.properties["content_type"] = "text/plain"
            msg.properties["delivery_mode"] = 2

            ch.basic_publish(exchange="TRX_QUEUE", routing_key="TRX_QUEUE", msg=msg)
            self.log("publish successful ...", "info")
        except Exception, e:
            self.log("Error attempting to publish to Rabbit: %r " % e, "error")
            conn.close()
        else:
            ch.close()
            conn.close()


