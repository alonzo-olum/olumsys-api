from celery import Celery

BROKER_URL = 'amqp://{user}:{password}@{host}:{port}//'.format(user="alonzo", password="@l0n30",
                                                               host="localhost", port="5672")

celery = Celery('celery_config', broker=BROKER_URL, include='consumer')
celery.config_from_object('celeryconfig')

