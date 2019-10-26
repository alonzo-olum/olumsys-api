# CELERY IMPORTS
CELERY_IMPORTS = ('consumer',)
# BROKER URL
BROKER_URL = 'amqp://{user}:{password}@{host}:{port}//'.format(user="bobby", password="toor123!",
                                                               host="172.31.186.203", port="5672")
#BROKER_URL = 'amqp://{user}:{password}@{host}:{port}//'.format(user="alonzo", password="@l0n30",
#                                                               host="localhost", port="5672")





