#!/user/bin/env python

import pika
import logging
import argparse

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')

LOGGER = logging.getLogger(__name__)

class Publisher:

    def __init__(self, queue=None, rabbitmqUrl = None):
         #'amqp://guest:guest@localhost:5672/%2F'

        self.rabbitmqUrl = rabbitmqUrl
        self.queue = queue
        self.channel = None

    def connect_queue(self):
        '''This method connects to RabbitMQ, returning the connection handle.
        :return: pika.BlockingConnection
        '''
        LOGGER.info('Connecting to %s', self.rabbitmqUrl)

        params = pika.URLParameters(self.rabbitmqUrl)
        params.socket_timeout = 5  # default socket_timeout is 0.25s
        params.connection_attempts = 3  # default connection attempt is 1
        params.retry_delay = 5  # set retey delay interval to 5 seconds

        return pika.BlockingConnection(params)

    def setup_channel(self, connection):
        '''This method sets up a channel from a connection

        :return: channel
        '''
        if connection:
            return connection.channel()

    def setup_queue(self, channel):
        '''Setup the queue on RabbitMQ by invoking the Queue. Declare RPC command.
        When it is complete, the on_queue_declareok method will be invoked by pika.

        :param queue: name of the queue to declare
        '''
        LOGGER.info('Declaring queue %s', self.queue)
        if channel:
            channel.queue_declare(queue=self.queue)

    def publish(self, channel, message):
        '''Publish messages to queue
        '''
        if channel:
            channel.basic_publish(exchange='', routing_key=self.queue, body=message)
            print(' [x] Sent ' + message)

    def close_connection(self, connection):
        '''Close the connetion to RabbitMQ
        '''
        if connection:
            connection.close()
            print ("connection to %s is closed!" % self.rabbitmqUrl)

def main():

    parser = argparse.ArgumentParser(description="Publish messages to a queue")
    parser.add_argument("-url", dest="rabbitmq_url", help="Input RabbitMQ URL", required=True)
    parser.add_argument("-queue", dest="queue_name", help="queue name", required=True)

    args = parser.parse_args()

    LOGGER.info("Publishing messages to queue %s in rabbitMq %s", args.queue_name, args.rabbitmq_url)

    publisher = Publisher(queue=args.queue_name, rabbitmqUrl=args.rabbitmq_url)
    connection = publisher.connect_queue()
    channel = publisher.setup_channel(connection)

    for i in range(6):
        publisher.publish(channel, "I can count " + str(i))

    publisher.close_connection(connection)

if __name__ == "__main__":
    main()

