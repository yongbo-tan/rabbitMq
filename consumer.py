#/user/lib/env python

import pika
import logging
import json
import argparse

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                '-35s %(lineno) -5d: %(message)s')

LOGGER = logging.getLogger(__name__)

class Consumer():
    def __init__(self, queue=None, rabbitmqUrl = None):
        self.queue = queue
        self.rabbitmqUrl = rabbitmqUrl
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

    def callback(self, channel, method, properties, body):
        print(" [x] Received %r" % body)
        print " [x] Done"
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def consume(self, queue, channel):
        if channel:
            channel.basic_consume(self.callback, queue=self.queue)
            print (' [*] Waiting for messages. To exit press CTRL+C')
            channel.start_consuming()

def main():
    parser = argparse.ArgumentParser(description="Consume the messages from a queue")
    parser.add_argument("-url", dest="rabbitmq_url", help="Input RabbitMQ URL", required=True)
    parser.add_argument("-queue", dest="queue_name", help="Input queue name", required=True)

    args = parser.parse_args()

    LOGGER.info("Consuming messages from queue %s in rabbitMq %s", args.queue_name, args.rabbitmq_url)
    
    consumer = Consumer(queue=args.queue_name, rabbitmqUrl=args.rabbitmq_url)
    connection = consumer.connect_queue()
    channel = consumer.setup_channel(connection)
    consumer.setup_queue(channel)
    consumer.consume(args.queue_name, channel)

if __name__ == "__main__":
    main()

