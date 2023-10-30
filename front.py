import nsq
import tornado.ioloop
from functools import partial
import random
import json

def pub_message():
    message = {}
    message['number'] = random.randint(1,1000)
    writer.pub('x_topic', json.dumps(message).encode('utf8'), finish_pub)

def load_prime_results(message):
    message.enable_async()
    data = json.loads(message.body)
    print(f"The number is {data['number']} and the prime is {data['prime']}")

def finish_pub(conn, data):
    if data.decode('utf8') == "OK":
        print("success!")
        handler = partial(load_prime_results)
        nsq.Reader(
            message_handler = handler,
            nsqd_tcp_addresses = ['127.0.0.1:4150'],
            topic = 'results',
            channel = 'work_group_a')
    else:
        print("failed!")

if __name__ == '__main__':
    writer = nsq.Writer(['127.0.0.1:4150'])
    tornado.ioloop.PeriodicCallback(pub_message, 1000).start()
    nsq.run()
