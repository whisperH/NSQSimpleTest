import nsq
from tornado import gen
import tornado
from functools import partial
import json

def is_prime(n):
    n = int(n)
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2  # return False
    k = 3
    while k*k <= n:
        if n % k == 0:
            return False
        k += 2
    return True

@gen.coroutine
def write_message(topic, data, writer):
    response = writer.pub(topic, data.encode('utf8'))
    if isinstance(response, nsq.Error):
        print ("Error with Message: {}:{}".format(data, response))
    else:
        print ("Published Message: ", data)

def calculate_prime(message, writer):
    message.enable_async()
    data = json.loads(message.body)

    prime = is_prime(data["number"])
    data["prime"] = prime

    topic = "results"

    output_message = json.dumps(data)
    write_message(topic, output_message, writer)
    # callback = partial(write_message, topic=topic, msg=output_message)
    # writer.pub(topic, output_message, callback=callback)
    message.finish()

if __name__ == "__main__":
    writer = nsq.Writer(['127.0.0.1:4150'])
    handler = partial(calculate_prime, writer=writer)
    reader  = nsq.Reader(
        message_handler = handler,
        nsqd_tcp_addresses = ['127.0.0.1:4150'],
        topic = 'x_topic',
        channel = 'work_group_a')

    nsq.run()
