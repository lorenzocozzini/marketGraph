import logging
import asyncio
from hbmqtt.broker import Broker
from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_1
from utils import IP_BROKER

logger = logging.getLogger(__name__)

config = {
        'listeners': {
            'default': {
                'max-connections': 50000,
                'bind': '{}:9999'.format(IP_BROKER),    # 0.0.0.0:1883 #160.78.100.132
                'type': 'tcp',
            }
        },
        'auth': {
            'allow-anonymous': True
        },
        'plugins': ['auth_anonymous'],
        'topic-check': {
            'enabled': True,
            'plugins': ['topic_taboo'],
        },
    }

broker = Broker(config)

@asyncio.coroutine
def startBroker():
    yield from broker.start()

@asyncio.coroutine
def brokerGetMessage():
    C = MQTTClient()
    yield from C.connect('mqtt://{}:9999/'.format(IP_BROKER))
    yield from C.subscribe([
        ("Symbol", QOS_1),
        ("Node", QOS_1)
    ])
    logger.info('Subscribed!')
    try:
        for i in range(1,100):
            message = yield from C.deliver_message()
            packet = message.publish_packet
            print(packet.payload.data.decode('utf-8'))
    except ClientException as ce:
        logger.error("Client exception : %s" % ce)

if __name__ == '__main__':
    formatter = "[%(asctime)s] :: %(levelname)s :: %(name)s :: %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    asyncio.get_event_loop().run_until_complete(startBroker())
    asyncio.get_event_loop().run_until_complete(brokerGetMessage())
    asyncio.get_event_loop().run_forever()