from datetime import datetime
import os
import sys
import argparse
import time
import json
import paho.mqtt.publish as MqttPublisher
from opcua import Client as OPCLibClient


class Logger():
    """A logger utility.
    """
    #           0       1        2       3          4
    _levels = ['TRACE', 'DEBUG', 'INFO', 'WARNING', 'ERROR']
    Level = 2

    @staticmethod
    def parse_level(stringLevel):
        stringLevel = stringLevel.lower()
        stringLevels = [l.lower() for l in Logger._levels]
        if stringLevel in stringLevels:
            return stringLevels.index(stringLevel)
        return None

    @staticmethod
    def _log(level, msg, fields=None):
        t = datetime.now()
        txt = '{} | {: ^7} | "{}"'.format(t.isoformat(), Logger._levels[level], msg)
        if fields is not None:
            txt += ' '
            for k, v in fields.items():
                txt += '{}={} '.format(k, v)
        if Logger.Level <= level:
            print txt

    @staticmethod
    def trace(msg, fields=None):
        Logger._log(0, msg, fields=fields)

    @staticmethod
    def debug(msg, fields=None):
        Logger._log(1, msg, fields=fields)

    @staticmethod
    def info(msg, fields=None):
        Logger._log(2, msg, fields=fields)

    @staticmethod
    def warning(msg, fields=None):
        Logger._log(3, msg, fields=fields)

    @staticmethod
    def error(msg, fields=None):
        Logger._log(4, msg, fields=fields)


class Message():
    """A message to use for publishing.

    Use this to create a message object
    that the broker can simply publish to
    the remote.

    A message is expected to contain a "data-update",
    meaning a timestamp/value tuple. Each message
    then belongs to a topic which identifies the
    series.

    Attributes
    ----------
    __uri : str
        The identifier for origin of this message
    __topic : str
        The mqtt topic to publish this at
    __timestamp : datetime
        The datetime of the message
    __value : any
        The value of the message
    """

    TOPIC_PREFIX = '/idatase'
    def __init__(self, uri, timestamp, value):
        """Create a new message.

        Parameters
        ----------
        uri : str
            The identifier for origin of this message
        timestamp : datetime
            The datetime of the message
        value : any
            The value of the message
        """
        self.__topic = '{}/{}'.format(Message.TOPIC_PREFIX, uri)
        self.__uri = uri
        self.__timestamp = timestamp
        self.__value = value
        Logger.trace('Created message', fields={
            'topic': self.__topic,
            'uri': self.__uri,
            'timestamp': self.__timestamp.isoformat(),
            'value': self.__value
        })

    def topic(self):
        """Return the topic of the message.
        """
        return self.__topic

    def payload(self):
        """Return the payload of the message.
        """
        return json.dumps({
            'topic': self.__topic,
            'uri': self.__uri,
            'timestamp': self.__timestamp.isoformat(),
            'value': self.__value
        })


class MqttBroker():
    """Publishing Broker to connect to a remote Broker.

    Use this to simply publish to a remote broker
    given host/port and authorization data.

    Attributes
    ----------
    host : str
        The host of the remote broker
    port : str
        The port of the remote broker
    username : str|None
        The username to use for authorization
    password : str|None
        The password to use for authorization
    """

    def __init__(self, host, port=1883, username=None, password=None):
        """Create a new Broker.

        Parameters
        ----------
        host : str
            The host of the remote broker
        port : str
            The port of the remote broker
        username : str|None
            The username to use for authorization
        password : str|None
            The password to use for authorization
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        Logger.trace('Created MqttBroker', fields={
            'host': host,
            'port': port,
            'username': username,
            'password': '***' if password is not None else None
        })

    def publish(self, message):
        """Publish a single message.

        This publishes the message to the remote broker.

        Parameters
        ----------
        message : Message
            The message to publish.
        """
        # ensure type
        assert isinstance(message, Message)
        self.publish_multiple([message])

    def publish_multiple(self, messages):
        """Publish multiple messages at once.

        This publishes the messages to the remote broker.

        Parameters
        ----------
        messages : Message[]
            The messages to publish.
        """
        # ensure type
        msgs = []
        for message in messages:
            assert isinstance(message, Message)
            msgs.append((message.topic(), message.payload()))
        # prep auth
        auth = None
        if self.username is not None:
            auth = {"username": self.username, "password": self.password}
        # publish messages
        Logger.trace('Publishing messages', fields={
            'count': len(msgs),
            'host': self.host,
            'port': self.port,
            'auth': True if auth is not None else False
        })
        MqttPublisher.multiple(
            msgs, hostname=self.host, port=self.port, auth=auth)


class OpcClient():
    """A simplified OpClient.

    This is a opc client that can be leveraged to
    subscribe to the OPC data change notifications.
    
    You should interact with the client using
    * add_notification_handler
    * subscribe_variable
    * unsubscribe_variable

    So typically:
    ```
    # setup client
    cl = OpcClient(...)
    # define handler
    def handler(update):
        # data update : {'node': <node>, 'timestamp': <datetime>, 'value': <value>, 'data': <data>}
        # do something with update here
    # add handler
    cl.add_notification_handler(handler)
    # subscribe to variable
    cl.subscribe_variable(cl.get_node("ns=3;i=2002"))

    time.sleep(100)
    cl.disconnect()
    ```

    *Note*
    Make sure to call disconnect before shutting down in
    order to ensure a clean close.

    See: https://github.com/FreeOpcUa/python-opcua

    Attributes
    ----------
    _address : str
        The connection string address
    _address_obfuscated : str
        The connection string address obfuscating a potnetial password
    _client : OPCLibClient
        The opc lib client
    _subscription : ua.Subscription
        The ua subscription handle that can be used
        to create new subscritions or unsubscribe
    _sub_handles : dict
        A dictionary mapping ua.Node variables to
        subscription handles to be able to unsubscribe
        from them again
    _handlers : list
        A list of handler functions
    """
    def __init__(self, host, port, path='/', username=None, password=None):
        """Create a new OpClient.

        This will create a new opc client that can be
        leveraged to subscribe to the OPC data change
        notifications.

        Parameters
        ----------
        host : str
            The host name of the opc server.
        port : int
            The port of the opc server.
        path : str
            The path to add to the server as base
        username : str|None
            A username to use for authorization.
        password : str|None
            A password to use for authorization.
        """

        self._client = None
        self._subscription = None
        self._sub_handles = {}
        self._handlers = []

        # setup authorization
        auth = ''
        if username is not None:
            auth = '{}:{}@'.format(username, password)

        # define address
        self._address = self._address_obfuscated = "opc.tcp://{}{}:{}{}".format(auth, host, port, path)
        if username is not None and password is not None:
            self._address_obfuscated = self._address.replace(password, '***')

        Logger.trace('Created OpcClient', fields={
            'address': self._address_obfuscated
        })

        # setup client
        self.init_client()
        # setup subscriptions
        self.init_subscriptions()

    def init_client(self):
        """Initialize the client.

        This will connect to the client using
        the address. This is required to be
        called before anything else interacting
        with the opc server is done.
        """
        try:
            self._client = OPCLibClient(self._address)
            self._client.connect()
            self._client.load_type_definitions()
            self.namespace = self._client.get_namespace_array()
            Logger.trace('Connection established')
        except Exception as e:
            Logger.error('Failed connecting to address "{}"'.format(self._address_obfuscated))
            Logger.error('{}'.format(e))
            raise e

    def log_info(self):
        root = self._client.get_root_node()
        objects = self._client.get_objects_node()
        children = root.get_children()
        # print '{}:{}'.format(namespace[children[1].nodeid.NamespaceIndex], children[1].nodeid.Identifier)
        # print self._client.get_node("ns=0;i=86")
        Logger.debug("Retrieved Client Info", fields={
            'root': root,
            'objects': objects,
            'children': children,
            'namespace': self.namespace
        })

    def full_uri(self, node):
        """Resolve the full uri of a ua.Node.

        Parameters
        ----------
        node : ua.Node
            A node from the OPC interface.

        Returns
        -------
        str :
            The namespace prefixed full uri of the entity.
        """
        return '{}:{}'.format(
            self.namespace[node.nodeid.NamespaceIndex], node.nodeid.Identifier)

    def get_by_uri(self, uri):
        """Resolve the full uri to a ua.Node.

        Parameters
        ----------
        uri : str
            The uri to the ua.Node.

        Returns
        -------
        ua.Node :
            The resolved node.
        """
        ns_match = None
        for ns in self.namespace:
            if '{}:'.format(ns) in uri:
                ns_match = ns
                break
        if ns_match is None:
            raise RuntimeError('Namespace of "{}" not available'.format(uri))
        ns_idx = self._client.get_namespace_index(ns_match)
        identifier = uri.replace('{}:'.format(ns_match), '')
        try:
            int(identifier)
            is_int = True
        except:
            is_int = False

        substituted = 'ns={};{}={}'.format(ns_idx, 'i' if is_int else 's', identifier)
        return self._client.get_node(substituted)

    def init_subscriptions(self):
        """Initialize the subscriptions.

        This will initialize a subscription handler
        and afterwards will be ready to subscribe to
        specific variables.
        """
        if self._client is None:
            raise RuntimeError('Client not initialized yet.')

        self._subscription = self._client.create_subscription(500, self)
        Logger.trace('Base Subscription established')

    def subscribe_variable(self, variable):
        """Subscribe to a OPC variable.

        This will subscribe the client to
        the given variable and any data change
        notification will be published.

        Parameters
        ----------
        variable : ua.Node
            The ua node, e.g. client.get_node(ua.NodeId(1002, 2)) or
            client.get_node("ns=3;i=2002")
        """
        if self._subscription is None:
            raise RuntimeError('Subscriptions not initialized yet.')

        if variable in self._sub_handles.keys():
            Logger.info('Already subscribed to "{}"'.format(variable))
            return
        self._sub_handles[variable] = self._subscription.subscribe_data_change(variable)

    def unsubscribe_variable(self, variable):
        """Unsubscribe from a OPC variable.

        This will unsubscribe the client from
        the given variable.

        Parameters
        ----------
        variable : ua.Node
            The ua node, e.g. client.get_node(ua.NodeId(1002, 2)) or
            client.get_node("ns=3;i=2002")
        """
        if self._subscription is None:
            raise RuntimeError('Subscriptions not initialized yet.')

        if variable not in self._sub_handles.keys():
            Logger.info('Not subscribed to "{}"'.format(variable))
            return
        self._subscription.unsubscribe(self._sub_handles[variable])
        del self._sub_handles[variable]

    def disconnect(self):
        """Disconnect the client.
        """
        if self._client is None:
            return
        try:
            self._client.disconnect()
        except Exception:
            pass

    def datachange_notification(self, node, value, data):
        """Receiver from the OPC client.

        This gets called by OPC client on subscription
        notification.

        Parameter
        ---------
        node : ua.Node
            The node from which we received the data.
        value : any
            The value notification
        data : any
            The data of the notification
        """
        timestamp = datetime.now()
        if hasattr(data, 'MonitoredItemNotification') and \
                hasattr(data.MonitoredItemNotification, 'SourceTimestamp'):
            timestamp = data.MonitoredItemNotification.SourceTimestamp
        Logger.debug('OpcClient: Received data change notification', fields={
            'node': node,
            'value': value,
            'data': data,
            'timestamp': timestamp.isoformat()
        })
        # send update to handlers
        update = {
            'node': node,
            'value': value,
            'data': data,
            'timestamp': timestamp
        }
        for hdl in self._handlers:
            hdl(update)


    def add_notification_handler(self, handler):
        """Add a handler function.

        This handler `def handler(update)` will be
        called upon reception of a notification from
        the OPC Client. The update will have the
        following data:
        ```{
            'node': <node>,
            'value': <value>,
            'data': <data>,
            'timestamp': <timestamp>
        }```
        """
        self._handlers.append(handler)


class Gateway():

    def __init__(self, opc_client, mqtt_broker):
        self.opc_client = opc_client
        self.mqtt_broker = mqtt_broker
        Logger.trace('Created Gateway', fields={
            'opc_client': opc_client,
            'mqtt_broker': mqtt_broker
        })
        # add notification handler
        def handler(update):
            try:
                self.route_update(update)
            except (Exception, IOError, RuntimeError) as e:
                Logger.error('Failed routing update: {}'.format(e))
        opc_client.add_notification_handler(handler)

    def route_update(self, update):
        """Route an update from the OPC Client to the MQTT Broker.

        Parameters
        ----------
        update : dict
            An update as received from the opc client.
        """
        uri = self.opc_client.full_uri(update['node'])
        msg = Message(uri, update['timestamp'], update['value'])
        Logger.trace('Routing update', fields={
            'uri': uri
        })
        self.mqtt_broker.publish(msg)

    def setup_proxy_for(self, uris):
        """Add subscriptions at the OPC Client for the uris.

        Parameters
        ----------
        uris : str
            The uris to proxy to the MQTT Broker
        """
        for uri in uris:
            node = self.opc_client.get_by_uri(uri)
            self.opc_client.subscribe_variable(node)
            Logger.info('Subscribed variable', fields={
                'node': node,
                'uri': uri
            })

    def keep_alive(self):
        """
        """
        Logger.info('Locking Gateway')
        try:
            while True:
                time.sleep(1)
        except:
            Logger.info('Stopped Gateway')
            return


def run(
        opc_host, opc_port, mqtt_host, opc_path='/', opc_username=None,
        opc_password=None, mqtt_port=1883, mqtt_username=None, mqtt_password=None,
        mqtt_topic_prefix='/idatase', sub_uris=[]):
    opc_client = None
    try:
        # Setup MQTT Broker
        Message.TOPIC_PREFIX = mqtt_topic_prefix
        mqtt_broker = MqttBroker(
            mqtt_host, port=mqtt_port, username=mqtt_username,
            password=mqtt_password)

        # Setup Opc Client
        opc_client = OpcClient(
            opc_host, opc_port, path=opc_path, username=opc_username,
            password=opc_password)
        opc_client.log_info()

        # Define Gateway to Route Data
        gateway = Gateway(opc_client, mqtt_broker)

        # setup subscriptions to be proxied
        gateway.setup_proxy_for(sub_uris)

        # loop forever
        gateway.keep_alive()
    finally:
        # disconnect if necessary
        if opc_client is not None:
            opc_client.disconnect()


def parse_args():
    parser = argparse.ArgumentParser(
        description='OPC-UA - MQTT Gateway.',
        epilog='''Any of the cli-arguments can be as well set through the 
        environment variables noted in the descriptions.
        The cli-arguments will always have priority over environment
        variables.''')

    # OPC Client Config
    parser.add_argument(
        '--opc-host', default=os.environ.get('GATEWAY_OPC_HOST', 'localhost'),
        type=str, help='OPC Hostname. Env: GATEWAY_OPC_HOST')
    parser.add_argument(
        '--opc-port', default=os.environ.get('GATEWAY_OPC_PORT', 8080),
        type=str, help='OPC Port. Env: GATEWAY_OPC_PORT')
    parser.add_argument(
        '--opc-path', default=os.environ.get('GATEWAY_OPC_PATH', '/'),
        type=str, help='OPC Path. Env: GATEWAY_OPC_PATH')
    parser.add_argument(
        '--opc-username', default=os.environ.get('GATEWAY_OPC_USERNAME'),
        type=str, help='OPC Username. Env: GATEWAY_OPC_USERNAME')
    parser.add_argument(
        '--opc-password', default=os.environ.get('GATEWAY_OPC_PASSWORD'),
        type=str, help='OPC Password. Env: GATEWAY_OPC_PASSWORD')

    # MQTT Broker
    parser.add_argument(
        '--mqtt-host', default=os.environ.get('GATEWAY_MQTT_HOST', 'localhost'),
        type=str, help='MQTT Hostname. Env: GATEWAY_MQTT_HOST')
    parser.add_argument(
        '--mqtt-port', default=os.environ.get('GATEWAY_MQTT_PORT', 1883),
        type=str, help='MQTT Port. Env: GATEWAY_MQTT_PORT')
    parser.add_argument(
        '--mqtt-username', default=os.environ.get('GATEWAY_MQTT_USERNAME'),
        type=str, help='MQTT Username. Env: GATEWAY_MQTT_USERNAME')
    parser.add_argument(
        '--mqtt-password', default=os.environ.get('GATEWAY_MQTT_PASSWORD'),
        type=str, help='MQTT Password. Env: GATEWAY_MQTT_PASSWORD')
    parser.add_argument(
        '--mqtt-topic-prefix', default=os.environ.get('GATEWAY_MQTT_TOPIC_PREFIX', '/idatase'),
        type=str, help='MQTT Topic Prefix. Env: GATEWAY_MQTT_TOPIC_PREFIX')

    # Subscription Config
    parser.add_argument(
        '--proxy-uri', action='append', type=str,
        default=os.environ['GATEWAY_PROXY_URIS'].split(';') if 'GATEWAY_PROXY_URIS' in os.environ.keys() else [],
        help='Add a uri to proxy from the OPC Client to the MQTT Broker. Env: GATEWAY_PROXY_URIS (note '
        'the environment variable expects uris to be separated by a ";". Also note that whatever is '
        'set through the environment will be added to what is supplied through the cli options, unlike '
        'with the rest of the options, this is not overwritten)')

    # System Config
    parser.add_argument(
        '--log-level', type=str, default='INFO',
        help='Log Level: One of {}'.format(', '.join(Logger._levels)))


    return parser.parse_args()

if __name__ == '__main__':
    # parse args
    args = parse_args()

    # set log level
    if Logger.parse_level(args.log_level) is None:
        print 'Unknown loglevel: {}'.format(args.log_level)
        sys.exit(1)
    Logger.Level = Logger.parse_level(args.log_level)

    # run the gateway
    run(args.opc_host, args.opc_port, args.mqtt_host, opc_path=args.opc_path,
        opc_username=args.opc_username, opc_password=args.opc_password,
        mqtt_port=args.mqtt_port, mqtt_username=args.mqtt_username,
        mqtt_password=args.mqtt_password, mqtt_topic_prefix=args.mqtt_topic_prefix,
        sub_uris=list(set(args.proxy_uri)))

    # Some sample usage:
    # python gateway.py \
    #   --opc-host 'opcuaserver.com' --opc-port 48484 \
    #   --mqtt-host 'test.mosquitto.org' --mqtt-topic-prefix '/idatase' \
    #   --proxy-uri 'urn:unconfigured:application:Countries.AR.CENTENARIO.Temperature' \
    #   --proxy-uri 'urn:unconfigured:application:Countries.AR.CENTENARIO.WindSpeed' \
    #   --proxy-uri 'urn:unconfigured:application:Countries.AR.CENTENARIO.WindBearing'

    # Public OPC Servers:
    # https://github.com/node-opcua/node-opcua/wiki/publicly-available-OPC-UA-Servers-and-Clients

    # Public MQTT Brokers:
    # hivemq.com
    # mosquitto.org

