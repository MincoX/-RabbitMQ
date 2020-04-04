import pika


class Manager:

    def __init__(self, username='MincoX', password='mincoroot', host='49.232.19.51', port=5672, virtual_host='/'):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.v_host = virtual_host
        self._connection = None

    def get_connect(self):
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(self.host, self.port, self.v_host, credentials)
        connection = pika.BlockingConnection(parameters)
        self._connection = connection

        return connection

    def close(self):
        self._connection.close()
