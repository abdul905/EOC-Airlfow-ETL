
import sshtunnel as sshtunnel
from clickhouse_driver import connect
from clickhouse_driver import Client

def getConnectionDev():
    client = Client(host='161.97.136.95',
                    user='default',
                    password='pakistan',
                    port='9000', settings={"use_numpy": True})
    return client    


def getConnectionProd():
    with sshtunnel.SSHTunnelForwarder(
        ('172.16.3.68', 22),
        ssh_username="root",
        ssh_password="COV!D@19#",
        remote_bind_address=('localhost', 9000)) as server:

        local_port = server.local_bind_port
        print(local_port)

        conn = connect(f'clickhouse://default:mm@1234@localhost:{local_port}/test')
        #conn = connect(host='172.16.3.68', database='test', user='default', password='mm@1234')

        cursor = conn.cursor()
        # cursor.execute('SHOW TABLES')
        # print(cursor.fetchall())

        client = Client(host='localhost',port=local_port, database='test',
                                user='default',
                                password='mm@1234',
                                settings={"use_numpy": True})
        # client = Client(host='172.16.3.68',
        #                 user='default',
        #                 password='mm@1234',
        #                 port='9000', settings={"use_numpy": True})
        return client    

