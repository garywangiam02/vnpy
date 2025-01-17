from __future__ import print_function
from __future__ import absolute_import
from time import sleep

from vnpy.rpc import RpcClient


class TestClient(RpcClient):
    """
    Test RpcClient
    """

    def __init__(self):
        """
        Constructor
        """
        super(TestClient, self).__init__()

    def callback(self, topic, data):
        """
        Realize callable function
        """
        print(f"client received topic:{topic}, data:{data}")


if __name__ == "__main__":
    req_address = "tcp://127.0.0.1:2014"
    sub_address = "tcp://127.0.0.1:4102"

    tc = TestClient()
    tc.subscribe_topic("")
    tc.start(req_address, sub_address)

    while 1:
        print(tc.add(1, 3))
        sleep(2)
