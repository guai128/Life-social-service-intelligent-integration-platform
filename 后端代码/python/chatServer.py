# client1 -> server -> client2
import json
from session_handler import SessionHandler
from config import *
import asyncio
import websockets


class ConnectionPool:
    def __init__(self, max_connection=1000):
        self._max_connection = max_connection
        self.connection = dict()

    def add_connection(self, userid: str, ws):
        if self.connection.__len__() == self._max_connection:
            if userid in self.connection:
                self.connection[userid] = ws
            else:
                return False

        self.connection[userid] = ws
        return True

    def remove_connection(self, userid):
        if userid in self.connection:
            self.connection.pop(userid)
            return True

        return False

    def connection_exist(self, userid: str):
        return userid in self.connection

    def get_connection_ws(self, userid):
        return self.connection.get(userid)


class ChatWebsocketServer:
    def __init__(self, max_client=1000):
        self.client_pool = ConnectionPool(max_connection=max_client)
        self.handler = SessionHandler(host=DATABASE_ADDR,
                                      user=DATABASE_USER,
                                      password=DATABASE_PASSWORD,
                                      db=DATABASE_DATABASE_NAME,
                                      port=DATABASE_PORT,
                                      charset=DATABASE_CHARSET)
        # self.handler.sqlProxy.create_keep_live_interval()

    async def serverHands(self, websocket):
        while True:
            try:
                recv_text = await websocket.recv()
                print(f"recv from {websocket.remote_address} : {recv_text}")
                try:
                    data = json.loads(recv_text)
                    if 'type' in data and data['type'] == 'register_chat' and 'userid' in data:
                        print(f"client {websocket.remote_address} connect")
                        self.client_pool.add_connection(data['userid'], websocket)
                        return
                except json.JSONDecodeError:
                    pass
            except websockets.exceptions.ConnectionClosedOK:
                await websocket.close()
                print(f"client {websocket.remote_address} disconnected")
                break

    # 接收从客户端发来的消息并处理，再返给客户端ok
    async def serverRecv(self, websocket):
        while True:
            try:
                recv_text = await websocket.recv()
                print(f"recv from {websocket.remote_address} : {recv_text}")
                ret_json = self.handler.handle(recv_text)
                if ret_json['status'] == 'no reply':
                    continue
                elif ret_json['status'] == 'success':
                    # get target userid
                    if ret_json['data']['isfirsttosecond']:
                        target_userid = ret_json['data']['seconduserid']
                    else:
                        target_userid = ret_json['data']['firstuserid']

                    target_ws = self.client_pool.get_connection_ws(target_userid)
                    if target_ws is not None:
                        await target_ws.send(json.dumps(ret_json))
                else:
                    await websocket.send(json.dumps(ret_json))
            except websockets.exceptions.ConnectionClosed:
                await websocket.close()
                print(f"client {websocket.remote_address} disconnected")
                break

    # 握手并且接收数据
    async def serverRun(self, websocket):
        await self.serverHands(websocket)
        await self.serverRecv(websocket)

    def run(self):
        _server = websockets.serve(self.serverRun, WEBSOCKET_ADDR, WEBSOCKET_CHAT_PORT)
        try:
            asyncio.get_event_loop().run_until_complete(_server)
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            print("======server main end======")


# main function
if __name__ == '__main__':
    server = ChatWebsocketServer()
    server.run()
