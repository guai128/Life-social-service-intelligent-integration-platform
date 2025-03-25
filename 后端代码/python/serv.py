import asyncio
import json
from concurrent.futures.process import ProcessPoolExecutor
import websockets
import session_handler
from delayScheduler import DelayScheduler
from utils import print_and_record
from config import *


def handle(recv_text):
    handler = session_handler.SessionHandler(host=DATABASE_ADDR,
                                             user=DATABASE_USER,
                                             password=DATABASE_PASSWORD,
                                             db=DATABASE_DATABASE_NAME,
                                             port=DATABASE_PORT,
                                             charset=DATABASE_CHARSET)
    ret = handler.handle(recv_text)
    handler.sqlProxy.conn.close()

    return ret


# 握手，通过接收hello，发送"123"来进行双方的握手。
async def serverHands(websocket):
    while True:
        try:
            recv_text = await websocket.recv()
            print_and_record(f"recv from {websocket.remote_address} : {recv_text}")
            # 获取连接的客户端的信息
            if recv_text == 'hello':
                print_and_record(f"client {websocket.remote_address} connect")
                return
            else:
                await websocket.send("connected fail")
        except websockets.exceptions.ConnectionClosedOK:
            await websocket.close()
            print_and_record(f"client {websocket.remote_address} disconnected")
            break


# 接收从客户端发来的消息并处理，再返给客户端ok
async def serverRecv(websocket):
    while True:
        try:
            recv_text = await websocket.recv()
            print_and_record(f"recv from {websocket.remote_address} : {recv_text}")
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(processPool, handle, recv_text)
            status = response['status']
            if status == 'no reply':
                continue
            else:
                response = json.dumps(response)
                print_and_record(f"send to {websocket.remote_address} : {response}")
                await websocket.send(response)

            # 处理延时任务
            if status == 'success':
                old_request = json.loads(recv_text)
                if old_request['type'] == 'register_user_at':
                    # 添加延迟注销任务
                    request = json.loads(recv_text)
                    request['type'] = 'remove_user_at'
                    delayScheduler.add_task(handle, [json.dumps(request)], 1000 * 5 * 60)

        except websockets.exceptions.ConnectionClosed:
            await websocket.close()
            print_and_record(f"client {websocket.remote_address} disconnected")
            break


# 握手并且接收数据
async def serverRun(websocket):
    await serverRecv(websocket)


processPool = ProcessPoolExecutor(max_workers=MAX_WORKER)
delayScheduler = DelayScheduler()

# main function
if __name__ == '__main__':
    print_and_record("======server main begin======")
    server = websockets.serve(serverRun, WEBSOCKET_ADDR, WEBSOCKET_DISPATCH_PORT)
    try:
        asyncio.get_event_loop().run_until_complete(server)
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        print_and_record("======server main end======")
