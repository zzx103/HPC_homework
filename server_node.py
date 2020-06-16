# python3.7
import socket
import multiprocessing
import time
import sys
import os
import random


# 节点端
class node:
    def __init__(self, server_addr, f_path):
        # 服务器地址
        self.s_addr = server_addr
        self.buffsize = 2048
        # 存放路径
        self.path = f_path

    # 接收文件
    def _recvfile(self, sock, path):
        msg = sock.recv(self.buffsize)
        sock.send("File size received".encode())
        total_size = int(msg.decode())
        received_size = 0
        f = open(path, "wb")
        while received_size < total_size:
            msg = sock.recv(self.buffsize)
            f.write(msg)
            received_size += len(msg)
        f.close()

    def workstart(self):

        # 存放数据，程序的路径
        node_data_path = self.path + '_data'
        node_program_path = self.path + '_program'

        # 与服务器建立连接
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        saddr = self.s_addr.split(',')
        ssock.connect((saddr[0], int(saddr[1])))

        ssock.send('connected'.encode())
        msg = ssock.recv(self.buffsize)

        # 请求服务器发送数据
        ssock.send('get_data'.encode())
        self._recvfile(ssock, node_data_path)

        # 请求服务器发送程序
        ssock.send('get_program'.encode())
        self._recvfile(ssock, node_program_path)

        # 启动任务进程
        arg = 'python ' + node_program_path + ' ' + node_data_path + ' ' + self.s_addr
        os.system(arg)

        ssock.send('quit'.encode())
        ssock.close()


# 服务器端
class server:
    def __init__(self, addr, num):
        self.addr = addr
        self.buffsize = 2048
        self.n = num
        self.address_table = multiprocessing.Manager().dict()
        self.sp_id = random.randint(0, num - 1)
        self.res = multiprocessing.Manager().list()
        self.all_nodes_ready = multiprocessing.Event()
        self.begin_to_work = multiprocessing.Event()
        self.work_done = multiprocessing.Event()
        self.node_sp_ready = multiprocessing.Event()
        self.lock = multiprocessing.Lock()

    # 发送文件
    def _sendfile(self, sock, path):
        filesize = str(os.path.getsize(path))
        sock.send(filesize.encode())
        msg = sock.recv(self.buffsize)
        f = open(path, "rb")
        for line in f:
            sock.send(line)
        f.close()

    # 节点管理进程
    def _nodecontrol(self, node_sock, datapath, programpath):
        # 与节点建立连接

        while True:
            # 获取节点的请求并作出回应
            msg = node_sock.recv(self.buffsize)

            if msg.decode() == 'connected':
                node_sock.send('connected'.encode())

            # 节点请求发送数据，发送数据文件
            elif msg.decode() == 'get_data':
                self._sendfile(node_sock, datapath)
            # 节点请求发送程序，发送程序文件
            elif msg.decode() == 'get_program':
                self._sendfile(node_sock, programpath)

            elif msg.decode() == 'quit':
                node_sock.close()
                break

    # 任务管理进程
    def _taskcontrol(self, task_sock, task_id):

        # 所有任务进程已连接，通知主进程
        self.lock.acquire()
        taddr = task_sock.getpeername()
        self.address_table[task_id] = taddr[0]
        if len(self.address_table) == self.n:
            self.all_nodes_ready.set()
        self.lock.release()

        # 等待主线程开始计算任务
        self.begin_to_work.wait()

        while True:
            # 获取任务进程的请求并作出回应
            msg = task_sock.recv(self.buffsize)

            # 任务进程请求获取编号
            if msg.decode() == 'get_task_id':
                task_sock.send(str(task_id).encode())

            # 任务进程请求获取节点总数
            elif msg.decode() == 'get_task_num':
                task_sock.send(str(self.n).encode())

            # 任务进程请求获取特殊节点编号
            elif msg.decode() == 'get_special_id':
                s_id = str(self.sp_id)
                task_sock.send(s_id.encode())

            # 一般任务进程请求获取特殊节点ip
            elif msg.decode() == 'get_special_ip':
                s_ip = self.address_table[self.sp_id]
                # 等待特殊任务进程监听就绪
                self.node_sp_ready.wait()
                task_sock.send(s_ip.encode())

            # 特殊任务进程请求同步
            elif msg.decode() == 'sp_ready':
                self.node_sp_ready.set()
                task_sock.send('wait'.encode())

            # 任务进程请求发送结果
            elif msg.decode() == 'send_result':
                task_sock.send('ready'.encode())
                msg = task_sock.recv(self.buffsize)
                result = int(msg.decode())

                self.lock.acquire()
                self.res.append(result)
                self.lock.release()

                task_sock.send('received'.encode())

            # 任务进程计算工作结束
            elif msg.decode() == 'done':
                task_sock.send('done'.encode())
                self.work_done.set()

            elif msg.decode() == 'quit':
                task_sock.close()
                break

    # 服务器工作主进程
    def workstart(self, datapath, programpath):
        t1 = time.time()

        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        saddr = self.addr.split(',')
        ssock.bind((saddr[0], int(saddr[1])))
        ssock.listen(50)

        # 启动节点控制进程
        for nodeid in range(self.n):
            nsock, _ = ssock.accept()
            t = multiprocessing.Process(target=self._nodecontrol, args=(nsock, datapath, programpath))
            t.start()

        # 启动任务控制进程
        for taskid in range(self.n):

            tsock, taddr = ssock.accept()

            t = multiprocessing.Process(target=self._taskcontrol, args=(tsock, taskid))
            t.start()

        # 等待所有节点就绪
        self.all_nodes_ready.wait()

        t2 = time.time()

        # 通知节点控制进程开始计算工作
        self.begin_to_work.set()

        # 等待结果
        self.work_done.wait()

        t3 = time.time()

        # 保存结果
        with open('res.txt', 'a') as f:
            f.write(time.asctime(time.localtime(time.time())) + '\n')
            f.write('节点数：' + str(self.n) + '\n')
            f.write('传送文件时间：' + str(t2 - t1) + '\n')
            f.write('计算时间：' + str(t3 - t2) + '\n')
            f.write('总时间：' + str(t3 - t1) + '\n')
            f.write('最大元素与最大互质元素：\n')
            for re in self.res:
                f.write(str(re) + ' ')
            f.write('\n')

        print('fin')


def main(argv):

    # 服务器模式
    # 'python server_node.py server test_data.txt programtest.py 2 127.0.0.1,12340'
    if argv[0] == 'server':
        # 数据路径
        data_path = argv[1]
        # 任务程序路径
        program_path = argv[2]
        # 节点数量
        ns = int(argv[3])
        # 服务器地址
        server_addr = argv[4]

        s = server(server_addr, ns)
        s.workstart(data_path, program_path)

    # 节点模式
    # 'python server_node.py node 1 127.0.0.1,12340'
    elif argv[0] == 'node':
        # 节点编号
        n_id = argv[1]
        # 服务器地址
        saddr = argv[2]

        n = node(saddr, n_id)
        n.workstart()


if __name__ == '__main__':
    main(sys.argv[1:])


