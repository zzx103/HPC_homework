# python3.7
import socket
import threading
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
        # nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # naddr = self.addr.split(',')
        # nsock.bind((naddr[0], int(naddr[1])))
        # nsock.listen(10)

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

        # # 与任务进程建立连接
        # tsock, taddr = nsock.accept()
        # tsock.send('connected'.encode())
        #
        # # 从任务进程接收局部最大值
        # msg = tsock.recv(self.buffsize)
        #
        # # 通知服务器并发送局部最大值
        # ssock.send('send_local_max_number'.encode())
        # tmsg = ssock.recv(self.buffsize)
        # ssock.send(msg)
        #
        # # 从任务进程接收局部最大互质数
        # tsock.send('get_prime'.encode())
        # local_prime = tsock.recv(self.buffsize)
        #
        # # 结束与任务进程通信
        # tsock.send('over'.encode())
        # tsock.close()
        #
        # # 通知服务器并发送局部最大互质数
        # msg = ssock.recv(self.buffsize)
        # ssock.send('send_local_prime'.encode())
        # msg = ssock.recv(self.buffsize)
        # ssock.send(local_prime)

        # 结束与服务器通信
        # msg = ssock.recv(self.buffsize)
        ssock.send('quit'.encode())
        ssock.close()


# 服务器端
class server:
    def __init__(self, addr, num):
        self.addr = addr
        self.buffsize = 2048
        # self.nodeaddress = nodeaddress
        self.n = num
        # self.taskstate = [0] * num
        self.address_table = {}
        self.sp_id = random.randint(0, num - 1)
        self.res = []
        self.all_nodes_ready = threading.Event()
        self.begin_to_work = threading.Event()
        self.work_done = threading.Event()

    # 发送文件
    def _sendfile(self, sock, path):
        filesize = str(os.path.getsize(path))
        sock.send(filesize.encode())
        msg = sock.recv(self.buffsize)
        f = open(path, "rb")
        for line in f:
            sock.send(line)
        f.close()

    # 节点控制线程
    def _nodecontrol(self, server_sock, datapath, programpath):
        # 与节点建立连接
        nsock, _ = server_sock.accept()

        while True:
            # 获取节点的请求并作出回应
            msg = nsock.recv(self.buffsize)
            if msg.decode() == 'connected':
                nsock.send('connected'.encode())

            # 节点请求发送数据，发送数据文件
            elif msg.decode() == 'get_data':
                self._sendfile(nsock, datapath)
            # 节点请求发送程序，发送程序文件
            elif msg.decode() == 'get_program':
                self._sendfile(nsock, programpath)

            elif msg.decode() == 'quit':
                nsock.close()
                break


        # # 节点请求发送局部最大值，准备接收
        # msg = nsock.recv(self.buffsize)
        # if msg.decode() == 'send_local_max_number':
        #     nsock.send('ready_to_receive'.encode())
        #     lmn = nsock.recv(self.buffsize)
        #     # 保存局部最大值
        #     self.res.append(int(lmn.decode()))
        #     # 若所有节点均已发送局部最大值，通知主线程
        #     if len(self.res) == self.n:
        #         self.all_local_max_ready.set()
        #     nsock.send('local_max_number_received'.encode())
        #
        # # 节点请求发送局部最大互质数，准备接收
        # msg = nsock.recv(self.buffsize)
        # if msg.decode() == 'send_local_prime':
        #     nsock.send('ready_to_receive'.encode())
        #     lmp = nsock.recv(self.buffsize)
        #     # 若所有节点均已发送局部最大互质数，通知主线程
        #     nsock.send('local_prime_received'.encode())
        #     self.res.append(int(lmp.decode()))
        #     if len(self.res) == self.n + 1:
        #         self.all_local_prime_ready.set()



    # 任务控制线程
    def _taskcontrol(self, server_sock, task_id):
        # 与任务进程建立连接
        tsock, taddr = server_sock.accept()

        self.address_table[task_id] = taddr[0]

        #
        if len(self.address_table) == self.n:
            self.all_nodes_ready.set()

        # 等待主线程通知各任务进程开始计算任务
        self.begin_to_work.wait()

        while True:
            # 获取任务进程的请求并作出回应
            msg = tsock.recv(self.buffsize)

            # 任务进程请求获取任务角色（编号），发送任务编号
            if msg.decode() == 'get_task_id':
                # self.taskstate[task_id] = 1
                tsock.send(str(task_id).encode())

            # 任务进程请求获取数据总数
            elif msg.decode() == 'get_task_num':

                # 发送节点总数量
                tsock.send(str(self.n).encode())

            elif msg.decode() == 'get_special_id':
                #
                s_id = str(self.sp_id)
                tsock.send(s_id.encode())

            elif msg.decode() == 'get_special_ip':
                #
                # print('addrt ', self.address_table)
                # print('sid ', self.sp_id)
                # print(self.address_table[self.sp_id])
                s_ip = self.address_table[self.sp_id]
                tsock.send(s_ip.encode())

            # 任务进程请求发送全局最大值
            elif msg.decode() == 'send_global_max_number':
                tsock.send('ready'.encode())
                msg = tsock.recv(self.buffsize)
                g_max = int(msg.decode())
                self.res.append(g_max)
                tsock.send('received'.encode())

            elif msg.decode() == 'send_global_max_prime':
                tsock.send('ready'.encode())
                msg = tsock.recv(self.buffsize)
                g_max_p = int(msg.decode())
                self.res.append(g_max_p)
                tsock.send('received'.encode())
                self.work_done.set()

            elif msg.decode() == 'quit':
                tsock.close()
                break

    # 服务器工作主线程
    def workstart(self, datapath, programpath):
        t1 = time.time()

        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        saddr = self.addr.split(',')
        ssock.bind((saddr[0], int(saddr[1])))
        ssock.listen(50)

        # 启动节点控制线程
        for nodeid in range(self.n):
            t = threading.Thread(target=self._nodecontrol, args=(ssock, datapath, programpath))
            t.start()

        # 启动任务控制线程
        for taskid in range(self.n):
            t = threading.Thread(target=self._taskcontrol, args=(ssock, taskid))
            t.start()

        # 等待所有节点就绪
        self.all_nodes_ready.wait()

        t2 = time.time()

        # 通知节点控制线程开始计算工作
        self.begin_to_work.set()

        # 等待结果
        self.work_done.wait()

        t3 = time.time()

        # 保存结果
        with open('res.txt', 'a') as f:
            f.write(time.asctime(time.localtime(time.time())) + '\n')
            f.write(str(self.n) + '\n')

            f.write(str(t2 - t1) + '\n')
            f.write(str(t3 - t2) + '\n')
            f.write(str(t3 - t1) + '\n')
            for re in self.res:
                f.write(str(re) + '\n')
            f.write('\n')

        print('fin')


def main(argv):

    # 服务器模式
    # 'python server_node.py server test_data.txt programtest.py 1 server_address'
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
    # 'python server_node.py node 1 saddr'
    elif argv[0] == 'node':
        # 节点编号
        n_id = argv[1]
        # 服务器地址
        saddr = argv[2]

        n = node(saddr, n_id)
        n.workstart()


if __name__ == '__main__':
    main(sys.argv[1:])


