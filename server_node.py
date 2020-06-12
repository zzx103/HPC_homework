# python3.7
import socket
import threading
import time
import sys
import os


# 节点端
class node:
    def __init__(self, addr, server_addr, f_path):
        # 节点地址
        self.addr = addr
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
        nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        naddr = self.addr.split(',')
        nsock.bind((naddr[0], int(naddr[1])))
        nsock.listen(10)

        # 存放数据，程序的路径
        node_data_path = self.path + '_data'
        node_program_path = self.path + '_program'

        # 与服务器建立连接
        ssock, _ = nsock.accept()
        ssock.send('connected'.encode())
        msg = ssock.recv(self.buffsize)

        # 请求服务器发送数据
        ssock.send('get_data'.encode())

        self._recvfile(ssock, node_data_path)

        # 请求服务器发送程序
        ssock.send('get_program'.encode())

        self._recvfile(ssock, node_program_path)

        # 启动任务进程
        arg = 'python ' + node_program_path + ' ' + node_data_path + ' ' + self.s_addr + ' ' + self.addr
        os.popen(arg)

        # 与任务进程建立连接
        tsock, taddr = nsock.accept()
        tsock.send('connected'.encode())

        # 从任务进程接收局部最大值
        msg = tsock.recv(self.buffsize)

        # 通知服务器并发送局部最大值
        ssock.send('send_local_max_number'.encode())
        tmsg = ssock.recv(self.buffsize)
        ssock.send(msg)

        # 从任务进程接收局部最大互质数
        tsock.send('get_prime'.encode())
        local_prime = tsock.recv(self.buffsize)

        # 结束与任务进程通信
        tsock.send('over'.encode())
        tsock.close()

        # 通知服务器并发送局部最大互质数
        msg = ssock.recv(self.buffsize)
        ssock.send('send_local_prime'.encode())
        msg = ssock.recv(self.buffsize)
        ssock.send(local_prime)

        # 结束与服务器通信
        msg = ssock.recv(self.buffsize)
        ssock.close()


# 服务器端
class server:
    def __init__(self, addr, nodeaddress):
        self.addr = addr
        self.buffsize = 2048
        self.nodeaddress = nodeaddress
        self.n = len(nodeaddress)
        self.taskstate = [0] * len(nodeaddress)
        self.res = []
        self.all_nodes_ready = threading.Event()
        self.begin_to_work = threading.Event()
        self.all_local_max_ready = threading.Event()
        self.global_max_ready = threading.Event()
        self.all_local_prime_ready = threading.Event()

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
    def _nodecontrol(self, id, datapath, programpath):
        # 与节点建立连接
        nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_addr = self.nodeaddress[id].split(',')
        nsock.connect((node_addr[0], int(node_addr[1])))

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'connected':
            nsock.send('connected'.encode())

        # 获取节点的请求并作出回应
        # 节点请求发送数据，发送数据文件
        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'get_data':
            self._sendfile(nsock, datapath)

        # 节点请求发送程序，发送程序文件
        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'get_program':
            self._sendfile(nsock, programpath)

        # 节点请求发送局部最大值，准备接收
        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'send_local_max_number':
            nsock.send('ready_to_receive'.encode())
            lmn = nsock.recv(self.buffsize)
            # 保存局部最大值
            self.res.append(int(lmn.decode()))
            # 若所有节点均已发送局部最大值，通知主线程
            if len(self.res) == self.n:
                self.all_local_max_ready.set()
            nsock.send('local_max_number_received'.encode())

        # 节点请求发送局部最大互质数，准备接收
        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'send_local_prime':
            nsock.send('ready_to_receive'.encode())
            lmp = nsock.recv(self.buffsize)
            # 若所有节点均已发送局部最大互质数，通知主线程
            nsock.send('local_prime_received'.encode())
            self.res.append(int(lmp.decode()))
            if len(self.res) == self.n + 1:
                self.all_local_prime_ready.set()

        nsock.close()

    # 任务控制线程
    def _taskcontrol(self, server_sock, task_id):
        # 与任务进程建立连接
        tsock, _ = server_sock.accept()

        # 获取任务进程的请求并作出回应
        # 任务进程请求获取任务角色（编号），发送任务编号
        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_task_id':
            self.taskstate[task_id] = 1
            if len(self.taskstate) == self.n:
                self.all_nodes_ready.set()
            tsock.send(str(task_id).encode())

        # 任务进程请求获取数据总数
        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_task_num':
            # 等待主线程通知各任务进程开始计算任务
            self.begin_to_work.wait()
            # 发送节点总数量
            tsock.send(str(self.n).encode())

        # 任务进程请求获取全局最大值
        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_global_max_number':
            # 等待主线程确定全局最大值
            self.global_max_ready.wait()
            # 发送全局最大值
            g_max = self.res[0]
            tsock.send(str(g_max).encode())

        tsock.close()

    # 服务器工作主线程
    def workstart(self, datapath, programpath):
        t1 = time.time()
        # 存放结果
        results = []

        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        saddr = self.addr.split(',')
        ssock.bind((saddr[0], int(saddr[1])))
        ssock.listen(10)

        # 启动节点控制线程
        for nodeid in range(self.n):
            t = threading.Thread(target=self._nodecontrol, args=(nodeid, datapath, programpath))
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

        # 等待所有节点上传局部最大值
        self.all_local_max_ready.wait()

        # 计算全局最大值
        maxnum = max(self.res)
        self.res.clear()
        self.res.append(maxnum)
        results.append(str(maxnum))

        # 通知所有节点可获取全局最大值
        self.global_max_ready.set()

        # 等待所有节点上传局部最大互质数
        self.all_local_prime_ready.wait()

        # 计算全局最大互质数
        maxp = max(self.res[1:])
        results.append(str(maxp))

        t3 = time.time()
        results.append(str(t2 - t1))
        results.append(str(t3 - t2))
        results.append(str(t3 - t1))

        # 保存结果
        with open('res.txt', 'a') as f:
            f.write(time.asctime(time.localtime(time.time())) + '\n')
            f.write(str(self.n) + '\n')
            for re in results:
                f.write(re + '\n')
            f.write('\n')

        print('fin')


def main(argv):

    # 服务器模式
    # 'python server_node.py server test_data.txt programtest.py 1 address_file'
    if argv[0] == 'server':
        # 数据路径
        data_path = argv[1]
        # 任务程序路径
        program_path = argv[2]
        # 节点数量
        ns = int(argv[3])
        # 地址路径，第一条为服务器地址，其余为节点地址
        address_path = argv[4]

        with open(address_path) as f:
            lines = f.readlines()
            addrs = [line.strip() for line in lines]

        server_addr = addrs[0]
        s = server(server_addr, addrs[1:1 + ns])
        s.workstart(data_path, program_path)

    # 'python server_node.py node 1 addr saddr'
    elif argv[0] == 'node':
        # 节点编号
        n_id = argv[1]
        # 节点地址
        addr = argv[2]
        # 服务器地址
        saddr = argv[3]

        n = node(addr, saddr, n_id)
        n.workstart()


if __name__ == '__main__':
    main(sys.argv[1:])


