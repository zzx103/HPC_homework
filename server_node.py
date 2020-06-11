# python3.7
import socket
import threading
import time
import sys
import os


# 节点端
class node:
    def __init__(self, addr, server_addr, f_path):
        self.addr = addr
        self.s_addr = server_addr
        self.buffsize = 2048
        self.path = f_path

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

        node_data_path = self.path + '_data'
        node_program_path = self.path + '_program'

        # print(self.addr + ' start')
        ssock, _ = nsock.accept()
        ssock.send('connected'.encode())

        msg = ssock.recv(self.buffsize)

        ssock.send('get_data'.encode())
        self._recvfile(ssock, node_data_path)
        # print(node_data_path + " received")

        ssock.send('get_program'.encode())
        self._recvfile(ssock, node_program_path)
        # print(node_program_path + " received")

        # 执行程序
        arg = 'python ' + node_program_path + ' ' + node_data_path + ' ' + self.s_addr + ' ' + self.addr
        t = threading.Thread(target=os.popen, args=(arg,))
        t.start()

        tsock, taddr = nsock.accept()

        tsock.send('connected'.encode())
        msg = tsock.recv(self.buffsize)
        # print('local max number: ' + msg.decode())

        ssock.send('send_local_max_number'.encode())
        tmsg = ssock.recv(self.buffsize)
        ssock.send(msg)

        tsock.send('get_prime'.encode())
        local_prime = tsock.recv(self.buffsize)
        # print('local prime: ' + local_prime.decode())
        tsock.send('over'.encode())
        tsock.close()

        msg = ssock.recv(self.buffsize)
        ssock.send('send_local_prime'.encode())
        msg = ssock.recv(self.buffsize)
        ssock.send(local_prime)

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
        # self.sig = 0
        self.tasktable = {}
        self.all_nodes_ready = threading.Event()
        self.begin_to_work = threading.Event()
        self.all_local_max_ready = threading.Event()
        self.global_max_ready = threading.Event()
        self.all_local_prime_ready = threading.Event()

    def _sendfile(self, sock, path):
        filesize = str(os.path.getsize(path))

        sock.send(filesize.encode())
        msg = sock.recv(self.buffsize)  # 挂起服务器发送，确保客户端单独收到文件大小数据，避免粘包
        f = open(path, "rb")
        for line in f:
            sock.send(line)
        f.close()

    def _nodecontrol(self, id, datapath, programpath):

        nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_addr = self.nodeaddress[id].split(',')
        nsock.connect((node_addr[0], int(node_addr[1])))

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'connected':
            nsock.send('connected'.encode())

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'get_data':
            self._sendfile(nsock, datapath)

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'get_program':
            self._sendfile(nsock, programpath)

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'send_local_max_number':
            nsock.send('ready_to_receive'.encode())
            lmn = nsock.recv(self.buffsize)
            # print(str(id) + ' local max number: ' + lmn.decode())
            self.res.append(int(lmn.decode()))
            if len(self.res) == self.n:
                self.all_local_max_ready.set()
            nsock.send('local_max_number_received'.encode())

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'send_local_prime':
            nsock.send('ready_to_receive'.encode())
            lmp = nsock.recv(self.buffsize)
            # print(str(id) + ' local max prime: ' + lmp.decode())
            nsock.send('local_prime_received'.encode())
            self.res.append(int(lmp.decode()))
            if len(self.res) == self.n + 1:
                self.all_local_prime_ready.set()

        nsock.close()

    def _taskcontrol(self, server_sock, task_id):
        tsock, _ = server_sock.accept()
        # print('task ' + str(task_id) + ' connected')

        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_task_id':
            self.taskstate[task_id] = 1
            if len(self.taskstate) == self.n:
                self.all_nodes_ready.set()
            tsock.send(str(task_id).encode())

        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_task_num':

            self.begin_to_work.wait()
            # while self.sig != 1:
            #     continue
            tsock.send(str(self.n).encode())

        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_global_max_number':

            self.global_max_ready.wait()
            # while self.sig != 2:
            #     continue
            g_max = self.res[0]
            tsock.send(str(g_max).encode())

        tsock.close()

    def workstart(self, datapath, programpath):

        t1 = time.time()
        results = []
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        saddr = self.addr.split(',')
        ssock.bind((saddr[0], int(saddr[1])))
        ssock.listen(10)

        for nodeid in range(self.n):
            t = threading.Thread(target=self._nodecontrol, args=(nodeid, datapath, programpath))
            t.start()

        for taskid in range(self.n):
            t = threading.Thread(target=self._taskcontrol, args=(ssock, taskid))
            t.start()

        self.all_nodes_ready.wait()
        # while sum(self.taskstate) != self.n:
        #     continue

        t2 = time.time()

        self.begin_to_work.set()
        # self.sig = 1

        self.all_local_max_ready.wait()
        # while len(self.res) != self.n:
        #     continue

        maxnum = max(self.res)
        self.res.clear()
        self.res.append(maxnum)
        results.append(str(maxnum))
        # print('global max number: ' + str(maxnum))

        self.global_max_ready.set()
        # self.sig = 2

        self.all_local_prime_ready.wait()
        # while len(self.res) != self.n + 1:
        #     continue

        maxp = max(self.res[1:])
        results.append(str(maxp))
        # print('global max prime: ' + str(maxp))

        t3 = time.time()
        results.append(str(t2 - t1))
        results.append(str(t3 - t2))
        results.append(str(t3 - t1))

        with open('res.txt', 'a') as f:
            f.write(time.asctime(time.localtime(time.time())) + '\n')
            f.write(str(self.n) + '\n')
            for re in results:
                f.write(re + '\n')
            f.write('\n')


def main(argv):

    # address_file = 'address.txt'
    #
    # with open(address_file) as f:
    #     lines = f.readlines()
    #     addrs = [line.strip() for line in lines]

    # server data_file program_file node_amount
    # 'server test_data.txt programtest.py n address_file'
    if argv[0] == 'server':

        data_file = argv[1]
        program_file = argv[2]
        ns = int(argv[3])

        address_file = argv[4]

        with open(address_file) as f:
            lines = f.readlines()
            addrs = [line.strip() for line in lines]

        server_addr = addrs[0]
        s = server(server_addr, addrs[1:1 + ns])
        s.workstart(data_file, program_file)
    # node node_id
    # 'node 1 addr saddr'
    elif argv[0] == 'node':
        n_id = argv[1]
        addr = argv[2]
        saddr = argv[3]
        n = node(addr, saddr, n_id)
        n.workstart()


if __name__ == '__main__':
    main(sys.argv[1:])


