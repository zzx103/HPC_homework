# python3.7
import socket
import threading
import time
import sys
import os

# 客户端
class node:
    def __init__(self, addr, f_path):
        self.addr = addr
        self.buffsize = 2048
        self.path = f_path
        self.res = []

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

    def start(self):
        nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        naddr = self.addr.split(',')
        nsock.bind((naddr[0], int(naddr[1])))
        nsock.listen(10)
        # 接收文件
        print(self.addr + ' start')
        ssock, saddr = nsock.accept()
        saddr_str = saddr[0] + ',' + str(saddr[1])

        ssock.send('connected'.encode())

        msg = ssock.recv(self.buffsize)

        ssock.send('get_data'.encode())
        self._recvfile(ssock, self.path + 'data')
        print(self.path + 'data' + " received")

        ssock.send('get_program'.encode())
        self._recvfile(ssock, self.path + 'program')
        print(self.path + 'program' + " received")

        # 执行程序
        arg = 'python ' + self.path + 'program' + ' ' + self.path + 'data' + ' ' + saddr_str + ' ' + self.addr
        t = threading.Thread(target=os.popen, args=(arg,))
        t.start()

        tsock, taddr = nsock.accept()
        print('task connected')
        msg = tsock.recv(self.buffsize)
        lmn = msg
        print('get local max number: ' + lmn.decode())


        ssock.send('send_local_max_number'.encode())
        msg = ssock.recv(self.buffsize)
        ssock.send(lmn)

        tsock.send('get_prime'.encode())
        local_prime = tsock.recv(self.buffsize)
        print('get local prime: ' + local_prime.decode())
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
        self.nodestate = [0] * len(nodeaddress)
        self.res = []
        self.sig = 0
        self.tasktable = {}

    def _sendfile(self, sock, path):
        filesize = str(os.path.getsize(path))

        sock.send(filesize.encode())
        msg = sock.recv(self.buffsize)  # 挂起服务器发送，确保客户端单独收到文件大小数据，避免粘包
        print('sending')
        f = open(path, "rb")
        for line in f:
            sock.send(line)
        f.close()
        print('send over')

    def _nodecontrol(self, id, nsock):

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'send_local_max_number':
            nsock.send('ready_to_receive'.encode())
            lmn = nsock.recv(self.buffsize)
            self.res.append(int(lmn.decode()))
            nsock.send('local_max_number_received'.encode())

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'send_local_prime':
            nsock.send('ready_to_receive'.encode())
            lmp = nsock.recv(self.buffsize)
            nsock.send('local_prime_received'.encode())
            self.res.append(int(lmp.decode()))

        nsock.close()


    def _taskcontrol(self, tsock, task_id):
        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_global_max_number':
            while self.sig != 1:
                continue
            g_max = self.res[0]
            tsock.send(str(g_max).encode())
        tsock.close()


    def workstart(self, datapath, programpath):
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        saddr = self.addr.split(',')
        ssock.bind((saddr[0], int(saddr[1])))

        ssock.listen(10)

        for nodeid in range(self.n):

            nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_addr = self.nodeaddress[nodeid].split(',')
            nsock.connect((node_addr[0], int(node_addr[1])))

            msg = nsock.recv(self.buffsize)
            if msg.decode() != 'connected':
                self.nodestate[nodeid] = -1
                continue

            msg = nsock.recv(self.buffsize)
            if msg.decode() == 'get_data':
                self._sendfile(nsock, datapath)

            msg = nsock.recv(self.buffsize)
            if msg.decode() == 'get_program':
                self._sendfile(nsock, programpath)

            t = threading.Thread(target=self._nodecontrol, args=(nodeid, nsock))
            t.start()

        for i in range(self.n):
            ksock, kaddr = ssock.accept()
            self.tasktable[i] = kaddr

            msg = ksock.recv(self.buffsize)
            if msg.decode() == 'get_task_id':
                ksock.send(str(i).encode())

            msg = ksock.recv(self.buffsize)
            if msg.decode() == 'get_task_num':
                ksock.send(str(self.n).encode())

            t = threading.Thread(target=self._taskcontrol, args=(ksock, i))
            t.start()

        while True:
            if len(self.res) != self.n:
                continue

        maxnum = max(self.res)
        self.res.clear()
        self.res.append(maxnum)
        print('global max number: ' + str(maxnum))
        self.sig = 1

        while True:
            if len(self.res) != self.n + 1:
                continue

        maxp = max(self.res[1:])
        print('global max prime: ' + str(maxp))


def main(argv):

    address_file = argv[0]

    with open(address_file) as f:
        lines = f.readlines()
        addrs = [line.strip() for line in lines]
    # address_file server data_file program_file
    if argv[1] == 'server':

        data_file = argv[2]
        program_file = argv[3]
        ns = 2
        server_addr = addrs[0]
        s = server(server_addr, addrs[1:1 + ns])
        s.workstart(data_file, program_file)
    # address_file node node_id
    elif argv[1] == 'node':
        n_id = argv[2]
        n = node(addrs[int(n_id)], n_id)
        n.start()


if __name__ == '__main__':
    main(sys.argv[1:])


