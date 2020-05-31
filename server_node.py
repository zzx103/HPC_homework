# python3.7
import socket
import threading
import time
import sys
import os

# 客户端
class node:
    def __init__(self, addr, server_addr, f_path):
        self.addr = addr
        self.s_addr = server_addr
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

        node_data_path = self.path + '_data'
        node_program_path = self.path + '_program'

        print(self.addr + ' start')
        ssock, _ = nsock.accept()
        ssock.send('connected'.encode())

        msg = ssock.recv(self.buffsize)

        ssock.send('get_data'.encode())
        self._recvfile(ssock, node_data_path)
        print(node_data_path + " received")

        ssock.send('get_program'.encode())
        self._recvfile(ssock, node_program_path)
        print(node_program_path + " received")

        # 执行程序
        arg = 'python ' + node_program_path + ' ' + node_data_path + ' ' + self.s_addr + ' ' + self.addr
        t = threading.Thread(target=os.popen, args=(arg,))
        t.start()

        tsock, taddr = nsock.accept()
        print('task connected')

        tsock.send('connected'.encode())
        msg = tsock.recv(self.buffsize)
        print('get local max number: ' + msg.decode())

        ssock.send('send_local_max_number'.encode())
        tmsg = ssock.recv(self.buffsize)
        ssock.send(msg)

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

    def _nodecontrol(self, id, datapath, programpath):

        nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_addr = self.nodeaddress[id].split(',')
        nsock.connect((node_addr[0], int(node_addr[1])))

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'connected':
            self.nodestate[id] = 1
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
            print(str(id) + ' local max number: ' + lmn.decode())
            self.res.append(int(lmn.decode()))
            nsock.send('local_max_number_received'.encode())

        msg = nsock.recv(self.buffsize)
        if msg.decode() == 'send_local_prime':
            nsock.send('ready_to_receive'.encode())
            lmp = nsock.recv(self.buffsize)
            print(str(id) + ' local max prime: ' + lmp.decode())
            nsock.send('local_prime_received'.encode())
            self.res.append(int(lmp.decode()))

        nsock.close()


    def _taskcontrol(self, server_sock, task_id):
        tsock, _ = server_sock.accept()
        print('task ' + str(task_id) + 'connected')

        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_task_id':
            tsock.send(str(task_id).encode())

        msg = tsock.recv(self.buffsize)
        if msg.decode() == 'get_task_num':
            tsock.send(str(self.n).encode())

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
            t = threading.Thread(target=self._nodecontrol, args=(nodeid, datapath, programpath))
            t.start()

        for i in range(self.n):
            t = threading.Thread(target=self._taskcontrol, args=(ssock, i))
            t.start()

        while True:
            if len(self.res) == self.n:
                maxnum = max(self.res)
                self.res.clear()
                self.res.append(maxnum)
                print('global max number: ' + str(maxnum))
                self.sig = 1
                break

        while True:
            if len(self.res) == self.n + 1:
                maxp = max(self.res[1:])
                print('global max prime: ' + str(maxp))
                break


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
        n = node(addrs[int(n_id)], addrs[0], n_id)
        n.start()


if __name__ == '__main__':
    main(sys.argv[1:])


