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
        print(self.path + ' receiving')
        fp = open(path, 'w')
        while True:
            msg = sock.recv(self.buffsize)
            data = msg.decode()
            if data == '$finish':
                print('received')
                break
            fp.write(data)
        fp.close()


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
        ssock.send('getdata'.encode())
        self._recvfile(ssock, self.path + 'data')
        ssock.send('getprogram'.encode())
        self._recvfile(ssock, self.path + 'program')
        # 执行程序
        arg = 'python ' + self.path + 'program' + ' ' + self.path + 'data' + ' ' + saddr_str + ' ' + self.addr
        t = threading.Thread(target=os.popen, args=(arg,))
        t.start()

        tsock, taddr = nsock.accept()
        msg = tsock.recv(self.buffsize)
        ssock.send('sendresult'.encode())
        ssock.send(msg)
        msg = tsock.recv(self.buffsize)
        ssock.send('sendresult'.encode())
        ssock.send(msg)
        ssock.send('quit'.encode())
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
        fp = open(path, 'r')
        while True:
            data = fp.read(self.buffsize)
            if not data:
                break
            sock.send(data.encode())
        fp.close()
        print('send over')
        fin = '$finish'
        sock.send(fin.encode())


    def _nodecontrol(self, id, nsock):


        while True:
            msg = nsock.recv(self.buffsize)

            if msg.decode() == 'sendresult':
                res = nsock.recv(self.buffsize)
                self.res.append(int(res.decode()))
            elif msg.decode() == 'quit':
                nsock.close()
                break
            else:
                print(str(id) + 'wrong!')


    def _taskcontrol(self, sock, rolenum):
        while True:
            msg = sock.recv(self.buffsize)
            dmsg = msg.decode()

            if dmsg == 'getmaxnumber':
                while self.sig != 1:
                    continue
                res = self.res[0]
                sock.send(str(res).encode())
            elif dmsg == 'gettaskaddr':
                id = sock.recv(self.buffsize)
                taddr = self.tasktable[int(id.decode())]
                sock.send(str(taddr))
            elif dmsg == 'quit':
                sock.close()
                break
            else:
                print('task ' + str(rolenum) + 'wrong!')


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
            if msg.decode() == 'getdata':
                self._sendfile(nsock, datapath)

            msg = nsock.recv(self.buffsize)
            if msg.decode() == 'getprogram':
                self._sendfile(nsock, programpath)

            t = threading.Thread(target=self._nodecontrol, args=(nodeid, nsock))
            t.start()

        for i in range(self.n):
            ksock, kaddr = ssock.accept()
            self.tasktable[i] = kaddr

            msg = ksock.recv(self.buffsize)
            if msg.decode() == 'getrolenum':
                ksock.send(str(i).encode())

            msg = ksock.recv(self.buffsize)
            if msg.decode() == 'getnum':
                ksock.send(str(self.n).encode())

            t = threading.Thread(target=self._taskcontrol, args=(ksock, i))
            t.start()

        while True:
            if len(self.res) != self.n:
                continue
            maxnum = max(self.res)
            self.res.clear()
            self.res.append(maxnum)
            self.sig = 1

        while True:
            if len(self.res) != self.n + 1:
                continue

        print(self.res[0])
        print(max(self.res[1:]))


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


