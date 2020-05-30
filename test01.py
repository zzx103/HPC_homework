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
        fp = open(path, 'w')
        while True:
            data = sock.recv(self.buffsize)

            if data.decode() == '$finish':
                break
            fp.write(data.decode())
        fp.close()


    def start(self):
        nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        naddr = self.addr.split(',')
        nsock.bind((naddr[0], int(naddr[1])))
        nsock.listen(10)
        # 接收文件
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
        fin = '$finish'
        sock.send(fin.encode())
        fp.close()

    def _nodecontrol(self, id, naddr, datapath, programpath):
        ksock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ksock.connect(naddr)
        msg = ksock.recv(self.buffsize)
        if msg.decode() != 'connected':
            self.nodestate[id] = -1
            return

        while True:
            msg = ksock.recv(self.buffsize)
            if msg.decode() == 'getdata':
                self._sendfile(ksock, datapath)
            elif msg.decode() == 'getprogram':
                self._sendfile(ksock, programpath)
            elif msg.decode() == 'sendresult':
                res = ksock.recv(self.buffsize)
                self.res.append(int(res.decode()))
            elif msg.decode() == 'quit':
                ksock.close()
                break
            else:
                ksock.send('wrong'.encode())


    def _taskcontrol(self, sock, rolenum):
        while True:
            msg = sock.recv(self.buffsize)
            if msg.decode() == 'getrolenum':
                sock.send(str(rolenum).encode())
            elif msg.decode() == 'getnum':
                sock.send(str(self.n).encode())
            elif msg.decode() == 'getresult':
                while self.sig != 1:
                    continue
                res = self.res[0]
                sock.send(str(res).encode())
            elif msg.decode() == 'gettaskaddr':
                id = sock.recv(self.buffsize)
                taddr = self.tasktable[int(id.decode())]
                sock.send(str(taddr))
            elif msg.decode() == 'quit':
                sock.close()
                break
            else:
                sock.send('wrong'.encode())


    def workstart(self, datapath, programpath):
        ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ssock.bind((self.addr.split(',')[0], int(self.addr.split(',')[1])))

        ssock.listen(10)

        for nodeid in range(self.n):
            t = threading.Thread(target=self._nodecontrol, args=(nodeid, self.nodeaddress[nodeid], datapath, programpath))
            t.start()

        for i in range(self.n):
            ksock, kaddr = ssock.accept()
            self.tasktable[i] = kaddr
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






