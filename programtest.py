# python3.7
import socket
import threading
import sys

def is_p(a, b):
    q = 0
    while True:
        q = a % b
        if q == 0:
            break
        else:
            a = b
            b = q
    if b == 1:
        return True
    else:
        return False

dpath = sys.argv[1]
saddr = sys.argv[2]
naddr = sys.argv[3]

buffsize = 2048

nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
nsock.connect((naddr.split(',')[0], int(naddr.split(',')[1])))


ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssock.connect((saddr.split(',')[0], int(saddr.split(',')[1])))
ssock.send('getrolenum'.encode())
msg = ssock.recv(buffsize)
k = int(msg.decode())
ssock.send('getnum'.encode())
msg = ssock.recv(buffsize)
n = int(msg.decode())

fp = open(dpath, 'r')
lines = fp.readlines()
data = [int(line.strip()) for line in lines]
fp.close()
m = len(data)
tmaxnum = max(data[m // n * k: m // n * (k + 1)])
nsock.send(str(tmaxnum).encode())
ssock.send('getresult'.encode())
msg = ssock.recv(buffsize)
gmax = int(msg.decode())

res = 1
for num in data[m // n * k: m // n * (k + 1)]:
    if is_p(gmax, num) and num > res:
        res = num
nsock.send(str(res).encode())
nsock.close()
ssock.close()
