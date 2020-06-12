# python3.7
import socket
import sys

# 判断是否互质
def is_p(m, n):
    q = 0
    a = m
    b = n
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

# 数据路径
dpath = sys.argv[1]
# 服务器地址
saddr = sys.argv[2]
# 节点地址
naddr = sys.argv[3]

buffsize = 2048

# 与服务器建立连接
ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
saddress = saddr.split(',')
ssock.connect((saddress[0], int(saddress[1])))

# 请求服务器获取任务编号
ssock.send('get_task_id'.encode())
msg = ssock.recv(buffsize)
k = int(msg.decode())

# 请求服务器获取节点总数
ssock.send('get_task_num'.encode())
msg = ssock.recv(buffsize)
n = int(msg.decode())

# 读取数据
fp = open(dpath, 'r')
lines = fp.readlines()
data = [int(line.strip()) for line in lines]
fp.close()

# 计算对应局部最大值
m = len(data)
tmaxnum = max(data[m // n * k: m // n * (k + 1)])

# 与节点建立连接
nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
naddress = naddr.split(',')
nsock.connect((naddress[0], int(naddress[1])))

# 向节点发送局部最大值
msg = nsock.recv(buffsize)
nsock.send(str(tmaxnum).encode())

# 请求服务器获取全局最大值
ssock.send('get_global_max_number'.encode())
msg = ssock.recv(buffsize)
gmax = int(msg.decode())
ssock.close()

# 计算局部最大互质数
res = 1
for num in data[m // n * k: m // n * (k + 1)]:
    if is_p(gmax, num) and num > res:
        res = num

# 向节点发送局部最大互质数
msg = nsock.recv(buffsize)
nsock.send(str(res).encode())

msg = nsock.recv(buffsize)
nsock.close()

