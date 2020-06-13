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


def max_p(dlist, g_max):
    res = 1
    for num in dlist[m // n * k: m // n * (k + 1)]:
        if is_p(g_max, num) and num > res:
            res = num
    return res

# 数据路径
dpath = sys.argv[1]
# 服务器地址
saddr = sys.argv[2]

buffsize = 2048
port = 13450

# 与服务器建立连接
ssock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
saddress = saddr.split(',')
ssock.connect((saddress[0], int(saddress[1])))

t_addr = ssock.getsockname()

# 请求服务器获取任务编号
ssock.send('get_task_id'.encode())
msg = ssock.recv(buffsize)
k = int(msg.decode())

# 请求服务器获取节点总数
ssock.send('get_task_num'.encode())
msg = ssock.recv(buffsize)
n = int(msg.decode())

# 请求服务器获取
ssock.send('get_special_id'.encode())
msg = ssock.recv(buffsize)
sid = int(msg.decode())

# 读取数据
fp = open(dpath, 'r')
lines = fp.readlines()
data = [int(line.strip()) for line in lines]
fp.close()

# 计算对应局部最大值
m = len(data)
local_data = data[m // n * k: m // n * (k + 1)]
tmaxnum = max(local_data)


# with open(str(k) + 'debug.txt', 'w') as f:
#     f.write(sp_ip)
#     f.write('\n')
#     f.write(t_addr[0])

if sid == k:
    temp_res = [tmaxnum]
    task_sockets = []

    temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    temp_sock.bind((t_addr[0], port))
    temp_sock.listen(n - 1)

    for i in range(n - 1):
        s, _ = temp_sock.accept()
        msg = s.recv(buffsize)
        temp_res.append(int(msg.decode()))
        task_sockets.append(s)

    global_max_number = max(temp_res)

    for i in range(n - 1):
        task_sockets[i].send(str(global_max_number).encode())

    m_p = max_p(local_data, global_max_number)

    temp_res.clear()
    temp_res.append(m_p)
    for i in range(n - 1):
        msg = task_sockets[i].recv(buffsize)
        temp_res.append(int(msg.decode()))
        task_sockets[i].close()

    global_max_prime = max(temp_res)
    ssock.send('send_global_max_number'.encode())
    msg = ssock.recv(buffsize)
    ssock.send(str(global_max_number).encode())
    msg = ssock.recv(buffsize)

    ssock.send('send_global_max_prime'.encode())
    msg = ssock.recv(buffsize)
    ssock.send(str(global_max_prime).encode())
    msg = ssock.recv(buffsize)

else:
    # 请求服务器获取
    ssock.send('get_special_ip'.encode())
    msg = ssock.recv(buffsize)
    sp_ip = msg.decode()

    ksock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ksock.connect((sp_ip, port))
    ksock.send(str(tmaxnum).encode())
    msg = ksock.recv(buffsize)
    global_max_number = int(msg.decode())

    m_p = max_p(local_data, global_max_number)

    ksock.send(str(m_p).encode())
    ksock.close()


# # 与指定任务进程建立连接
# nsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# naddress = naddr.split(',')
# nsock.connect((naddress[0], int(naddress[1])))
#
# # 向指定任务进程发送局部最大值
# msg = nsock.recv(buffsize)
# nsock.send(str(tmaxnum).encode())
#
# # 请求服务器获取全局最大值
# ssock.send('get_global_max_number'.encode())
# msg = ssock.recv(buffsize)
# gmax = int(msg.decode())
# ssock.close()

# 计算局部最大互质数

ssock.send('quit'.encode())
ssock.close()
# # 向节点发送局部最大互质数
# msg = nsock.recv(buffsize)
# nsock.send(str(res).encode())
#
# msg = nsock.recv(buffsize)
# nsock.close()

