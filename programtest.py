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


def read_data(datapath):
    with open(datapath, 'r') as fp:
        lines = fp.readlines()
        d = [int(line.strip()) for line in lines]
        return d


if __name__ == '__main__':
    # 数据路径
    dpath = sys.argv[1]
    # 服务器地址
    saddr = sys.argv[2]

    buffsize = 2048
    # 与其他任务进程通信端口
    port = 13450

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

    # 请求服务器获取特殊节点编号
    ssock.send('get_special_id'.encode())
    msg = ssock.recv(buffsize)
    sid = int(msg.decode())

    # 读取数据
    data = read_data(dpath)

    # 计算对应局部最大值
    m = len(data)
    local_data = data[m // n * k: m // n * (k + 1)]
    tmaxnum = max(local_data)

    # 特殊节点
    if sid == k:
        temp_res = [tmaxnum]
        task_sockets = []

        t_addr = ssock.getsockname()
        temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        temp_sock.bind((t_addr[0], port))
        temp_sock.listen(n - 1)

        for i in range(n - 1):
            # 等待一般节点连接
            s, _ = temp_sock.accept()
            # 接收局部最大值
            msg = s.recv(buffsize)
            temp_res.append(int(msg.decode()))
            task_sockets.append(s)

        # 计算全局最大值
        global_max_number = max(temp_res)

        # 向一般节点发送全局最大值
        for i in range(n - 1):
            task_sockets[i].send(str(global_max_number).encode())

        # 计算局部最大互质数
        lmp = max_p(local_data, global_max_number)

        temp_res.clear()
        temp_res.append(lmp)

        for i in range(n - 1):
            # 接收一般节点局部最大互质数
            msg = task_sockets[i].recv(buffsize)
            temp_res.append(int(msg.decode()))
            task_sockets[i].close()

        # 计算全局最大互质数
        global_max_prime = max(temp_res)

        # 向服务器发送全局最大值
        ssock.send('send_global_max_number'.encode())
        msg = ssock.recv(buffsize)
        ssock.send(str(global_max_number).encode())
        msg = ssock.recv(buffsize)

        # 向服务器发送全局最大互质数
        ssock.send('send_global_max_prime'.encode())
        msg = ssock.recv(buffsize)
        ssock.send(str(global_max_prime).encode())
        msg = ssock.recv(buffsize)

    # 一般节点
    else:
        # 请求服务器获取特殊节点ip
        ssock.send('get_special_ip'.encode())
        msg = ssock.recv(buffsize)
        sp_ip = msg.decode()

        # 与特殊节点建立连接
        ksock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ksock.connect((sp_ip, port))

        # 向特殊节点发送局部最大值
        ksock.send(str(tmaxnum).encode())

        # 获取全局最大值
        msg = ksock.recv(buffsize)
        global_max_number = int(msg.decode())

        # 计算局部最大互质数
        m_p = max_p(local_data, global_max_number)

        # 发送局部最大互质数
        ksock.send(str(m_p).encode())
        ksock.close()

    ssock.send('quit'.encode())
    ssock.close()

