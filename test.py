import os
import sys

# 测试脚本
# 'python test.py n'
if __name__ == '__main__':

    address_file = 'address.txt'
    main_program_path = 'server_node.py'
    data_path = 'test_data.txt'
    program_path = 'programtest.py'
    n = int(sys.argv[1])

    with open(address_file) as f:
        lines = f.readlines()
        addrs = [line.strip() for line in lines]

    # 启动节点
    for i in range(n):
        arg = 'python ' + main_program_path + ' node ' + str(i + 1) + ' ' + addrs[i + 1] + ' ' + addrs[0]
        os.popen(arg)

    # 启动服务器
    sarg = 'python ' + main_program_path + ' server ' + data_path + ' ' + program_path + ' ' + str(n) + ' ' + address_file
    f = os.popen(sarg)

    t = f.read()
    print(t)
    f.close()

