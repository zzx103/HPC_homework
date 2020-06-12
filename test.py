import os
import sys

# 测试脚本
# 'address.txt server_node.py test_data.txt programtest.py n'
if __name__ == '__main__':
    address_file = sys.argv[1]
    main_program_path = sys.argv[2]
    data_path = sys.argv[3]
    program_path = sys.argv[4]
    n = int(sys.argv[5])

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

