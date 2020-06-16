import os
import sys

# 测试脚本 n为节点数
# 'python test.py n'
if __name__ == '__main__':

    server_address = '127.0.0.1,12300'
    main_program_path = 'server_node.py'
    data_path = 'matrix_data1024.txt'
    program_path = 'programtest.py'
    n = int(sys.argv[1])

    # 启动服务器
    sarg = 'python ' + main_program_path + ' server ' + data_path + ' ' + program_path + ' ' + str(
        n) + ' ' + server_address
    f = os.popen(sarg)

    # 启动节点
    for i in range(n):
        arg = 'python ' + main_program_path + ' node ' + str(i + 1) + ' ' + server_address
        os.popen(arg)

    t = f.read()
    print(t)
    f.close()

