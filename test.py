import os
import sys
import threading

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

    for i in range(n):
        arg = 'python ' + main_program_path + ' node ' + str(i + 1) + ' ' + addrs[i + 1] + ' ' + addrs[0]
        t = threading.Thread(target=os.popen, args=(arg,))
        t.start()
        t.join()

    sarg = 'python ' + main_program_path + ' server ' + data_path + ' ' + program_path + ' ' + str(n) + ' ' + address_file
    st = threading.Thread(target=os.popen, args=(sarg,))
    st.start()
    st.join()
