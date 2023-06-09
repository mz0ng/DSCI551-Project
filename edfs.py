from ipaddress import ip_address
import socket
import sys
import os
import pathlib
import subprocess
import time


def main(argvs):
    # build connection
    port = 5050
    ip_addr = socket.gethostbyname(socket.gethostname())
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    client.connect((ip_addr, port))
    # process command case by case
    if argvs[0] == '-ls':
        if len(argvs) > 2:
            print(
                'Error: Please follow the format: -ls [path to directory on server]')
            client.close()
            sys.exit()
        else:
            f = argvs[1]
            ind = f.rfind('.')
            if ind != -1:
                print('Error: Please specify path to a directory instead of file.')
                client.close()
                sys.exit()
    elif argvs[0] == '-rm':
        if len(argvs) > 2:
            print('Error: Please follow the format: -rm [path to file]')
            client.close()
            sys.exit()
        else:
            f = argvs[1]
            ind = f.rfind('.')
            if ind == -1:
                print('Error: Please specify path to a file instead of directory.')
                client.close()
                sys.exit()
    elif argvs[0] == '-put':
        if len(argvs) != 3:
            print(
                'Error: Please follow the format: -put [path to file on local machine] [path to file on server]')
            client.close()
            sys.exit()
        else:
            ind1 = argvs[1].rfind('.')
            ind2 = argvs[2].rfind('.')
            if ind1 == -1 or ind2 == -1:
                print('Error: Please specify path to a file instead of directory.')
                client.close()
                sys.exit()
            elif not os.path.isfile(argvs[1]):
                print('Error: Failed to find file on your local machine.')
                client.close()
                sys.exit()
            else:
                exts1 = pathlib.Path(argvs[1]).suffix
                exts2 = pathlib.Path(argvs[2]).suffix
                if exts1 != exts2:
                    print('Error: Please make sure the file extensions match.')
                    client.close()
                    sys.exit()
    elif argvs[0] == '-get':
        if len(argvs) != 3:
            print(
                'Error: Please follow the format: -get [path to file on server] [path to file on local machine]')
            client.close()
            sys.exit()
        else:
            ind1 = argvs[1].rfind('.')
            ind2 = argvs[2].rfind('.')
            ind3 = argvs[2].rfind('/')
            if ind1 == -1 or ind2 == -1:
                print('Error: Please specify path to a file instead of directory.')
                client.close()
                sys.exit()
            elif not os.path.exists(argvs[2][:ind3]):
                print('Error: Please specify a valid location to save your file.')
                client.close()
                sys.exit()
            else:
                exts1 = pathlib.Path(argvs[1]).suffix
                exts2 = pathlib.Path(argvs[2]).suffix
                if exts1 != exts2:
                    print('Error: Please make sure the file extensions match.')
                    client.close()
                    sys.exit()
    elif argvs[0] == '-mkdir':
        if len(argvs) != 2:
            print(
                'Error: Please follow the format: -mkdir [path to directory on server]')
            client.close()
            sys.exit()
        else:
            f = argvs[1]
            ind = f.rfind('.')
            if ind != -1:
                print('Error: Please specify path to a directory instead of file.')
                client.close()
                sys.exit()
    elif argvs[0] == '-rmdir':
        if len(argvs) != 2:
            print(
                'Error: Please follow the format: -rmdir [path to directory on server]')
            client.close()
            sys.exit()
        else:
            f = argvs[1]
            ind = f.rfind('.')
            if ind != -1:
                print('Error: Please specify path to a directory instead of file.')
                client.close()
                sys.exit()
    elif argvs[0] == '-cat':
        if len(argvs) != 2:
            print(
                'Error: Please follow the format: -cat [path to file on server]')
            client.close()
            sys.exit()
        else:
            f = argvs[1]
            ind = f.rfind('.')
            if ind == -1:
                print('Error: Please specify path to a file instead of directory.')
                client.close()
                sys.exit()
    elif argvs[0] == '-fs':
        if len(argvs) != 2:
            print(
                'Error: Please follow the format: -fs [path to file on server]')
            client.close()
            sys.exit()
        else:
            f = argvs[1]
            ind = f.rfind('.')
            if ind == -1:
                print('Error: Please specify path to a file instead of directory.')
                client.close()
                sys.exit()
    else:
        print('Error: Invalid command. The system only accepts the following requests: ls, rm, put, get, mkdir, rmdir, cat, and fs.')
        client.close()
        sys.exit()
    # build connection to server and send command line arguments to the server
    s = ' '.join(argvs)
    msg = s.encode('utf-8')
    msg_len = str(len(msg)).encode('utf-8')
    msg_len += b' '*(30-len(msg_len))
    client.send(msg_len)
    client.send(msg)
    # waiting for server's response, non-empty responses will be printed
    while True:
        msg_len = client.recv(30).decode('utf-8', 'ignore')
        if msg_len:
            msg_len = int(msg_len)
            if msg_len == -1:
                break
            else:
                received = 0
                s = ''
                while received < msg_len:
                    msg = client.recv(2048).decode('utf-8', 'ignore')
                    received += 2048
                    s += msg
                print(s)
                client.close()
                break


# verify number of arguments and then call the main function
if __name__ == '__main__':
    if len(sys.argv) <= 2 or len(sys.argv) > 4:
        print('Error: Invalid command.')
    p = subprocess.Popen(["python", "server.py"])
    time.sleep(0.8)
    main(sys.argv[1:])
