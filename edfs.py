from ipaddress import ip_address
import socket
import sys


def main(argvs):
    # build connection
    port = 5050
    ip_addr = socket.gethostbyname(socket.gethostname())
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((ip_addr, port))
    # process command case by case
    if argvs[0] == '-ls':
        pass
    elif argvs[0] == '-rm':
        pass
    elif argvs[0] == '-put':
        pass
    elif argvs[0] == '-get':
        pass
    elif argvs[0] == '-mkdir':
        pass
    elif argvs[0] == '-rmdir':
        pass
    elif argvs[0] == '-cat':
        pass
    else:
        raise ValueError(
            'Invalid command. The system only accepts the following requests: ls, rm, put, get, mkdir, rmdir, and cat.')
    # build connection to server and send command line arguments to the server
    s = ' '.join(argvs)
    msg = s.encode('utf-8')
    msg_len = str(len(msg)).encode('utf-8')
    msg_len += b' '*(30-len(msg_len))
    client.send(msg_len)
    client.send(msg)
    # waiting for server's response, non-empty responses will be printed
    while True:
        msg_len = client.recv(30).decode('utf-8')
        if msg_len:
            msg_len = int(msg_len)
            if msg_len == -1:
                break
            else:
                received = 0
                s = ''
                while received < msg_len:
                    msg = client.recv(2048).decode('utf-8')
                    received += 2048
                    s += msg
                print(s)
                break


# verify number of arguments and then call the main function
if __name__ == '__main__':
    if len(sys.argv) <= 2 or len(sys.argv) > 4:
        raise ValueError(
            'Invalid command.')
    main(sys.argv[1:])
