import socket
import os
import json
from filesplit.split import Split
import inspect
import uuid
from DataNode import read_from_node, write_to_node
import sys
import math
import shutil

# max block size is 50MB
max_size = 52428800


def insert_dict(d, keys, keyvalue):
    if len(keys) == 1:
        for k, v in keyvalue.items():
            d[keys[0]][k] = v
    else:
        insert_dict(d[keys[0]], keys[1:], keyvalue)


def drop_dict(d, keys, fname):
    print(keys)
    if len(keys) == 1:
        del d[keys[0]][fname]
    else:
        drop_dict(d[keys[0]], keys[1:], fname)


def find_directory(path):
    f = open('NameNode/records.json')
    d = json.load(f)
    if path != '/':
        segs = path.split('/')
        for i in segs[1:]:
            if i in d:
                d = d[i]
            else:
                return ("Error: Directory does not exist.")
    return d


def ls(dir):
    # dive into the structure and try to find the directory
    # return error message if it or any intermediate directory does not exist
    f = open('NameNode/records.json')
    d = json.load(f)
    if dir != '/':
        segs = dir.split('/')
        for i in segs[1:]:
            if i in d:
                d = d[i]
            else:
                return ("Error: Directory does not exist.")
    # if nothing is in the directory, return nothing
    if len(d) == 0:
        return
    # if it's called by the cat or put function, return the dict object
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    if calframe[1][3] == 'cat' or calframe[1][3] == 'fs':
        return d
    # list items in target dir
    ls = list(d.keys())
    s = ls[0]
    for i in ls[1:]:
        s += '\t'+i
    return s


def rm(path):
    # check if file exists
    ind = path.rfind('/')
    if ind == 0:
        dir = '/'
    else:
        dir = path[:ind]
    fname = path[ind+1:]
    d = find_directory(dir)
    # if file exists
    if isinstance(d, dict) and len(d) > 0 and fname in d.keys():
        # delete record from metadata
        m = json.load(open('NameNode/records.json'))
        if dir != '/':
            segs = dir.split('/')
            drop_dict(m, segs[1:], fname)
        else:
            del m[fname]
        with open('NameNode/records.json', 'w') as f:
            f.write(json.dumps(m, indent=4))
        # delete data saved in Datanodes
        fname = path[ind+1:]
        ftype = fname.split('.')[1]
        blocks = d[fname]
        for b in blocks[:-1]:
            for i in b[1:]:
                os.remove('DataNode'+i+'/'+b[0]+'.'+ftype)
    else:
        return ("Error: File doesn't exist.")


def put(file, newname):
    # check if parent directory exists
    ind = newname.rfind('/')
    if ind != 0:
        dir = newname[:ind]
    else:
        dir = '/'
    d = find_directory(dir)
    # if directory exists, start data transfer
    if isinstance(d, dict):
        f_name = newname[ind+1:]
        f_size = os.path.getsize(file)
        if f_size < 1000:
            size = str(f_size)+' B'
        else:
            size_name = ("B", "KB", "MB", "GB", "TB")
            i = int(math.floor(math.log(f_size, 1000)))
            p = math.pow(1000, i)
            approx = round(f_size / p, 2)
            size = str(approx)+' '+size_name[i]
        blocks = []
        # if file size exceeds block size, split the file into chunks
        if os.path.getsize(file) > max_size:
            os.mkdir('temp')
            Split(file, 'temp').bysize(size=max_size, newline=True)
            # generate unique id for each block and save the
            with open('temp/manifest', 'r') as f:
                next(f)
                for line in f:
                    uid = str(uuid.uuid4())
                    ls = []
                    ls.append((os.path.getsize('DataNode1'), "1"))
                    ls.append((os.path.getsize('DataNode2'), "2"))
                    ls.append((os.path.getsize('DataNode3'), "3"))
                    ls.sort()
                    s1, loc1 = ls[0]
                    write_to_node(uid, line.split(',')[0], loc1, False)
                    s2, loc2 = ls[1]
                    write_to_node(uid, line.split(',')[0], loc2, False)
                    blocks.append([uid, loc1, loc2])
            shutil.rmtree('temp')
        # if chunking is not needed, generate one id for the entire file and create a copy of it on server
        else:
            uid = str(uuid.uuid4())
            ls = []
            ls.append((os.path.getsize('DataNode1'), "1"))
            ls.append((os.path.getsize('DataNode2'), "2"))
            ls.append((os.path.getsize('DataNode3'), "3"))
            ls.sort()
            s1, loc1 = ls[0]
            write_to_node(uid, file, loc1, True)
            s2, loc2 = ls[1]
            write_to_node(uid, file, loc2, True)
            blocks.append([uid, loc1, loc2])
        # add file size as the last list element
        blocks.append([size])
        # update metadata
        v = {f_name: blocks}
        m = json.load(open('NameNode/records.json'))
        # if dir is not the root dir
        if dir != '/':
            segs = dir.split('/')
            insert_dict(m, segs[1:], v)
        # if dir is the root dir, simply add the pair
        else:
            m[f_name] = blocks
        with open('NameNode/records.json', 'w') as f:
            f.write(json.dumps(m, indent=4))
    # report error if dir doesn't exist
    else:
        return ("Error: Directory does not exist.")


def get(file, newname):
    content = cat(file)
    with open(newname, 'w') as f:
        f.write(content)


def mkdir(path):
    # check if user want to create the root directory
    if path == '/':
        return ("Error: Root directory already exists.")
    # check if parent directory exists
    ind = path.rfind('/')
    f_name = path[ind+1:]
    if ind != 0:
        dir = path[:ind]
    else:
        dir = '/'
    d = find_directory(dir)
    # if parent directory exists, add new record for the new folder
    if isinstance(d, dict):
        m = json.load(open('NameNode/records.json'))
        if dir == '/':
            m[f_name] = {}
        else:
            segs = dir.split('/')
            insert_dict(m, segs[1:], {f_name: {}})
        with open('NameNode/records.json', 'w') as f:
            f.write(json.dumps(m, indent=4))
    else:
        return ("Error: Invalid path.")


def rmdir(dir):
    # check if user want to remove the root directory
    if dir == '/':
        return ("Error: Cannot remove root directory.")
    # check if directory exists
    d = find_directory(dir)
    if isinstance(d, dict):
        # if dir is non-empty
        if len(d) != 0:
            return ("Error: Directory is not empty.")
        # remove empty dir
        else:
            ind = dir.rfind('/')
            f_name = dir[ind+1:]
            m = json.load(open('NameNode/records.json'))
            if ind == 0:
                del m[f_name]
            else:
                segs = dir[:ind].split('/')
                drop_dict(m, segs[1:], f_name)
            with open('NameNode/records.json', 'w') as f:
                f.write(json.dumps(m, indent=4))
    else:
        return ("Error: Directory does not exist.")


def cat(file):
    # check if parent directory exists
    ind1 = file.rfind('/')
    d = ls(file[:ind1])
    if isinstance(d, dict):
        # check if file exists
        if file[ind1+1:] in d.keys():
            # get list of lists which store locations of blocks on server
            ind2 = file.rfind('.')
            f_type = file[ind2+1:]
            blocks = d[file[ind1+1:]]
            # concatenate contents in order and return the string
            content = ''
            for b in blocks[:-1]:
                content += read_from_node(b[0], b[1:], f_type)
            return content
    # if parent dir is empty or any dir in the path does not exist, return error message
    return ("Error: File does not exist.")


def fs(file):
    # check if parent directory exists
    ind1 = file.rfind('/')
    d = ls(file[:ind1])
    if isinstance(d, dict):
        # check if file exists
        if file[ind1+1:] in d.keys():
            # get list of lists which store size of file on server
            blocks = d[file[ind1+1:]]
            print(blocks[-1][0])
    # if parent dir is empty or any dir in the path does not exist, return error message
    else:
        return ("Error: File does not exist.")


if __name__ == "__main__":
    # initialize the system
    cwd = os.getcwd()
    if not os.path.exists(cwd+'/NameNode'):
        os.mkdir('NameNode')
        with open(cwd+'/NameNode/records.json', 'w') as f:
            f.write('{}')
            f.close()
    if not os.path.exists(cwd+'/DataNode1'):
        os.mkdir('DataNode1')
    if not os.path.exists(cwd+'/DataNode2'):
        os.mkdir('DataNode2')
    if not os.path.exists(cwd+'/DataNode3'):
        os.mkdir('DataNode3')
    # build connection
    port = 5050
    ip_addr = socket.gethostbyname(socket.gethostname())
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((ip_addr, port))
    server.listen()
    while True:
        conn, addr = server.accept()
        connected = True
        while connected:
            msg_len = conn.recv(30).decode('utf-8', 'ignore')
            if msg_len:
                msg_len = int(msg_len)
                msg = conn.recv(msg_len).decode('utf-8', 'ignore')
                if msg:
                    segs = msg.split(' ')
                    comm = segs[0]
                    if comm == '-ls':
                        resp = ls(segs[1])
                    elif comm == '-rm':
                        resp = rm(segs[1])
                    elif comm == '-put':
                        resp = put(segs[1], segs[2])
                    elif comm == '-get':
                        resp = get(segs[1], segs[2])
                    elif comm == '-mkdir':
                        resp = mkdir(segs[1])
                    elif comm == '-rmdir':
                        resp = rmdir(segs[1])
                    elif comm == '-cat':
                        resp = cat(segs[1])
                    elif comm == '-fs':
                        resp = fs(segs[1])
                    connected = False
                    if not resp:
                        msg_len = '-1'.encode('utf-8')
                        msg_len += b' '*(30-len(msg_len))
                        conn.send(msg_len)
                    else:
                        msg = resp.encode('utf-8')
                        msg_len = str(len(msg)).encode('utf-8')
                        msg_len += b' '*(30-len(msg_len))
                        conn.send(msg_len)
                        conn.send(msg)
                    conn.close()
                    sys.exit()
