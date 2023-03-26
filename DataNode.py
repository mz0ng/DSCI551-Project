import os


def write_to_node(block, file, datanode, local):
    ind = file.rfind('.')
    f_type = file[ind+1:]
    # if file is not split into chunks
    if local:
        path = file
    else:
        path = os.getcwd()+'/temp/'+file
    with open(path, 'r') as f:
        data = f.read()
        with open('DataNode'+datanode+'/'+block+'.'+f_type, 'w') as new_f:
            new_f.write(data)


def read_from_node(block, datanodes, type):
    for i in datanodes:
        try:
            with open("DataNode"+i+"/"+block+"."+type) as f:
                content = f.read()
                return content
        except:
            pass
    return
