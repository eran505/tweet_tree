import os, re,json


def walk_rec(root, list_res, rec="", file_t=True, lv=-800, full=True):
    size = 0
    ctr = 0
    class_list = list_res
    if lv == 0:
        return list_res
    lv += 1
    for path, subdirs, files in os.walk(root):
        ctr += 1
        if file_t:
            for name in files:
                tmp = re.compile(rec).search(name)
                if tmp == None:
                    continue
                size += 1
                if full:
                    class_list.append(os.path.join(path, name))
                else:
                    class_list.append(str(name))
        else:
            for name in subdirs:
                tmp = re.compile(rec).search(name)
                if tmp == None:
                    continue
                size += 1
                if full:
                    class_list.append(os.path.join(path, name))
                else:
                    class_list.append(str(name))
        for d_dir in subdirs:
            walk_rec("{}/{}".format(path, d_dir), class_list, rec, file_t, lv, full)
        break
    return class_list


def mkdir_system(path_root, name, is_del=True):
    if path_root is None:
        raise Exception("[Error] passing a None path --> {}".format(path_root))
    if path_root[-1] != '/':
        path_root = path_root + '/'
    if os.path.isdir("{}{}".format(path_root, name)):
        if is_del:
            os.system('rm -r {}{}'.format(path_root, name))
        else:
            print "{}{} is already exist".format(path_root, name)
            return '{}{}'.format(path_root, name)
    os.system('mkdir {}{}'.format(path_root, name))
    return '{}{}'.format(path_root, name)


def partition(array, begin, end):
    pivot = begin
    for i in xrange(begin + 1, end + 1):
        if array[i] <= array[begin]:
            pivot += 1
            array[i], array[pivot] = array[pivot], array[i]
    array[pivot], array[begin] = array[begin], array[pivot]
    return pivot


def quicksort(array, begin=0, end=None):
    if end is None:
        end = len(array) - 1

    def _quicksort(array, begin, end):
        if begin >= end:
            return
        pivot = partition(array, begin, end)
        _quicksort(array, begin, pivot - 1)
        _quicksort(array, pivot + 1, end)

    return _quicksort(array, begin, end)


def json_tree(json_p,out):
    '''
    constract a tree out of a json file
    :param json_p: path to the json file
    :return: tree dict
    '''
    name = str(json_p).split('/')[-1]
    if os.path.isfile(json_p) is False:
        print "bad path"
        return None
    with open(json_p, 'r' ) as j_file:
        arr = j_file.readlines()
    d={}
    for line in arr:
        if line == '\n':
            continue
        split_data = str(line).split('@#@')
        d[split_data[0]]=[get_replay(split_data[1])]
    with open('{}/{}.txt'.format(out,name),'w') as f :
        for ky in d.keys():
            f.write('{}:{}'.format(ky,d[ky]))
    print 'done'


def get_replay(jsonstring):
    """
    Extracting replay id from the given json tweet data
    :param jsonstring: (string)
    :return: id (str) or None
    """
    replay_id = None
    data_stream = json.loads(jsonstring)
    if 'in_reply_to_status_id_str' in data_stream:
        data_replay = data_stream['in_reply_to_status_id_str']
        replay_id = str(data_replay)
    return replay_id


import sys
if __name__ == "__main__":
    args = sys.argv
    if args[1] == 't':
        json_tree(args[2],args[3])
    exit()
    print '------handler-----'
