import sys, json, os
import handler_tweet as ht
import treelib
import pandas as pd


class TweetNode():
    def __init__(self, id_tweet, replay=None):
        self.id = id_tweet
        self.data = None
        self.in_replay_to = None
        self.in_replay_to_object = None
        self.comment = None
        if replay is not None:
            self.in_replay_to = replay

    def __repr__(self):
        return "<ID:%s , in_replay_to:%s>" % (self.id, self.in_replay_to)

    def __str__(self):
        return "<ID:%s , in_replay_to:%s>" % (self.id, self.in_replay_to)


def get_hash_json_size(file_path):
    '''
    get the size of json var in a file
    :param big_p: path to the file
    :return:  ( int ) size
    '''
    d = {}
    size = 0
    with open(file_path, 'r+') as big_f:
        for line in big_f:
            if line == '\n':
                continue
            size = size + 1
    return size


def tree_lib_bulider(json_p):
    pass


def json_tree(json_p):
    '''
    constract a tree out of a json file
    :param json_p: path to the json file
    :return: tree dict
    '''
    name = str(json_p).split('/')[-1][:-4]
    if os.path.isfile(json_p) is False:
        print "bad path"
        return None
    with open(json_p, 'r') as j_file:
        arr = j_file.readlines()
    d = {}
    for line in arr:
        if line == '\n':
            continue
        j_data = str(line)
        twet_id = get_filed(j_data, 'id_str')
        rep_id = get_filed(j_data)
        if twet_id in d:
            msg = "[Error] the id is in the dict already"
            raise Exception(msg)
        d[twet_id] = TweetNode(twet_id, rep_id)
    for ky in d.keys():
        if d[ky].in_replay_to not in d and d[ky].in_replay_to != 'None':
            d[d[ky].in_replay_to]=TweetNode(d[ky].in_replay_to, 'None')
    for ky in d.keys():
        node_obj = d[ky]
        if node_obj.in_replay_to != 'None':
            node_obj.in_replay_to_object = d[node_obj.in_replay_to]
        for ky_sec, val in d.iteritems():
            if val.in_replay_to == ky:
                if node_obj.comment == None:
                    node_obj.comment = []
                node_obj.comment.append(d[ky_sec])
    root = find_root(d)
    dico = get_level(root)
    dico['nodes_size'] = len(d)
    return dico


def find_root(dict_tree):
    '''
    finding the root of the given tree
    :param dict_tree:
    :return:
    '''
    set_father = set()
    for ky in dict_tree.keys():
        first_element = dict_tree[ky]
        father = first_element
        while father is not None:
            new_node = father.in_replay_to_object
            if new_node is None:
                break
            else:
                father = new_node
        set_father.add(father.id)
    print list(set_father)
    set_father = list(set_father)
    return dict_tree[set_father[0]]


def get_filed(jsonstring, val='in_reply_to_status_id_str'):
    """
    Extracting replay id from the given json tweet data
    :param jsonstring: (string)
    :return: id (str) or None
    """
    replay_id = None
    data_stream = json.loads(jsonstring)
    if val in data_stream:
        data_replay = data_stream[val]
        replay_id = str(data_replay)
    return replay_id


def get_level(root):
    '''
    get the stat of each tree
    :param root: root node
    :return: dict
    '''
    d = {}
    data_lv = []
    rec_path(root, 0, data_lv)
    num_of_leaf = len(data_lv)
    avg = reduce(lambda x, y: x + y, data_lv) / float(len(data_lv))
    highest = max(data_lv)
    d['num_leaf']=num_of_leaf
    d['max_depth'] = highest
    d['avg_depth'] = avg
    d['med_depth'] = median(data_lv)
    data_lv = []
    rec_comment(root, data_lv)
    avg = reduce(lambda x, y: x + y, data_lv) / float(len(data_lv))
    highest = max(data_lv)
    d['max_branch'] = highest
    d['avg_branch'] = avg
    d['med_branch'] = median(data_lv)
    return d

def rec_comment(node, data):
    if node.comment is None:
        data.append(0)
        return
    if node.comment is not None:
        data.append(len(node.comment))
    for item in node.comment:
        rec_comment(item, data)


def rec_path(node, lv, data):
    if node.comment is None:
        data.append(lv)
        return
    for item in node.comment:
        rec_path(item, lv + 1, data)


def median(lst):
    '''
    get the median in a list
    :param lst: list
    :return: var
    '''
    sortedLst = sorted(lst)
    lstLen = len(lst)
    index = (lstLen - 1) // 2

    if (lstLen % 2):
        return sortedLst[index]
    else:
        return (sortedLst[index] + sortedLst[index + 1]) / 2.0


def get_stat(dir_tree, flag=False, num=7):
    '''
    This functopn go over all the tree in the given dir and output statistic about them
    :param dir_tree: dir path (str)
    :param flag: limit the size of the tree e.g. only trees that contains 3 or more nodes will be processed (True/False)
    :param num: the limit node in the tree (int)
    :return: None
    '''
    out = '/'.join(str(dir_tree).split('/')[:-1])
    all_tree = ht.walk_rec(dir_tree, [], '.txt')
    d_list = []
    if os.path.isfile('{}/log_stat.txt'.format(out)):
        os.system('rm {}/log_stat.txt'.format(out))
    size = float(len(all_tree))
    ctr = 0
    for tree_file in all_tree:
        ctr+=1
        print "{}%".format(ctr/size * 100)
        #print '{}'.format(tree_file)
        name = str(tree_file).split('/')[-1][:-4]
        with open('{}/log_stat.txt'.format(out) ,'a') as f:
            f.write('{}.txt \n'.format(name))
        if flag == True:
            ans = get_hash_json_size(tree_file)
            if ans < num:
                continue
        dico_i = json_tree(tree_file)
        dico_i['tree_name'] = name
        d_list.append(dico_i)
    df = pd.DataFrame(d_list)
    df.to_csv("{}/stat.csv".format(out))
    print "---> done!!!"


def command_parser():
    args = sys.argv
    ##args = ['','stat','/home/ise/NLP/tran']
    if args[1] == 't':
        dico_i = json_tree(args[2])
    if args[1] == 'stat':
        if len(args) == 3:
            get_stat(args[2])
        elif len(args) == 4:
            if args[3] == 't':
                get_stat(args[2], True)
            else:
                get_stat(args[2], False)
        elif len(args) == 5:
            if args[3] == 't':
                get_stat(args[2], True, int(args[4]))
            else:
                get_stat(args[2], False, int(args[4]))

def load_csvs(path_csv,lim_size=4):
    import pandas as pd
    df = pd.read_csv(path_csv)
    df_filter = df.loc[df['nodes_size'] > lim_size]
    path_p = '/'.join(str(path_csv).split('/')[:-1])
    df_filter.to_csv('{}/filter_{}.csv'.format(path_p,lim_size))

if __name__ == "__main__":
    #load_csvs('/home/ise/NLP/stat.csv')
    command_parser()
    exit()
