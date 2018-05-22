import os, json, re
from sys import exit

import handler_tweet as ht
import csv
import hashlib
from collections import OrderedDict


# # # # TWITTER  # # # #

class TweetNode():
    def __init__(self, id_tweet, data_tweet, replay):
        self.id = id_tweet
        self.data = data_tweet
        self.in_replay_to = replay
        if data_tweet is None:
            self.data = "{}"

    def __repr__(self):
        return "<ID:%s , in_replay_to:%s>" % (self.id, self.in_replay_to)

    def __str__(self):
        return "<ID:%s , in_replay_to:%s>" % (self.id, self.in_replay_to)


class TwitterParser():
    def __init__(self, path_dir_root, out_path):
        self.root_dir = path_dir_root
        self.output_path = out_path
        self.all_json = []

    def fix_json(self, path_json):
        text_new = []
        print "path: {}".format(path_json)
        with open(path_json, "r") as file:
            text = file.readlines()
        for line_i in text:
            ##print line_i
            if line_i.startswith('{"id":'):
                line_i = line_i.replace(line_i, ",{}".format(line_i))
                text_new.append(line_i)
        text_new[-1] = str(text_new[-1]) + "] }"
        ##print text_new[-1]
        text_new[0] = '{\n"arr_tweets":[\n' + str(text_new[0][1:])
        print text_new[0]
        with open(path_json, "w") as file:
            file.writelines(text_new)

    def get_all_json_file(self):
        res = ht.walk_rec(self.root_dir, [], ".json")
        if len(res) == 0:
            return False
        self.all_json = res
        return True

    def sort_all(self):
        for j_json in self.all_json:
            self.sort_file(j_json, self.output_path)

    def sort_file(self, path_file, path_out=None):
        '''
        sort file by ids and write to the given out path
        :param path_out:  output path, if None then overwrite
        :param path_file: path to the given file to sort
        '''
        name = str(path_file).split('/')[-1]
        data_stream = None
        dico = {}
        try:
            with open(path_file) as f:
                data = f.read()
                data_stream = json.loads(data)
                print "done"
        except IOError as e:
            print "[Error] : path:{}".format(path_file)
            print "I/O error({0}): {1}".format(e.errno, e.strerror)
            return None
        except Exception as e:
            print "[Error] Unexpected error: path:{}".format(path_file)
            print "Unexpected error({0})".format(e.message)
            return None
        for entry in data_stream['arr_tweets']:
            id_i = str(entry['id']).split(':')[-1]
            dico[id_i] = entry
        list_keys = dico.keys()
        list_keys = [long(x) for x in list_keys]
        list_keys = sorted(list_keys)
        print ""
        if path_out is None:
            path_out = '/'.join(str(path_file).split('/')[:-1])
        for ky in list_keys:
            with open('{}/{}'.format(path_out, name), 'a') as outfile:
                if len(str(ky)) < 4:
                    print "errorrr"
                    continue
                outfile.write("{}@#@".format(ky))
                json.dump(dico[str(ky)], outfile)
                outfile.write('\n')
        print "end !!"
        return True

    def parser_by(self, tweet_id=None):
        map_dict = {}
        for file_i in self.all_json:
            data_stream = json.load(open(file_i))
            for entry in data_stream['arr_tweets']:
                id_i = str(entry['id']).split(':')[-1]
                node_i = TweetNode(id_i, entry, None)
                if id_i in map_dict:
                    print "[Error]"
                else:
                    map_dict[id_i] = None
                if 'inReplyTo' in entry:
                    replay_id = str(entry['inReplyTo']).split('/')[-1][:-2]
                    node_i.in_replay_to = replay_id
                map_dict[id_i] = node_i
        return map_dict

    def check_duplication(self, d):
        pass

    def make_list(self, d, arr_tweets=None):
        self.check_duplication(d)
        if arr_tweets is None:
            d_arr = {}
        else:
            d_arr = arr_tweets
        for val in dict(d).keys():
            ans = self.looker(val, d, d_arr)
            if val in d_arr:
                print "dup : {}".format(val)
            d_arr[val] = ans[1:]
        return d_arr

    def looker(self, id, dico, memo):
        ans = []
        if id in memo:
            list_arr = [id]
            list_arr.extend(memo[id])
            return list_arr
        if id is None:
            return [{'EOF': 'True'}]
        if id not in dico:
            return [{"id": id, 'data': 'null(no data)'}]
        if id in dico:
            obj = dico[id]
            new_id = obj.in_replay_to
            ans.extend(self.looker(new_id, dico, memo))
            ans.insert(0, obj.data)
        return ans

    def get_IDs_files(self):
        ou_dir = ht.mkdir_system(self.output_path, 'ids')
        for file_i in self.all_json:
            map_dict = {}
            data_stream = json.load(open(file_i))
            for entry in data_stream['arr_tweets']:
                id_i = long((entry['id']).split(':')[-1])
                node_i = TweetNode(id_i, entry, None)
                if id_i in map_dict:
                    print "[Error]"
                else:
                    map_dict[id_i] = None
                if 'inReplyTo' in entry:
                    replay_id = str(entry['inReplyTo']).split('/')[-1][:-2]
                    node_i.in_replay_to = replay_id
                map_dict[id_i] = node_i
            name_file = str(file_i).split('/')[-1]
            list_sorted = sorted(map_dict.keys())
            with open('{}/{}.txt'.format(ou_dir, name_file), 'w') as file:
                for ky in list_sorted:
                    file.write(repr(map_dict[ky]))
                    file.write('\n')
        return map_dict

    def flush_all(self, map_dict):
        input_hash = ''
        ctr = 0
        end = 5
        for item_i in self.all_json:
            input_hash = '{}{}'.format(input_hash, item_i)
            if ctr > end:
                break
            else:
                ctr += 1
        random_seq = int(hashlib.sha1(input_hash).hexdigest(), 16) % (10 ** 8)
        out_path_dir = ht.mkdir_system(self.output_path, random_seq)
        for val in map_dict.keys():
            flush(map_dict[val], "{}_{}".format(val, len(map_dict[val])), out_path_dir)

    def fixer_json(self):
        """
        fixing the json files in a given path
        """
        self.get_all_json_file()
        for x in self.all_json:
            self.fix_json(x)


def flush(jsons_list, name, dir):
    d = {}
    d['arr'] = jsons_list
    if dir[-1] == '/':
        dir = dir[:-1]
    with open('{}/{}.json'.format(dir, name), 'w') as file:
        file.write(json.dumps(d))


def sort_to_big(all_sort_file, big_file_path):
    arr_pointer = {}
    arr_lines = {}
    is_break = False
    # inital array of pointers
    for file_sort in all_sort_file:
        arr_pointer[file_sort] = open(file_sort, "rw+")
    for f_sort in all_sort_file:
        line_cur = getline(arr_pointer[f_sort])
        arr_lines[f_sort] = line_cur
    while is_break is False:
        ky_val = extract_min(arr_lines)
        if ky_val is None:
            print "[Error] key is None "
            return
        with open("{}/big.json".format(big_file_path), "a") as myfile:
            myfile.write(arr_lines[ky_val])
        new_line = getline(arr_pointer[ky_val])
        if new_line is None or len(new_line) < 1:
            print "del -->{}".format(ky_val)
            del arr_pointer[ky_val]
            del arr_lines[ky_val]
        else:
            arr_lines[ky_val] = new_line
        if len(arr_lines) == 0:
            is_break = True
    print "done !"


def is_sorted(path):
    li = []
    with open("{}".format(path), "r") as myfile:
        for line in myfile:
            num = long(str(line).split('@#@')[0])
            li.append(num)
    if li == sorted(li):
        return True
    else:
        return False


def getline(file_io):
    data_line = file_io.readline()
    return data_line


def extract_min(dico):
    '''
    get the minimal item from all the files
    :return: key of the minimal
    '''
    min = None
    min_val = None
    for ky in dico.keys():
        num = dico[ky].split('@#@')[0]
        if (min is None or min > num) and len(num) > 1:
            min = num
            min_val = ky
    return min_val


def _get_size_file(f_name):
    with open(f_name) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def _get_line(index, f_name, fill, ram):
    size = _get_size_file(f_name)
    if size < index:
        print "xxx"
    f = open(f_name, 'r+')
    for i in xrange(index):
        line = f.readline()
        if fill:
            if i == 0:
                ram[0] = str(line).split('@#@')[0]
            elif i == index - 1:
                ram[1] = str(line).split('@#@')[0]
            ram[2].append(line)
    num = str(line).split('@#@')[0]
    return num, line


def get_replay(jsonstring):
    """
    Extracting replay id from the given json tweet data
    :param json_file: (string)
    :return: id (str) or None
    """
    replay_id = None
    #    with open('/home/ise/Desktop/ex_tweet.json','w') as f :
    #        f.write(jsonstring)
    data_stream = json.loads(jsonstring)
    if 'inReplyTo' in data_stream:
        data_replay = data_stream['inReplyTo']['link']
        replay_id = str(data_replay).split('/')[-1]
    return replay_id


def build_trees(f_name_big, long=5):
    """
    building the trees, by iterate over each record in the big file,
    this function also crate a lookup file for merging nodes in the tree
    :param f_name_big: the big file (sorted file)
    :return: trees files
    """
    ram_big = ["1", "1",[],0,0]
    out_dir = '/'.join(str(f_name_big).split('/')[:-1])
    lookup_dir = ht.mkdir_system(out_dir, 'lookup')
    trees_dir = ht.mkdir_system(out_dir, 'trees')
    log_dir = ht.mkdir_system(out_dir, 'log')
    not_done = True
    big_file = open('{}'.format(f_name_big), 'r')
    dico_data = {}
    start =True
    while not_done:
        if start:
            # for x in range(30000):
            #    cur_line = big_file.readline()
            cur_line = big_file.readline()
            start=False
        cur_line = big_file.readline()
        if cur_line is None or len(cur_line) < 1:
            break
        end_not = True
        while end_not:
            arr_split_data = str(cur_line).split('@#@')
            id_line = arr_split_data[0]
            with open('{}/log.txt'.format(log_dir), 'a') as log_f:
                log_f.write("{}\n".format(id_line))
            json_line = arr_split_data[1]
            dico_data[id_line] = cur_line
            ans = _look_up(id_line, lookup_dir, long)
            if ans is not None:
                flush_tree(dico_data, lookup_dir, trees_dir, long, ans)
                break
            replay = get_replay(json_line)
            if replay is None:
                end_not = False
                flush_tree(dico_data, lookup_dir, trees_dir, long)
                dico_data = {}
                continue
            print "{}->{}".format(id_line, replay)
            print "len: {} {}".format(len(id_line), len(replay))
            replay_data = binary_search(replay, f_name_big, ram_big)
            if replay_data is None:
                flush_tree(dico_data, lookup_dir, trees_dir, long)
                dico_data = {}
                break
            print "found !!!"
            cur_line = replay_data


def _look_up(id, dir_lookup, long):
    file_name = id[:long]
    if os.path.isfile('{}/{}.txt'.format(dir_lookup, file_name)):
        with open('{}/{}.txt'.format(dir_lookup, file_name), 'r') as file_look:
            lines = file_look.readlines()
            for line in lines:
                arr = str(line).split('##')
                if arr[0] == id:
                    return arr[1][:-1]
    return None


def flush_tree(dico_json_data, lookup_dir, trees_dir, long, append_to=None):
    """
    dump the list of json as a file and write to lookup table
    :param arr_json_data:
    :return:
    """
    if append_to is None:
        tree_name = dico_json_data.keys()[0]
    else:
        tree_name = append_to
    for ky_i in dico_json_data.keys():
        file_name = ky_i[:long]
        # The tree file
        with open("{}/{}.txt".format(trees_dir, tree_name), "a") as myfile:
            myfile.write(dico_json_data[ky_i])
            myfile.write('\n')
        # The look-up file
        with open("{}/{}.txt".format(lookup_dir, file_name), "a") as myfile:
            myfile.write('{}##{}'.format(ky_i, tree_name))
            myfile.write('\n')


def binary_search_ram(ram, value):
    if value >= ram[0]:
        if value <= ram[1]:
            high = len(ram[2])
            low = 1
            while low <= high:
                mid = low + (high - low) // 2
                mid_value = str(ram[2][mid]).split('@#@')[0]
                if mid_value == value:
                    return True, str(ram[2][mid])
                elif value > mid_value:
                    low = mid + 1
                else:
                    high = mid - 1
            return True, None
        else:
            if ram[4]==1:
                return True,None
    else:
        if ram[3]==1:
            return True,None
    return False, None


def binary_search(value, path_file, ram, ram_size=10000000):
    """
    ram[0] = min id
    ram[1] = max id
    ram[2] = data

    ram[3]=lower_bound
    ram[4]=upper_bound

    binary search in the big sorted file
    :param value: the target ID
    :param path_file: the path to the data file
    :return: json data or None
    """
    flag, ans = binary_search_ram(ram, value)
    if flag is False:
        miss = True
    else:
        miss = False
        return ans
    size = _get_size_file(path_file)
    high = size
    low = 1
    while low <= high:
        mid = low + (high - low) // 2
        mid_value, data = _get_line(mid, path_file, False, ram)
        if mid_value == value:
            return data
        elif value > mid_value:  # equal_str_number(value, mid_value):
            low = mid + 1
        else:
            high = mid - 1
        # ####### RAM ######### #
        if low + (high - low) <= ram_size:
            _fill_ram(ram,low,high,path_file,size)
            #mid_value, data = _get_line(mid, path_file, True, ram)
            if mid_value == value:
                return data
            else:
                flag, ans = binary_search_ram(ram, value)
                return ans
    return None

def _fill_ram(ram,low,high,path,size):
    print "filling RAM....."
    f = open(path, 'r+')
    ram[3]=0
    ram[4]=0
    if low-1 == 0:
        ram[3]=1
    if high > size-1:
        ram[4]=1
    for i in xrange(high+1):
        line = f.readline()
        if True:
            if i == low-1:
                ram[0] = str(line).split('@#@')[0]
            elif i == high:
                ram[1] = str(line).split('@#@')[0]
            if i >= low and high >= i:
                ram[2].append(line)

# def equal_str_number(x_big,x_small):
#    for j in range(len(x_big)-1,0,-1):
#        if x_big[j]>x_small[j]:
#            return True
#        elif x_big[j]<x_small[j]:
#            return False
#    raise Exception("the same number {} : {}".format(x_big,x_small))


def command_parser(args):
    if len(args) > 1:
        if args[1] == 'fix':
            print "args path: {}".format(args[2])
            TP = TwitterParser(args[2], args[2])
            if TP.get_all_json_file():
                TP.fixer_json()
                print "Done !"
            else:
                print "[Error] with the path: {} ".format(args[2])
            exit(0)
        if args[1] == 'sort':
            print "args path: {}".format(args[2])
            sorted_dir = ht.mkdir_system(args[2], 'sorted')
            TP = TwitterParser(args[2], sorted_dir)
            if TP.get_all_json_file():
                TP.sort_all()
                print "Done !"
            else:
                print "[Error] with the path: {} ".format(args[2])
        if args[1] == 'mk big':
            TP = TwitterParser(args[2], args[3])
            big_dir = ht.mkdir_system(args[3], 'big')
            if TP.get_all_json_file():
                sort_to_big(TP.all_json, big_dir)
        if args[1]=='build':
            print "path: {}".format(args[2])
            build_trees(args[2])



import sys

if __name__ == "__main__":
    # exit()
    # path_p = '/home/ise/NLP/json_twitter/tweetJson/sorted'
    # out_p = '/home/ise/Desktop/nlp_ex/out'
    # sys.argv = ['', 'ex', path_p, out_p]
    command_parser(sys.argv)
