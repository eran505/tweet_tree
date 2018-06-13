import os, json, re
from sys import exit

import handler_tweet as ht
import csv
import hashlib

import sys


def clean_string(string, prefix_symbol):
    ctr = 0
    for x in string:
        if x == prefix_symbol:
            break
        else:
            ctr += 1
    return string[ctr:]


def convert(string):
    if string is None:
        return None
    else:
        return long(string)


def getline(file_io):
    data_line = file_io.readline()
    while data_line is not None and len(str(data_line)) < 20:
        data_line = file_io.readline()
        if len(str(data_line)) < 1:
            return data_line
        print "->{}".format(data_line)
    # data_line = clean_string(data_line,'{')
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


class Parser:

    def __init__(self, dir_path, out_path):
        self.root_dir = dir_path
        self.out_dir = out_path
        self.files = None

    def decompress_file(self, file_path, out_file):
        """
        decompressed the file
        :param file_path:
        :return:
        """
        file_name = [x for x in str(file_path).split('/')[-1].split('.')[:-1] if x != 'json']
        file_name = '.'.join(file_name)
        if out_file is None:
            out_file = '/'.join(str(file_path).split('/')[:-1])
        if str(file_path).endswith('.gz'):
            command = "gunzip {} -c > {}/{}.json".format(file_path, out_file, file_name)
            os.system(command)
        elif str(file_path).endswith('.tar'):
            command = 'tar -xvzf {} -C {}/{}.json'.format(file_path, out_file, file_name)
            os.system(command)
        elif str(file_path).endswith('.zip'):
            command = 'unzip {} -d {}/{}.json'.format(file_path, out_file, file_name)
            os.system(command)
        else:
            print "[Error] cant find a macthing suffix"
            return

    def fix_json(self, path_json):
        '''
        fix the given json file
        :param path_json: path to the json file
        :return: None
        '''
        text_new = []
        print "path: {}".format(path_json)
        with open(path_json, "r") as file:
            text = file.readlines()
        for line_i in text:
            ##print line_i
            if line_i.startswith('{"created_at":'):
                line_i = line_i.replace(line_i, ",{}".format(line_i))
                text_new.append(line_i)
        text_new[-1] = str(text_new[-1]) + "] }"
        ##print text_new[-1]
        text_new[0] = '{"arr_tweets":[\n' + str(text_new[0][1:])
        with open(path_json, "w") as file:
            file.writelines(text_new)

    def get_all_files(self, dir_path=None):
        """
        get all files in the root dir for porcessing
        :return:
        """
        if dir_path is None:
            target_path = self.root_dir
        else:
            target_path = dir_path
        list_files_found = ht.walk_rec(target_path, [], '', lv=-2)
        if len(list_files_found) > 0:
            self.files = list_files_found
        else:
            print "[Error] cant find any file in the given dir {}".format(self.root_dir)

    def read_and_process(self, file_i):
        """
        reading and loading to the RAM the json file
        :param file_i:
        :return:
        """
        data_stream = None
        if str(file_i).endswith('json') is False:
            print " [Error] not a json file, the suffix is not .json --> {}".format(file_i)
            return None
        try:
            data_stream = json.load(open(file_i))
        except ValueError:
            print 'Decoding JSON has failed'
            return None
        for entry in data_stream['arr_tweets']:
            id_i = convert(entry['id_str'])
            reply_to_user_id_str = convert(entry['in_reply_to_user_id_str'])
            reply_to_status_id_str = convert(entry['in_reply_to_status_id_str'])
            print "{} <-- {} : {}".format(id_i, reply_to_status_id_str, reply_to_user_id_str)
        print "good"
        return

    def sort_file(self, path_file, path_out=None):
        """
        sort file by ids and write to the given out path
        :param path_out:  output path, if None then overwrite
        :param path_file: path to the given file to sort
        """
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
            id_i = str(entry['id'])
            dico[id_i] = entry
        list_keys = dico.keys()
        list_keys = [long(x) for x in list_keys]
        list_keys = sorted(list_keys)
        print ""
        if path_out is None:
            path_out = '/'.join(str(path_file).split('/')[:-1])
        print "the path sorted : {}/{}".format(path_out, name)
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

    def constract_fix_json_dir(self):
        '''
        :return:
        '''
        self.get_all_files()
        for file_gz in self.files:
            self.decompress_file(file_gz, self.out_dir)
        self.get_all_files(self.out_dir)
        for file_i in self.files:
            self.fix_json(file_i)
        self.get_all_files(self.out_dir)
        self.get_all_files(self.out_dir)
        p_path = ht.mkdir_system(self.out_dir, 'sorted')
        for x in self.files:
            self.sort_file(x, p_path)
        print "done !"

    def sort_to_big(self, all_sort_file, big_file_path):
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

    def make_big_json(self, path_sorted, path_out):
        """
        building the big file (merge all the sorted file into one big file)
        :param path_sorted:
        :param path_out:
        :return:
        """
        big_dir = ht.mkdir_system(path_out, 'big')
        self.get_all_files(path_sorted)
        self.sort_to_big(self.files, big_dir)

    def full_process(self):
        self.constract_fix_json_dir()
        if self.out_dir[-1] == '/':
            sort_p = '{}sorted'.format(self.out_dir)
            big_p = '{}big'.format(self.out_dir)
        else:
            big_p = '{}/big'.format(self.out_dir)
            sort_p = '{}/sorted'.format(self.out_dir)
        self.make_big_json(sort_p,self.out_dir)
        print "done !!"
        build_trees("{}/big.json".format(big_p))

def build_trees(f_name_big, long=5):
    """
    building the trees, by iterate over each record in the big file,
    this function also crate a lookup file for merging nodes in the tree
    :param f_name_big: the big file (sorted file)
    :return: trees files
    """
    ram_big = ["1", "1", [], 0, 0]
    out_dir = '/'.join(str(f_name_big).split('/')[:-1])
    lookup_dir = ht.mkdir_system(out_dir, 'lookup')
    trees_dir = ht.mkdir_system(out_dir, 'trees')
    log_dir = ht.mkdir_system(out_dir, 'log')
    user_dir = ht.mkdir_system(out_dir, 'usr')
    not_done = True
    big_file = open('{}'.format(f_name_big), 'r')
    dico_data = {}
    start = True
    while not_done:
        if start:
            #for x in range(30000):
            #    cur_line = big_file.readline()
            start = False
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
    if 'in_reply_to_status_id_str' in data_stream:
        data_replay = data_stream['in_reply_to_status_id_str']
        replay_id = str(data_replay)
    return replay_id


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


def binary_search(value, path_file, ram, ram_size=30000000):
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
            _fill_ram(ram, low, high, path_file, size)
            # mid_value, data = _get_line(mid, path_file, True, ram)
            if mid_value == value:
                return data
            else:
                flag, ans = binary_search_ram(ram, value)
                return ans
    return None


def _fill_ram(ram, low, high, path, size):
    print "filling RAM....."
    f = open(path, 'r+')
    ram[3] = 0
    ram[4] = 0
    if low - 1 == 0:
        ram[3] = 1
    if high > size - 1:
        ram[4] = 1
    for i in xrange(high + 1):
        line = f.readline()
        if True:
            if i == low - 1:
                ram[0] = str(line).split('@#@')[0]
            elif i == high:
                ram[1] = str(line).split('@#@')[0]
            if i >= low and high >= i:
                ram[2].append(line)


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
            if ram[4] == 1:
                return True, None
    else:
        if ram[3] == 1:
            return True, None
    return False, None

def parser_command():
    args = sys.argv
    if len(args) < 2:
        print 'no path was given'
        print "python parser_twitter [path_zip] [path_out] [ram_size=10M]"
        return
    else:
        if args[1] == 'big':
            build_trees(args[2])
            return
        p_pars = Parser(args[1], args[2])
        p_pars.full_process()


if __name__ == "__main__":
    print "starting..."
    parser_command()
    #path_in = '/home/ise/NLP/oren_data/DATA'
    #path_out = '/home/ise/NLP/oren_data/out'
    #path_big = '/home/ise/NLP/oren_data/out/big/big.json'
    #########################
    # Testing big function :
    #build_trees(path_big)
    ########################
    #p_pars = Parser(path_in, path_out)
    #p_pars.constract_fix_json_dir()
    #p_pars.make_big_json('/home/ise/NLP/oren_data/out/sorted', '/home/ise/NLP/oren_data/out')
    exit(0)
