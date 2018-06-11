import os, json, re
from sys import exit

import handler_tweet as ht
import csv
import hashlib

import sys

def clean_string(string, prefix_symbol):
    ctr=0
    for x in string:
        if x==prefix_symbol:
            break
        else:
            ctr+=1
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
        if len(str(data_line)) < 1 :
            return data_line
        print "->{}".format(data_line)
    #data_line = clean_string(data_line,'{')
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
        p_path = ht.mkdir_system(path_out, 'sorted')
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


if __name__ == "__main__":
    print "starting..."
    path_in = '/home/ise/NLP/oren_data/DATA'
    path_out = '/home/ise/NLP/oren_data/out'
    p_pars = Parser(path_in, path_out)
    p_pars.constract_fix_json_dir()

    p_pars.sort_to_big(p_pars.files,p_pars.out_dir)
