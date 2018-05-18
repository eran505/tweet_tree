import os, json, re
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
            self.sort_file(j_json,self.output_path)
    def sort_file(self,path_file,path_out=None):
        '''
        sort file by ids and write to the given out path
        :param path_out:  output path, if None then overwrite
        :param path_file: path to the given file to sort
        '''
        name = str(path_file).split('/')[-1]
        data_stream = None
        dico={}
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
            dico[id_i]=entry
        list_keys = dico.keys()
        list_keys = [long(x) for x in list_keys]
        list_keys = sorted(list_keys)
        print ""
        if path_out is None:
            path_out = '/'.join(str(path_file).split('/')[:-1])
        for ky in list_keys:
            with open('{}/{}'.format(path_out,name), 'a') as outfile:
                if len(str(ky))<4:
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
            map_dict={}
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
            with open('{}/{}.txt'.format(ou_dir,name_file), 'w') as file:
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

def sort_to_big(all_sort_file):
    arr_pointer={}
    #inital array of pointers
    for file_sort in all_sort_file:
        arr_pointer[file_sort]=0

def extract_min():
    '''
    get the minimal item from all the files
    :return: key of the minimal
    '''
    
    pass
def command_parser(args):
    if len(args)>1:
        if args[1] =='fix':
            print "args path: {}".format(args[2])
            TP = TwitterParser(args[2], args[2])
            if TP.get_all_json_file():
                TP.fixer_json()
                print "Done !"
            else:
                print "[Error] with the path: {} ".format(args[2])
            exit(0)
        if args[1]=='sort':
            print "args path: {}".format(args[2])
            sorted_dir = ht.mkdir_system(args[2],'sorted')
            TP = TwitterParser(args[2], sorted_dir)
            if TP.get_all_json_file():
                TP.sort_all()
                print "Done !"
            else:
                print "[Error] with the path: {} ".format(args[2])


import sys
if __name__ == "__main__":
    path_p = '/home/ise/NLP/json_twitter/tweetJson/'
    out_p = '/home/ise/Desktop/nlp_ex/out'
    sys.argv=['','sort','/home/ise/NLP/oren/big_data/DATA/tweetJson']
    command_parser(sys.argv)