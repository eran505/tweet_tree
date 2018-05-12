import os, json, re, pprint
import handler_tweet as ht


# # # # TWITTER  # # # #

class TweetNode():
    def __init__(self, id_tweet, data_tweet, replay):
        self.id = id_tweet
        self.data = data_tweet
        self.in_replay_to = replay


# # # class build the trees out of the raw data # # #
class TreeConstructor():
    def __init__(self, dir_json, out_path, worker=3):
        self.data_dir = dir_json
        self.out_dir = out_path
        self.num_worker = worker



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
            print line_i
            if line_i.startswith('{"id":'):
                line_i = line_i.replace(line_i, ",{}".format(line_i))
                text_new.append(line_i)
        text_new[-1] = str(text_new[-1]) + "] }"
        print text_new[-1]
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

    def parser_by(self, tweet_id=None):
        map_dict = {}
        for file_i in self.all_json:
            data_stream = json.load(open(file_i))
            for entry in data_stream['arr_tweets']:
                id_i = str(entry['id']).split(':')[-1]
                if id_i in map_dict:
                    print "[Error]"
                else:
                    map_dict[id_i] = None
                if 'inReplyTo' in entry:
                    replay_id = str(entry['inReplyTo']).split('/')[-1][:-2]
                    map_dict[id_i] = replay_id
        return map_dict


if __name__ == "__main__":
    path_p = '/home/ise/NLP/json_twitter'
    out_p = '/home/ise/Desktop/nlp_ex'
    TP = TwitterParser(path_p, out_p)
    if TP.get_all_json_file():
        map = TP.parser_by()
        exit()
        for x in TP.all_json:
            TP.fix_json(x)
