import multiprocessing
from functools import partial
from contextlib import contextmanager
import os, json, sys
from multiprocessing import freeze_support
import handler_tweet as ht
import parser_2015 as parser

# # # # TWITTER  # # # #

class TweetNode():
    def __init__(self, id_tweet, data_tweet, replay):
        self.id = id_tweet
        self.data = data_tweet
        self.in_replay_to = replay
        if data_tweet is None:
            self.data = "{}"

@contextmanager
def poolcontext(*args, **kwargs):
    pool = multiprocessing.Pool(*args, **kwargs)
    yield pool
    pool.terminate()
    pool.join()


def merge_names(a, b):
    return '{} & {}'.format(a, b)


def workers_func(target_path):
    object_parser = parser.TwitterParser('','')
    object_parser.all_json=target_path
    dict_map = object_parser.parser_by()
    res = object_parser.make_list(dict_map)
    data_stream = json.load(open(target_path))
    print "done!"


def get_all_json_file(path_dir):
    """
    :param path_dir: the path where the json files are
    :return: a list of paths
    """
    res = ht.walk_rec(path_dir, [], ".json")
    if len(res) == 0:
        return None
    return res


def convert_to_integer(str_num):
    """
    convert a string to int (integer)
    :param str_num: string
    :return: int or None if Exception is occurred
    """
    try:
        res = int(str_num)
    except ValueError as verr:
        print " Input does not contain anything convertible to int : input = %s" % str_num
        print verr.message
        return None
    except Exception as ex:
        print " Exception occurred while converting to int: input = %s" % str_num
        print ex.message
        return None
    return res


def get_list_names(num_worker, path_p):
    '''
    allocation each worker files to work on
    :param num_worker: the number of worker (processes)
    :param path_p:  the path where all the json files in
    :return: a list of tasks
    '''
    if num_worker < 1:
        print '[Error] the number of worker must be greater then 1 '
        exit(-1)
    list_json = get_all_json_file(path_p)
    size = len(list_json)
    if size == 0:
        print '[Error] no jsons in the path: {}'.format(path_p)
        exit(-1)
    share = size / num_worker
    if share * num_worker == size:
        leftover = 0
    else:
        leftover = size - share * num_worker
    list_targets = []
    i = 0
    while i < size:
        arr = []
        for j in range(share):
            arr.append(list_json[i + j])
        if leftover > 0:
            arr.append(list_json[i + share + 1])
            leftover -= 1
            i += 1
        list_targets.append(arr)
        i = i + share
    print list_targets


def main_init(num_process=2):
    if len(sys.argv) > 1:
        num_process = convert_to_integer(sys.argv[1])
    path = '/home/ise/NLP/json_twitter'
    names = get_all_json_file(path)
    get_list_names(num_process, path)
    exit()
    with poolcontext(processes=2) as pool:
        results = pool.map(readers, names)
    print(results)


if __name__ == '__main__':
    # Add support for when a program which uses
    # multiprocessing has been frozen to produce a Windows executable.
    # Only for Windows OS
    if os.name == 'nt':
        freeze_support()
    main_init()
