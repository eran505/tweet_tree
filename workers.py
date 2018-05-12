from multiprocessing import Pool
from time import sleep
import logging,os


def init_log(dir_path_log=None,name_file=None): #filemode = w , to overwrite , filemode = a , to append,
    file_name = "run_out.log"
    if dir_path_log is None:
        logging.basicConfig(filename=file_name , level=logging.DEBUG,filemode='a')
    else:
        if os.path.isdir(dir_path_log) is False:
            raise Exception("not vaild path {}".format(dir_path_log))
        logging.basicConfig(filename=file_name , level=logging.DEBUG, filemode='a')

data = (
    ['a', '2'], ['b', '4'], ['c', '6'], ['d', '8'],
    ['e', '1'], ['f', '3'], ['g', '5'], ['h', '7']
)

def mp_worker((inputs, the_time)):
    print " Processs %s\tWaiting %s seconds" % (inputs, the_time)
    sleep(int(the_time))
    print " Process %s\tDONE" % inputs

def mp_handler():
    p = Pool(2)
    p.map(mp_worker, data)

if __name__ == '__main__':
    init_log()
    mp_handler()