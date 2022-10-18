import dan
import sys, os, time
from datetime import datetime
import psutil

import data_manager_utils
from version_utils import Versioning

head_addr = f'{dan.get_local_ip()}:'

class Profiling():
    """ Profiling Object """
    
    def __init__(self, task_name, time_start="", duration="", data_stat="", mem_stat="", io_stat=""):
        self.task_name = task_name
        self.time_start = time_start
        self.duration = duration
        self.data_stat = data_stat
        self.mem_stat = mem_stat
        self.io_stat = io_stat
        self.cpu_stat = ""
        self.pid = psutil.Process().pid
    
    def start(self):
        self.time_start = datetime.now()
        self.mem_start = psutil.Process(self.pid).memory_info().rss
        self.io_start = psutil.Process(self.pid).io_counters().read_bytes
        return self
        
    def complete(self, data_stat=None, mem_stat=None, io_stat=None, cpu_stat=None):
        self.duration = str(datetime.now() - self.time_start)
        self.time_start = str(self.time_start)
        if data_stat:
            self.data_stat = str(data_stat)
        if mem_stat:
            self.mem_stat = str(mem_stat)
        else:
            mem_stop = psutil.Process(self.pid).memory_info().rss
            self.mem_stat = str(mem_stop - self.mem_start)
        if io_stat:
            self.io_stat = str(io_stat)
        else:
            io_stop = psutil.Process(self.pid).io_counters().read_bytes
            self.io_stat = str(io_stop - self.io_start)
        if cpu_stat:
            self.cpu_stat = str(cpu_stat)
        else:
            self.cpu_stat = str(psutil.Process(self.pid).cpu_percent())
        return self
    
    def profile(self):
        profiling = {'task_name': self.task_name, 
                    'time_start': self.time_start, 
                    'duration': self.duration, 
                    'data_stat': self.data_stat, 
                    'mem_stat': self.mem_stat,
                    'io_stat': self.io_stat,
                    'cpu_stat': self.cpu_stat}
        return profiling
    

class MLProfiler():
    """ML Profiler Tool"""

    def __init__(self, dan_port, head_addr=head_addr):
        # connect dan to cluster
        dan.connect(head_addr=head_addr+str(dan_port), launch=False)
        self.proj_id = None
        self.lc_id = None

    def init_profiling(self, proj_name, data_version=None, model_version=None, training_version=None, inference_version=None, predict_version=None):
        proj_id = data_manager_utils.select_proj_by_name(proj_name)
        if not proj_id:
            print('Project {} does not exist!'.format(proj_name))
            return None
        self.proj_id = proj_id
            
        # make a pipeline
        lc_pipeline = ''
        if data_version:
            lc_pipeline = 'data_version:{}'.format(data_version)
        elif model_version:
            lc_pipeline = 'model_version:{}'.format(model_version)
        elif training_version:
            lc_pipeline = 'training_version:{}'.format(training_version)
        elif inference_version:
            lc_pipeline = 'inference_version:{}'.format(inference_version)
        elif predict_version:
            lc_pipeline = 'predict_version:{}'.format(predict_version)
        
        # check if pipeline existed
        lc_id = data_manager_utils.select_profiling(proj_id, lc_pipeline)
        if not lc_id:
            # insert profiling
            lc_id = data_manager_utils.insert_profiling(self.proj_id, lc_pipeline)
        self.lc_id = lc_id
        return lc_id
    
    def profile_task(self, profiling):
        if self.lc_id:
            # insert profiling task
            task_id = data_manager_utils.insert_profiling_task(self.lc_id, profiling.profile())
        return task_id

    def view_profiling(self, proj_name, data_version=None, model_version=None, training_version=None, inference_version=None, predict_version=None):
        proj_id = data_manager_utils.select_proj_by_name(proj_name)
        if not proj_id:
            print('Project {} does not exist!'.format(proj_name))
            return None
            
        # check versioning existed
        versioning = Versioning()
        if data_version:
            data_version = str(data_version).strip()
            data_target = 'train'
            data_node = versioning.select_data_version(proj_id, data_version, data_target)
            if not data_node:
                print('Data version {} does not exist!'.format(data_version))
                return None
        elif model_version:
            model_version = str(model_version).strip()
            model_node = versioning.select_model_version(proj_id, model_version)
            if not model_node:
                print('Model version {} does not exist!'.format(model_version))
                return None
        elif training_version:
            training_version = str(training_version).strip()
            version_node = versioning.select_training_version(proj_id, training_version)
            if not version_node:
                print('Training version {} does not exist!'.format(training_version))
                return None
        elif inference_version:
            inference_version = str(inference_version).strip()
            version_node = versioning.select_inference_version(proj_id, inference_version)
            if not version_node:
                print('Inference version {} does not exist!'.format(inference_version))
                return None
        elif predict_version:
            predict_version = str(predict_version).strip()
            version_node = versioning.select_predict_version(proj_id, predict_version)
            if not version_node:
                print('Predict version {} does not exist!'.format(predict_version))
                return None
            
        # make a pipeline
        lc_pipeline = ''
        if data_version:
            lc_pipeline = 'data_version:{}'.format(data_version)
        elif model_version:
            lc_pipeline = 'model_version:{}'.format(model_version)
        elif training_version:
            lc_pipeline = 'training_version:{}'.format(training_version)
        elif inference_version:
            lc_pipeline = 'inference_version:{}'.format(inference_version)
        elif predict_version:
            lc_pipeline = 'predict_version:{}'.format(predict_version)
        
        # get pipeline id from profiling table
        profiling = []
        lc_id = data_manager_utils.select_profiling(proj_id, lc_pipeline)
        if lc_id:
            profiling = data_manager_utils.select_profiling_task(lc_id)
        return lc_pipeline, profiling, lc_id

        
if __name__ == "__main__":
    print('Main')
    
