import dan
import sys, os, time, ast
import pandas as pd
from datetime import datetime
import re

import data_manager_utils
from version_utils import Versioning
from data_manager_images import DataManagerImages
from data_manager_graph import DataManagerGraph
from kg_training_knowledge import KGTrainingKnowledge


local_head_addr = f'{dan.get_local_ip()}:'

class AnalyzerTraining():
    """Analyzer API for Training analysis"""

    def __init__(self, proj_name, dan_port, head_addr=local_head_addr):
        # connect dan to cluster
        dan.connect(head_addr=head_addr+str(dan_port), launch=False)
        # get proj_id
        proj_id = data_manager_utils.select_proj_by_name(proj_name)
        if not proj_id:
            print('Project {} does not exist!'.format(proj_name))
            self.proj_id = None
        self.proj_name = proj_name
        self.proj_id = proj_id
        self.data_manager_images = DataManagerImages(proj_name, dan_port, head_addr)
        self.data_manager_graph = DataManagerGraph(proj_name, dan_port, head_addr)

    #### Training Analyzer APIs #####
    
    def metadata_training_version(self, training_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        training_version = str(training_version)
        versioning = Versioning()
        training_node = versioning.select_training_version(proj_id, training_version)
        if not training_node:
            print("Training version {} does not exist!".format(training_version))
            return None
            
        # get metadata and relationship features
        metadata_feature_names = ['timestamp', 'training_version', 'framework', 'distributed_training', 'number_of_gpus', 'number_of_nodes']
        metadata_features = []
        version_feature_names = ['data_version', 'model_version', 'batch_size', 'learning_rate', 'num_of_epochs']
        version_features = ['', '']
        
        metadata_id = training_node["metadata_id"]
        metadata_values = data_manager_utils.select_training_metadata(metadata_id)
        for feature_name in metadata_feature_names:
            metadata_features.append(metadata_values[feature_name])
        
        data_node = versioning.select_training_version_data_relationship(proj_id, training_version)
        if data_node:
            # get data target and data version
            data_version = data_node["version"]
            version_features[0] = data_version
        
        model_node = versioning.select_training_version_model_relationship(proj_id, training_version)
        if model_node:
            # get model version
            model_version = model_node["version"]
            version_features[1] = model_version
            
        version_features.append(training_node.get('batch_size', 'None'))
        version_features.append(training_node.get('learning_rate', 'None'))
        version_features.append(training_node.get('epochs', 'None'))
        
        # Add training knowledge
        kg_training = KGTrainingKnowledge()
        # training framework
        framework = metadata_values['framework']
        if re.search('tensorflow 2', framework, re.IGNORECASE):
            training_tf2 = kg_training.add_knowledge_node("Training_Framework", "Tensorflow2")
            kg_training.add_rel_training_version_knowledge(training_node, "Training_Framework", training_tf2)
        elif re.search('tensorflow 1', framework, re.IGNORECASE):
            training_tf1 = kg_training.add_knowledge_node("Training_Framework", "Tensorflow1")
            kg_training.add_rel_training_version_knowledge(training_node, "Training_Framework", training_tf1)
        elif re.search('pytorch', framework, re.IGNORECASE):
            training_pt = kg_training.add_knowledge_node("Training_Framework", "Pytorch")
            kg_training.add_rel_training_version_knowledge(training_node, "Training_Framework", training_pt)
        
        # distributed training
        distributed_training_system_name = metadata_values['distributed_training']
        training_distributed_system = kg_training.add_knowledge_node("Training_Distributed_System", distributed_training_system_name)
        kg_training.add_rel_training_version_knowledge(training_node, "Training_Distributed_System", training_distributed_system)
        
        # use CPU or GPU
        number_of_gpus = metadata_values['number_of_gpus']
        if number_of_gpus == 0:
            training_accelerator_CPU = kg_training.add_knowledge_node("Training_Accelelator", "CPU")
            kg_training.add_rel_training_version_knowledge(training_node, "Training_Accelelator", training_accelerator_CPU)
        elif number_of_gpus > 0:
            training_accelerator_GPU = kg_training.add_knowledge_node("Training_Accelelator", "GPU")
            kg_training.add_rel_training_version_knowledge(training_node, "Training_Accelelator", training_accelerator_GPU)
        
        # result results as dataframe
        results = []
        results.append(metadata_feature_names + version_feature_names)
        results.append(metadata_features + version_features)
        df = pd.DataFrame(data=results[1:], columns=results[0])
        return df

    
    def view_training_log(self, training_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist training version
        training_version = str(training_version)
        versioning = Versioning()
        training_node = versioning.select_training_version(proj_id, training_version)
        if not training_node:
            print("Training version {} does not exist!".format(training_version))
            return None
            
        # return training history
        history = []
        log_points = training_node.get("training_logs")
        if not log_points:
            print('Training history does not existed!')
        else:
            fields = ast.literal_eval(training_node["fields"])
            tname = training_node["tname"]
        
            # select data from storage
            log_data = data_manager_utils.data_return_by_rows(tname, log_points)
            
            # log analysis
            field_names = list(fields.keys())
            for _, items in enumerate(log_data):
                log_dict = {}
                for i in range(len(items)):
                    log_dict[field_names[i]] = items[i]
                history.append(log_dict)
        
        return history
        
    
    # compare between training version
    def compare_training_version(self, base_version, compare_versions):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        base_version = str(base_version)
        versioning = Versioning()
        base_training_node = versioning.select_training_version(proj_id, base_version)
        if not base_training_node:
            print("Training version {} does not exist!".format(base_version))
            return None
            
        # check exist compare versions
        compare_training_nodes = []
        for compare_version in compare_versions.split(","):
            compare_version = str(compare_version).strip()
            compare_training_node = versioning.select_training_version(proj_id, compare_version)
            if not compare_training_node:
                print("Training version {} does not exist!".format(compare_version))
                return None
            else:
                compare_training_nodes.append(compare_training_node)
        
        # list of comparison versions
        compare_training_nodes.insert(0, base_training_node)
        compare_feature_names = ["timestamp", "training_version", "data_version", "model_version", 
                                "framework", "distributed_training", "number_of_gpus", "number_of_nodes", 'batch_size', 'learning_rate', 'num_of_epochs']
        
        # get metadata information
        metadata_df = []
        for compare_training_node in compare_training_nodes:
            df = self.metadata_training_version(compare_training_node["version"])
            df = df[compare_feature_names]
            metadata_df.append(df)
        
        results = pd.concat(metadata_df)
        return results

    # compute training evaluation on test version
    def test_version_evaluation(self, training_version, test_version, data_labels=None, data_version=None, data_target='train'):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist train version
        training_version = str(training_version)
        versioning = Versioning()
        training_node = versioning.select_training_version(proj_id, training_version)
        if not training_node:
            print("Training version {} does not exist!".format(training_version))
            return None
            
        # check exist test version
        test_version = str(test_version)
        testing_node = versioning.select_test_version(proj_id, test_version)
        if not testing_node:
            print("Test version {} does not exist!".format(test_version))
            return None
            
        # get data version from training version
        if not data_version:
            data_node = versioning.select_training_version_data_relationship(proj_id, training_version)
            if data_node:
                # get data target and data version
                data_version = data_node["version"]
            
        # get model node from training version
        model_node = versioning.select_training_version_model_relationship(proj_id, training_version)
        model_type = 'detection'
        if model_node:
            model_meta_id = model_node['metadata_id']
            df = data_manager_utils.select_model_metadata(model_meta_id)
            model_type = df['model_type']
        print(model_type)
        
        if model_type == 'classification':
            test_acc, avg_fps = self.data_manager_images.compute_test_evaluation_classification(test_version, data_version, data_labels, data_target)
        elif model_type == 'detection':
            test_acc, avg_fps = self.data_manager_images.compute_test_evaluation(test_version, data_version, data_labels, data_target)
        elif model_type == 'gnn':
            test_acc, avg_fps =  self.data_manager_graph.compute_test_evaluation(test_version, data_version, data_labels, data_target)
        return test_acc, avg_fps

