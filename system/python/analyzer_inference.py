import dan
import sys, os, time, ast
import pandas as pd
from datetime import datetime
import re

import data_manager_utils
from version_utils import Versioning
from kg_inference_knowledge import KGInferenceKnowledge

local_head_addr = f'{dan.get_local_ip()}:'

class AnalyzerInference():
    """Analyzer API for Inference analysis"""

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


    #### Inference Analyzer APIs #####
    
    def metadata_inference_version(self, inference_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        inference_version = str(inference_version)
        versioning = Versioning()
        inference_node = versioning.select_inference_version(proj_id, inference_version)
        if not inference_node:
            print("Inference version {} does not exist!".format(inference_version))
            return None
            
        # get metadata and relationship features
        metadata_feature_names = ['timestamp', 'inference_version', 'device', 'optimization', 'precision']
        metadata_features = []
        version_feature_names = ['training_version', 'predict_version']
        version_features = ['', '']
        
        metadata_id = inference_node["metadata_id"]
        metadata_values = data_manager_utils.select_inference_metadata(metadata_id)
        for feature_name in metadata_feature_names:
            metadata_features.append(metadata_values[feature_name])
        
        training_node = versioning.select_inference_version_training_relationship(proj_id, inference_version)
        if training_node:
            # get training version
            training_version = training_node["version"]
            version_features[0] = "Training version {}".format(training_version)
        
        predict_nodes = versioning.select_inference_version_predict_relationship(proj_id, inference_version)
        if predict_nodes and len(predict_nodes) > 0:
            # get list of predict version
            predict_versions = [node["version"] for node in predict_nodes]
            version_features[1] = "Predict version {}".format(','.join(predict_versions))
        
        # Add inference knowledge
        kg_inference = KGInferenceKnowledge()
        # inference device
        device = metadata_values['device']
        if re.search('coral', device, re.IGNORECASE):
            inference_coral = kg_inference.add_knowledge_node("Inference_Device", "Google Coral")
            kg_inference.add_rel_inference_version_knowledge(inference_node, "Inference_Device", inference_coral)
        elif re.search('jetson', device, re.IGNORECASE):
            inference_jetson = kg_inference.add_knowledge_node("Inference_Device", "Nvidia Jetson")
            kg_inference.add_rel_inference_version_knowledge(inference_node, "Inference_Device", inference_jetson)
        elif re.search('cloud', device, re.IGNORECASE):
            inference_cloud = kg_inference.add_knowledge_node("Inference_Device", "Cloud Server")
            kg_inference.add_rel_inference_version_knowledge(inference_node, "Inference_Device", inference_cloud)
        elif re.search('gpu', device, re.IGNORECASE):
            inference_gpu = kg_inference.add_knowledge_node("Inference_Device", "GPU Server")
            kg_inference.add_rel_inference_version_knowledge(inference_node, "Inference_Device", inference_gpu)
        elif re.search('desktop', device, re.IGNORECASE) or re.search('pc', device, re.IGNORECASE):
            inference_pc = kg_inference.add_knowledge_node("Inference_Device", "Desktop PC")
            kg_inference.add_rel_inference_version_knowledge(inference_node, "Inference_Device", inference_pc)
        
        # inference optimization
        optimization_method = metadata_values['optimization']
        if re.search('no quantization', optimization_method, re.IGNORECASE) \
        or re.search('no optimization', optimization_method, re.IGNORECASE) \
        or re.search('none', optimization_method, re.IGNORECASE):
            inference_optimization_none = kg_inference.add_knowledge_node("Optimization_Method", "None")
            kg_inference.add_rel_inference_version_knowledge(inference_node, "Optimization_Method", inference_optimization_none)
        else:
            inference_optimization = kg_inference.add_knowledge_node("Optimization_Method", optimization_method)
            kg_inference.add_rel_inference_version_knowledge(inference_node, "Optimization_Method", inference_optimization)
        
        # inference precision
        precision = metadata_values['precision']
        inference_precision = kg_inference.add_knowledge_node("Inference_Precision", precision)
        kg_inference.add_rel_inference_version_knowledge(inference_node, "Inference_Precision", inference_precision)
        
        # result results as dataframe
        results = []
        results.append(metadata_feature_names + version_feature_names)
        results.append(metadata_features + version_features)
        df = pd.DataFrame(data=results[1:], columns=results[0])
        return df

    
    # compare between inference version
    def compare_inference_version(self, base_version, compare_versions):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        base_version = str(base_version)
        versioning = Versioning()
        base_inference_node = versioning.select_inference_version(proj_id, base_version)
        if not base_inference_node:
            print("Inference version {} does not exist!".format(base_version))
            return None
            
        # check exist compare versions
        compare_inference_nodes = []
        for compare_version in compare_versions.split(","):
            compare_version = str(compare_version).strip()
            compare_inference_node = versioning.select_inference_version(proj_id, compare_version)
            if not compare_inference_node:
                print("Inference version {} does not exist!".format(compare_version))
                return None
            else:
                compare_inference_nodes.append(compare_inference_node)
        
        # list of comparison versions
        compare_inference_nodes.insert(0, base_inference_node)
        compare_feature_names = ["inference_version", "timestamp", 'device', 'optimization', 'precision', "training_version", "predict_version"]
        
        # get metadata information
        metadata_df = []
        for compare_inference_node in compare_inference_nodes:
            df = self.metadata_inference_version(compare_inference_node["version"])
            df = df[compare_feature_names]
            metadata_df.append(df)
        
        results = pd.concat(metadata_df)
        return results


           
