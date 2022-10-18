import dan
import sys, os, time
import pandas as pd
import numpy as np
import cv2
from datetime import datetime
import random

import data_manager_utils
from version_utils import Versioning
from data_manager_graph import DataManagerGraph
from kg_data_knowledge import KGDataKnowledge

head_addr = f'{dan.get_local_ip()}:'

class AnalyzerGraph():
    """Analyzer API for Graph data type"""

    def __init__(self, proj_name, dan_port, head_addr=head_addr):
        # connect dan to cluster
        dan.connect(head_addr=head_addr+str(dan_port), launch=False)
        # get proj_id
        proj_id = data_manager_utils.select_proj_by_name(proj_name)
        if not proj_id:
            print('Project {} does not exist!'.format(proj_name))
            self.proj_id = None
        self.proj_name = proj_name
        self.proj_id = proj_id
        self.data_manager = DataManagerGraph(proj_name, dan_port, head_addr)
        self.versioning = Versioning()

    def list_data_version(self):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # list all data version
        versioning = Versioning()
        data_nodes = versioning.select_all_dataversion(proj_id)
        data_versions = []
        for data in data_nodes:
            data_versions.append(data)
        return data_versions
        
    def metadata_data_version(self, data_version, data_target):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        data_version = str(data_version)
        versioning = Versioning()
        data_node = versioning.select_data_version(proj_id, data_version, data_target)
        if not data_node:
            print("Data version {} and target {} do not exist!".format(data_version, data_target))
            return None
            
        # get metadata and version features
        metadata_feature_names = ['data_type', 'data_timestamp', 'data_version', 'data_source']
        metadata_features = []
        version_feature_names = ['version', 'target', 'total_datapoints', 'data_points']
        version_features = []
        
        metadata_id = data_node["metadata_id"]
        metadata_values = data_manager_utils.select_data_metadata(metadata_id)
        for feature_name in metadata_feature_names:
            metadata_features.append(metadata_values[feature_name])
            
        for feature_name in version_feature_names:
            version_features.append(data_node[feature_name])
        
        # result results as dataframe
        results = []
        results.append(metadata_feature_names + version_feature_names)
        results.append(metadata_features + version_features)
        df = pd.DataFrame(data=results[1:], columns=results[0])
        return df

    def analyze_data_version(self, data_version, data_target, test_versions=None):
        # process data
        self.data_manager.process_data(data_version, data_target)
        
        # data analysis
        stats = {}
        stats['number_of_nodes'] = self.data_manager.node_ids.size
        stats['number_of_edges'] = self.data_manager.edges.shape[0]
        nodes = self.data_manager.nodes
        labels = np.unique(np.array(nodes['label']))
        years = np.unique(np.array(nodes['year']))
        stats['number_of_classes'] = len(labels)
        stats['label'] = labels
        stats['year'] = years
        
        return stats        
    
    def visualize_data_version(self, data_version, data_target, node_ids=None, test_versions=None, filter_labels=None, example_count=1):
        # process data
        self.data_manager.process_data(data_version, data_target)
            
        # load all data (TODO: performance problem)
        nodes = self.data_manager.node_ids
        edges = self.data_manager.edges
        srcs, dsts = edges[:,0], edges[:,1]
        
        # list test version of this data version (if existed)
        if not test_versions:
            list_test_versions = self.data_manager.list_test_version_of_data_version(data_version)
            if list_test_versions:
                print('List of test versions:')
                print(list_test_versions)
        test_predictions = []
        test_nodes = []
        if test_versions:
            for test_version in test_versions:
                # process test version results
                test_prediction = self.data_manager.process_test_results(test_version)
                test_predictions.append(test_prediction)
                test_nodes.append(self.versioning.select_test_version(self.proj_id, test_version))
        
        # get node id by label
        if filter_labels:
            node_ids = []
            while len(node_ids) < example_count:
                node_id = np.random.choice(nodes)
                label = self.data_manager.annotations[node_id]['label']
                if label in filter_labels:
                    node_ids.append(node_id)
                    
        if not node_ids:
            node_ids = np.random.choice(nodes, example_count).tolist()
        
        # get nodes relationship
        ret_nodes, ret_labels, ret_edges, ret_preds = [], [], [], []
        for i in range(len(node_ids)):
            node_id = node_ids[i]
            label = self.data_manager.annotations[node_id]['label']
            if test_predictions:
                pred = test_predictions[0][node_id]['pred']
                ret_preds.append(pred)
            print('Node: {}({}) '.format(node_id, label))
            ret_nodes.append(node_id)
            ret_labels.append(label)
            # get source relationships
            source = np.where(srcs == node_id)[0]
            for s in source:
                label = self.data_manager.annotations[dsts[s]]['label']
                if test_predictions:
                    pred = test_predictions[0][dsts[s]]['pred']
                    ret_preds.append(pred)
                print('{}->{}({})'.format(node_id, dsts[s], label))
                ret_nodes.append(dsts[s])
                ret_labels.append(label)
                ret_edges.append([node_id, dsts[s]])
            # get dest relationships
            target = np.where(dsts == node_id)[0]
            for t in target:
                label = self.data_manager.annotations[srcs[t]]['label']
                if test_predictions:
                    pred = test_predictions[0][srcs[t]]['pred']
                    ret_preds.append(pred)
                print('{}({})->{}'.format(srcs[t], label, node_id))
                ret_nodes.append(srcs[t])
                ret_labels.append(label)
                ret_edges.append([srcs[t], node_id])
        return ret_nodes, ret_edges, ret_labels, ret_preds

    # compare between data version
    def compare_data_version(self, base_version, compare_versions, data_target):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        base_version = str(base_version)
        versioning = Versioning()
        base_data_node = versioning.select_data_version(proj_id, base_version, data_target)
        if not base_data_node:
            print("Data version {} and target {} do not exist!".format(base_version, data_target))
            return None
            
        # check exist compare versions
        compares_data_nodes = []
        for compare_version in compare_versions.split(","):
            compare_version = str(compare_version).strip()
            compare_data_node = versioning.select_data_version(proj_id, compare_version, data_target)
            if not compare_data_node:
                print("Data version {} and target {} do not exist!".format(compare_version, data_target))
                return None
            else:
                compares_data_nodes.append(compare_data_node)
        
        # list of comparison data versions
        compares_data_nodes.insert(0, base_data_node)
        compare_feature_names = ["version", "data_timestamp", "data_source", "total_datapoints"]
        
        # get metadata information
        metadata_df = []
        for compare_data_node in compares_data_nodes:
            df = self.metadata_data_version(compare_data_node["version"], data_target)
            df = df[compare_feature_names]
            stats = self.analyze_data_version(compare_data_node["version"], data_target)
            df['number_of_nodes'] = stats['number_of_nodes']
            df['number_of_edges'] = stats['number_of_edges']
            metadata_df.append(df)
        
        results = pd.concat(metadata_df)
        return results
        
