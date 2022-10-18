import dan
import sys, os, time

import data_manager_utils
from version_utils import Versioning

head_addr = f'{dan.get_local_ip()}:'

class Inferencer():
    """ML Life cycle Inference management"""

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
        self.versioning = Versioning()

    # check available inference version and return that version or None
    def is_available_inference_version(self, version):
        versioning = self.versioning
        inference_version_node = versioning.is_available_inference_version(self.proj_id, version)
        if inference_version_node is not None:
            print('Inference version {} existed!'.format(version))
        return version

    def exist_training_version(self, training_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        versioning = self.versioning
        training_version = str(training_version).strip()
        self.training_version = training_version
        training_node = versioning.select_training_version(proj_id, training_version)
        return (training_node is not None)
    
    def exist_inference_version(self, inference_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        versioning = self.versioning
        inference_version = str(inference_version).strip()
        self.inference_version = inference_version
        inference_node = versioning.select_inference_version(proj_id, inference_version)
        return (inference_node is not None)
    
    # get training checkpoint from training version
    def training_checkpoint(self, training_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        # check training version
        versioning = self.versioning
        training_version = str(training_version).strip()
        training_node = versioning.select_training_version(proj_id, training_version)
        if not training_node:
            print("Training version {} does not exist!".format(training_version))
            return None
        # get checkpoint path from training version
        checkpoint_path = training_node.get("checkpoint", "")
        return checkpoint_path
    
    def inference_insert(self, inference_version, training_version, inference_metadata):
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        # check inference version
        versioning = self.versioning
        inference_version = str(inference_version).strip()
        inference_node = versioning.select_inference_version(proj_id, inference_version)
        if inference_node:
            print('Inference version {} existed!'.format(inference_version))
            return None
            
        # insert inference meta data
        inference_id = data_manager_utils.insert_inference_metadata(proj_id, inference_version, inference_metadata)
        
        # insert inference version
        versioning = self.versioning
        training_version = str(training_version).strip()
        versioning.insert_inference_version(inference_id, proj_id, inference_version, training_version, inference_metadata)
        self.inference_version = inference_version
        
        return inference_id

    # get inference model from inference version
    def inference_model(self, inference_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        # check inference version
        versioning = self.versioning
        inference_version = str(inference_version).strip()
        inference_node = versioning.select_inference_version(proj_id, inference_version)
        if not inference_node:
            print('Inference version {} does not exist!'.format(inference_version))
            return None
        
        # get inference model path from inferene version
        inference_model = inference_node.get("inference_model", "")
        return inference_model

    # get inference information
    def inference_info(self, inference_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        # check inference version
        versioning = self.versioning
        inference_version = str(inference_version).strip()
        inference_node = versioning.select_inference_version(proj_id, inference_version)
        if not inference_node:
            print('Inference version {} does not exist!'.format(inference_version))
            return None
            
        inference_info = {}
        inference_id = inference_node.get("metadata_id")
        if inference_id:
            # select training version
            training_version = versioning.select_inference_version_training_relationship(proj_id, inference_version)['version']
            inference_info['training_version'] = training_version
        return inference_info

if __name__ == "__main__":
    print('main')
    
