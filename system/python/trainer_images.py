import dan
import sys, os, time

import data_manager_utils
from version_utils import Versioning

head_addr = f'{dan.get_local_ip()}:'

class Trainer():
    """ML Trainer"""

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

    # check available model version and return that version or None
    def is_available_model_version(self, version):
        versioning = self.versioning
        model_version_node = versioning.is_available_model_version(self.proj_id, version)
        if model_version_node is not None:
            print('Model version {} existed!'.format(version))
        return version

    # check available training version and return that version or None
    def is_available_training_version(self, version):
        versioning = self.versioning
        training_version_node = versioning.is_available_training_version(self.proj_id, version)
        if training_version_node is not None:
            print('Training version {} existed!'.format(version))
        return version

    # check available testing version and return that version or None
    def is_available_testing_version(self, version):
        versioning = self.versioning
        testing_version_node = versioning.is_available_testing_version(self.proj_id, version)
        if testing_version_node is not None:
            print('Testing version {} existed!'.format(version))
        return version

    def exist_model_version(self, model_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        versioning = self.versioning
        model_version = str(model_version).strip()
        self.model_version = model_version
        model_node = versioning.select_model_version(proj_id, model_version)
        return (model_node is not None)
    
    def exist_data_version(self, data_version, data_target):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        versioning = self.versioning
        data_version = str(data_version).strip()
        self.data_version = data_version
        data_node = versioning.select_data_version(proj_id, data_version, data_target)
        print(proj_id)
        return (data_node is not None)
    
    def exist_training_version(self, training_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        versioning = self.versioning
        training_version = str(training_version).strip()
        training_node = versioning.select_training_version(proj_id, training_version)
        return (training_node is not None)
    
    def model_insert(self, model_version, model_name, model_type, model_framework, model_input, model_classes, model_checkpoint, model_graph):
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        # check model version
        versioning = self.versioning
        model_version = str(model_version).strip()
        model_node = versioning.select_model_version(proj_id, model_version)
        if model_node:
            print('Model version {} existed!'.format(model_version))
            return None
        
        # insert model meta data
        _model_metadata = {
            'model_name': model_name,
            'model_type': model_type,
            'model_framework': model_framework,
            'model_input': model_input,
            'model_classes': model_classes,
            }
        model_id = data_manager_utils.insert_model_metadata(proj_id, model_version, _model_metadata)
        
        # create new model version node for training
        versioning = self.versioning
        versioning.insert_model_version_node(proj_id, model_id, model_version, model_name)
        versioning.add_model_info(proj_id, model_version, checkpoint=model_checkpoint, graph=model_graph)
        
        return model_id

    # create a model finetune
    def model_finetune(self, from_model_version, model_version, model_name, model_classes, model_graph, data_version=None):
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check model version
        versioning = self.versioning
        from_model_version = str(from_model_version).strip()
        model_version = str(model_version).strip()
        from_model_node = versioning.select_model_version(proj_id, from_model_version)
        if not from_model_node:
            print('Model version {} does not exist!'.format(from_model_version))
            return None
        model_node = versioning.select_model_version(proj_id, model_version)
        if model_node:
            print('Model version {} existed!'.format(model_version))
            return None
        
        # check data version
        if data_version:
            versioning = self.versioning
            data_version = str(data_version).strip()
            data_node = versioning.select_data_version(proj_id, data_version, 'train')
            if not data_node:
                print('Data version {} does not existed!'.format(data_version))
                return None
                
        # select from_model_version node
        from_model_id = from_model_node['metadata_id']
        from_model_metadata = data_manager_utils.select_model_metadata(from_model_id)
        
        # insert model meta data
        _model_metadata = {
            'model_name': model_name,
            'model_type': from_model_metadata.get('model_type', ''),
            'model_framework': from_model_metadata.get('model_framework', ''),
            'model_input': from_model_metadata.get('model_input', ''),
            'model_classes': model_classes,
            }
        model_id = data_manager_utils.insert_model_metadata(proj_id, model_version, _model_metadata)
        
        versioning.insert_model_version_finetune(proj_id, model_id, model_version, from_model_version, model_name, data_version)
        versioning.add_model_info(proj_id, model_version, graph=model_graph)
        
        return model_id

    def select_model_version(self, model_version):
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check model version
        versioning = self.versioning
        model_version = str(model_version).strip()
        model_node = versioning.select_model_version(proj_id, model_version)
        if not model_node:
            print('Model version {} does not exist!'.format(model_version))
            return None
        
        model_id = model_node['metadata_id']
        model_metadata = data_manager_utils.select_model_metadata(model_id)
        # add other model info
        model_metadata['model_checkpoint'] = model_node.get('checkpoint')
        model_metadata['model_graph'] = model_node.get('graph')
        
        return model_metadata

    def training_insert(self, training_version, data_version, model_version, training_metadata, pre_training_version=None):
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        # check training version
        versioning = self.versioning
        training_version = str(training_version).strip()
        training_node = versioning.select_training_version(proj_id, training_version)
        if training_node:
            print("Training version {} existed!".format(training_version))
            return None
                
        # insert training meta data
        _training_metadata = {
            'framework': training_metadata.get('framework', ''),
            'distributed_training': training_metadata.get('distributed_training', False),
            'number_of_gpus': training_metadata.get('number_of_gpus', 0),
            'number_of_nodes': training_metadata.get('number_of_nodes', 0),
            'batch_size': training_metadata.get('batch_size', 1),
            'epochs': training_metadata.get('epochs', 1),
            'learning_rate': training_metadata.get('lr', 0),
            }
        training_id = data_manager_utils.insert_training_metadata(proj_id, training_version, _training_metadata)
        
        # insert training version
        versioning = self.versioning
        data_version = str(data_version).strip()
        model_version = str(model_version).strip()
        versioning.insert_training_version(training_id, proj_id, training_version, data_version, model_version, _training_metadata, pre_training_version)
        self.training_version = training_version
        
        return training_id

    # insert training log to training version
    def training_log(self, training_history, training_version=None):
        proj_id = self.proj_id
        if not training_version:
            training_version = self.training_version
        if not training_version:
            print('Training version does not exist!')
            return None
            
        # insert training log data of this training id
        versioning = self.versioning
        training_node = versioning.select_training_version(proj_id, training_version)
        training_id = training_node['metadata_id']
        tname, fields, rowids = data_manager_utils.insert_training_log(training_id, training_history)
            
        # update training version for training history
        versioning = self.versioning
        # get previous saving training logs
        pre_rowids = []
        pre_training_node = versioning.select_training_version(proj_id, training_version)
        if pre_training_node:
            pre_rowids = pre_training_node.get("training_logs")
            if not pre_rowids:
                pre_rowids = []
        # update latest training logs
        rowids = pre_rowids + rowids
        versioning.add_training_logs(proj_id, training_version, tname, str(fields), rowids)
        
        return tname, fields, rowids

    def add_training_results(self, checkpoint, evaluation, training_version=None):
        proj_id = self.proj_id
        if not training_version:
            training_version = self.training_version
        if not training_version:
            print('Training version does not exist!')
            return None
            
        # update training version for training results
        versioning = self.versioning
        versioning.add_training_results(proj_id, training_version, checkpoint, evaluation)
        
        return training_version
        
    # get training checkpoint path
    def training_checkpoint(self, training_version):
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        versioning = self.versioning
        training_version = str(training_version).strip()
        training_node = versioning.select_training_version(proj_id, training_version)
        if not training_node:
            print("Training version {} does not exist!".format(training_version))
            return None
        checkpoint_path = training_node.get("checkpoint")
        return checkpoint_path

    # get training information
    def training_info(self, training_version):
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        versioning = self.versioning
        training_version = str(training_version).strip()
        training_node = versioning.select_training_version(proj_id, training_version)
        if not training_node:
            print("Training version {} does not exist!".format(training_version))
            return None
        
        # get training metadata
        metadata_feature_names = ['framework', 'distributed_training', 'number_of_gpus', 'number_of_nodes']
        version_feature_names = ['batch_size', 'epochs', 'learning_rate']
        training_metadata = {}
        training_id = training_node.get("metadata_id")
        if training_id:
            training_metadata = data_manager_utils.select_training_metadata(training_id)
            for feature_name in metadata_feature_names:
                training_metadata[feature_name] = training_metadata[feature_name]
            for feature_name in version_feature_names:
                training_metadata[feature_name] = training_node[feature_name]
            # select data version
            data_version = versioning.select_training_version_data_relationship(proj_id, training_version)['version']
            training_metadata['data_version'] = data_version
            # select model version
            model_version = versioning.select_training_version_model_relationship(proj_id, training_version)['version']
            training_metadata['model_version'] = model_version
        return training_metadata

    # add new testing version
    def addnew_testing_version(self, training_version, images_dir, prediction_dir, test_version, test_params, data_version=None, target='test'):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        
        # check training_version
        training_version = str(training_version)
        versioning = self.versioning
        training_node = versioning.select_training_version(proj_id, training_version)
        if not training_node:
            print("Training version {} does not exist!".format(training_version))
            return None
        
        # check test_version
        test_version = str(test_version)
        versioning = self.versioning
        test_node = versioning.select_test_version(proj_id, test_version)
        if test_node:
            print("Test version {} existed!".format(test_version))
            return None
        
        # insert new metadata for test version
        data_type = 'Images'
        if not data_version:
            from_data_version = versioning.select_training_version_data_relationship(proj_id, training_version)['version']
        else:
            from_data_version = data_version
        data_metadata = {
            'data_source': 'Data version {}'.format(from_data_version),
            'data_type': data_type,
            'data_version': test_version,
        }
        metadata_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert test images results
        data_tname, ttype, fields, row_ids, _ = data_manager_utils.insert_test_images(proj_id=proj_id, images_dir=images_dir, predictions_dir=prediction_dir)
        
        # insert testing version
        versioning.insert_test_version_node(metadata_id, proj_id, training_version, test_version, data_tname, row_ids, test_params, data_version)
        

if __name__ == "__main__":
    print('main')
    
