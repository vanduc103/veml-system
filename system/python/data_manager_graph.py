import dan
import sys, os, time
import numpy as np
import data_manager_utils
from version_utils import Versioning


head_addr = f'{dan.get_local_ip()}:'

class DataManagerGraph():
    """Data Manager for Graph data type"""

    def __init__(self, proj_name, dan_port, head_addr=head_addr):
        # connect dan to cluster
        dan.connect(head_addr=head_addr+str(dan_port), launch=False)
        
        self.versioning = Versioning()
        # create new project (if not existed)
        self.create_project(proj_name)

    # check if exist data version        
    def check_data_version(self, proj_id, version, target):
        versioning = self.versioning
        data_version_node = versioning.select_data_version(proj_id, version, target)
        return (data_version_node is not None)

    # create new project            
    def create_project(self, proj_name, proj_owner='levanduc'):
        proj_id = data_manager_utils.select_proj_by_name(proj_name)
        if not proj_id:
            # create new table
            proj_id = data_manager_utils.insert_project(proj_name, proj_owner)
            
            # create new node of project
            versioning = self.versioning
            versioning.insert_project_node(proj_id, proj_name)
        self.proj_id = proj_id
        self.proj_name = proj_name
        return proj_id

    # check available data version and return that version
    def is_available_data_version(self, version):
        versioning = self.versioning
        data_version = str(version).strip()
        data_version_node = versioning.is_available_data_version(self.proj_id, version)
        if data_version_node is not None:
            print('Data version {} existed!'.format(version))
        return version

    # get list test version of the data version
    def list_test_version_of_data_version(self, version):
        versioning = self.versioning
        data_version = str(version).strip()
        test_versions = versioning.list_test_version_of_data_version(self.proj_id, data_version)
        return test_versions

    # add new data version with 1 data dir for nodes and edges information
    def addnew_data_version(self, data_path, data_version, data_target, data_description):
        if data_path:
            if not os.path.isdir(data_path):
                print('Directory {} must be existed!'.format(data_path))
                return None
        
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        
        # check version
        data_version = str(data_version)
        if self.check_data_version(proj_id, data_version, data_target):
            print("Data version {} and target {} existed!".format(data_version, data_target))
            return None
        
        # insert new metadata for data version
        data_source = data_path
        data_type = 'Graph'
        data_version = data_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': data_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert graph data
        data_gname, ttype, fields, row_ids, has_annotation = data_manager_utils.insert_data_graph(proj_id=proj_id, data_dir=data_path, gname=str(data_version), batch_size=10000)
        
        # insert data version
        versioning = self.versioning
        versioning.insert_data_version_node(proj_id, dt_id, data_version, data_target, data_description, data_gname, row_ids, has_annotation)


    # process data information
    def process_data(self, data_version, data_target):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        data_versions = list()
        if type(data_version) is not list:
            data_versions = [data_version]
        else:
            data_versions += data_version # can work with a list
        versioning = self.versioning
        for data_version in data_versions:
            data_node = versioning.select_data_version(proj_id, data_version, data_target)
            if not data_node:
                print("Data version {} and target {} do not exist!".format(data_version, data_target))
                return None

        data_points, annot_points = list(), list()
        # get data version information
        for data_version in data_versions:
            data_node = versioning.select_data_version(proj_id, data_version, data_target)
            
            data_points_version = data_node.get("data_points")
            # merge 2 list without duplicates
            if data_points_version:
                data_points = data_points + list(set(data_points_version) - set(data_points))
            data_gname = data_node.get("data_tname")
            
        # select data from storage
        nodes, srcs, dsts, annotations = data_manager_utils.get_data_graph(proj_id, data_gname, data_points)
        
        self.process_nodes(nodes)
        self.process_edges(srcs, dsts)
        self.process_annotations(annotations)
        self.process_categories(annotations)

    # process nodes information
    def process_nodes(self, nodes):
        self.nodes = nodes
        self.node_ids = np.array(nodes['node_id'])

    # process edge information
    def process_edges(self, srcs, dsts):
        self.edges = np.array([srcs, dsts])
        del srcs, dsts
        self.edges = np.transpose(self.edges)
     
    # process annotations information
    def process_annotations(self, annotations):
        self.annotations = annotations

    # get categories
    def process_categories(self, annotations):
        self.categories = {}
        for _, annotation in annotations.items():
            cat_name = annotation['label']
            # Add category to the categories dict
            if cat_name not in self.categories:
                category = {'name': cat_name}
                category['count'] = 0
                self.categories[cat_name] = category


    # process test prediction results
    def process_test_results(self, test_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist version
        test_version = str(test_version)
        versioning = self.versioning
        test_node = versioning.select_test_version(proj_id, test_version)
        if not test_node:
            print("Test version {} does not exist!".format(test_version))
            return None
            
        # get test node information
        data_points = test_node.get("data_points")
        data_gname = test_node.get("data_tname")
        # select test results from storage
        annotations = data_manager_utils.get_prediction_graph(proj_id, data_gname, data_points)
        self.test_predictions = annotations
        
        return self.test_predictions

    def compute_test_evaluation(self, test_version, data_version, data_labels=None, data_target='all'):
        # get test results
        self.process_test_results(test_version)
        predictions = self.test_predictions
        
        labels, preds = [], []
        for _, annot in predictions.items():
            labels.append(annot['label'])
            preds.append(annot['pred'])
            
        # compute accuracy by filter labels
        from sklearn.metrics import precision_score, accuracy_score
        labels, preds = np.array(labels), np.array(preds)
        accuracies = {}
        if data_labels:
            for filter_label in data_labels:
                mask = np.isin(labels, [filter_label])
                masked_labels = labels[mask]
                masked_preds = preds[mask]
                
                accuracy = accuracy_score(masked_labels, masked_preds)
                accuracies[filter_label] = accuracy*100
        else:
            accuracy = accuracy_score(labels, preds)
            accuracies['all'] = accuracy*100
        del labels, preds
        fps = 0
        return accuracies, fps

    # create new data by filtering from existed data version
    def addfilter_data_version(self, data_version, data_target, newdata_version, new_target, filter_conditions, data_description, annotation=True):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        
        # check data version (can be a list)
        data_versions = list()
        if type(data_version) is not list:
            data_versions = [data_version]
        else:
            data_versions += data_version # can be a list
        versioning = self.versioning
        for data_version in data_versions:
            data_node = versioning.select_data_version(proj_id, data_version, data_target)
            if not data_node:
                print("Data version {} and target {} do not exist!".format(data_version, data_target))
                return None
        
        # check new data version
        newdata_version = str(newdata_version)
        versioning = self.versioning
        newdata_node = versioning.select_data_version(proj_id, newdata_version, new_target)
        if newdata_node:
            print("Data version {} and target {} existed!".format(newdata_version, new_target))
            return None
            
        # process data
        self.process_data(data_versions, data_target)

        # filter condition
        filter_rowids = None
        if "rowids" in filter_conditions:
            filter_rowids = filter_conditions["rowids"]
            filter_rowids = filter_rowids.split(',')
            filter_rowids = [int(s.strip()) for s in filter_rowids]
        filter_labels = None
        if "labels" in filter_conditions:
            if not annotation:
                print("Data version {} does not have any labels!".format(data_version))
                return None
            filter_labels = filter_conditions["labels"]
            filter_labels = filter_labels.split(',')
            filter_labels = [s.strip() for s in filter_labels]
        if not (filter_rowids or filter_labels):
            print('Must choose filtering by rowids or labels!')
            return None
        
        filtered_images = []
        filtered_annots = []
        for _, image_id in enumerate(self.images):
            image = self.images[image_id]
            imageName = image['name']
            if filter_rowids and image_id not in filter_rowids:
                continue
            if not filter_labels:
                if annotation:
                    if self.annotations.get(image_id):
                        filtered_images.append(image)
                        filtered_annots.append(self.annotations.get(image_id))
                else:
                    filtered_images.append(image)
                    if self.annotations.get(image_id):
                        filtered_annots.append(self.annotations.get(image_id))
                    else:
                        filtered_annots.append([])
            else:
                if self.annotations.get(image_id):
                    annots = []
                    for _, annot in enumerate(self.annotations[image_id]):
                        label = annot['label']
                        if label in filter_labels:
                            annots.append(annot)
                    if annots:
                        filtered_images.append(image)
                        filtered_annots.append(annots)
        
        # add filtered results to a new data version
        data_source = "Filter from data version {}".format(data_versions)
        data_type = 'Images'
        newdata_version = newdata_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': newdata_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # get rowids and annot_ids from filtered results
        row_ids, annot_ids = [], []
        for _, image in enumerate(filtered_images):
            row_ids.append(image['id'])
        for _, annots in enumerate(filtered_annots):
            for _, annot in enumerate(annots):
                annot_ids.append(annot['annot_id'])
        
        # insert data version
        from_versions = data_versions
        from_target = data_target
        new_version = newdata_version
        new_target = new_target
        versioning = self.versioning
        versioning.addfilter_data_version(proj_id, dt_id, from_versions, from_target, new_version, new_target, data_description, row_ids, annot_ids, str(filter_conditions))

        print('View versioning information:')
        versioning.select_data_version(proj_id, new_version, new_target)


def main():
    pass
    
if __name__ == "__main__":
    main()
        
