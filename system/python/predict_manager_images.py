import dan
import sys, os, time
import data_manager_utils
from version_utils import Versioning


head_addr = f'{dan.get_local_ip()}:'

class PredictManagerImages():
    """Predict Manager for Images data type"""

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

    # add new prediction version
    def addnew_predict_version(self, pred_device_name, inference_version, predict_path, pred_version, pred_params, data_version, target='predict'):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        
        # check inference_version
        inference_version = str(inference_version)
        versioning = self.versioning
        infer_node = versioning.select_inference_version(proj_id, inference_version)
        if not infer_node:
            print("Inference version {} does not exist!".format(inference_version))
            return None
        
        # check pred_version
        pred_version = str(pred_version)
        versioning = self.versioning
        pred_node = versioning.select_predict_version(proj_id, pred_version, target)
        if pred_node:
            print("Predict version {} existed!".format(pred_version))
            return None
            
        # insert new metadata for predict version
        data_metadata = {
            'data_source': pred_device_name,
            'data_type': 'Images',
            'data_version': pred_version,
        }
        metadata_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert images data
        data_tname, ttype, fields, row_ids, has_predict = data_manager_utils.insert_predict_images(proj_id=proj_id, data_dir=predict_path)
        
        # insert predict version
        version = pred_version
        target = target
        versioning.insert_predict_version_node(metadata_id, proj_id, inference_version, version, target, data_tname, row_ids, has_predict, pred_params, data_version)
        

    # update an existing version with new prediction
    def update_predict_version(self, pred_path, pred_version, target='predict'):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        
        # check version
        pred_version = str(pred_version)
        versioning = self.versioning
        pred_node = versioning.select_predict_version(proj_id, pred_version, target)
        if not pred_node:
            print("Updating a predict version but predict version {} does not exist!".format(pred_version))
            return None
                
        # get data version metadata
        metadata_id = pred_node["metadata_id"]
        
        # insert images data
        data_tname, ttype, fields, row_ids, has_predict = data_manager_utils.insert_predict_images(proj_id=proj_id, data_dir=pred_path)
        
        # update data version information
        version = pred_version
        target = target
        pre_rowids = []
        pre_rowids = versioning.select_predict_points(proj_id, version, target)
        row_ids = pre_rowids + row_ids # update rowids
        total_datapoints = len(row_ids)
        versioning.update_predict_version_node(proj_id, version, target, row_ids, total_datapoints)

    # process predict information
    def process_predict(self, pred_version, target='predict'):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        pred_version = str(pred_version)
        versioning = self.versioning
        pred_node = versioning.select_predict_version(proj_id, pred_version, target)
        if not pred_node:
            print("Predict version {} and target {} do not exist!".format(pred_version, target))
            return None
            
        # get data node information
        data_points = pred_node.get("data_points")
        has_predict = pred_node.get("has_predict")
        # select data from storage
        self.data = data_manager_utils.get_predict_images(proj_id, data_points)
        
        self.process_images()
        self.process_annotations()
        self.process_categories()

    # get images                
    def process_images(self):
        self.images = {}
        for image in self.data['images']:
            image_id = image['id']
            if image_id in self.images:
                print("ERROR: Skipping duplicate image id: {}".format(image))
            else:
                self.images[image_id] = image
     
    # get annotations           
    def process_annotations(self):
        self.annotations = {}
        for annotation in self.data['annotations']:
            image_id = annotation['image_id']
            if image_id not in self.annotations:
                self.annotations[image_id] = []
            self.annotations[image_id].append(annotation)


    # get categories
    def process_categories(self):
        self.categories = {}
        for annotation in self.data['annotations']:
            cat_name = annotation['label']
            
            # Add category to the categories dict
            if cat_name not in self.categories:
                category = {'name': cat_name}
                category['count'] = 0
                self.categories[cat_name] = category


    # API to filter predict version by filter conditions
    def select_predict_version(self, pred_version, target, filter_conditions=None, predict=True):
        # process data
        self.process_predict(pred_version, target)

        # no filter
        if filter_conditions is None:
            filtered_images = []
            filtered_annots = []
            for _, image_id in enumerate(self.images):
                image = self.images[image_id]
                if predict:
                    if self.annotations.get(image_id):
                        filtered_images.append(image)
                        filtered_annots.append(self.annotations.get(image_id))
                else:
                    filtered_images.append(image)
                    if self.annotations.get(image_id):
                        filtered_annots.append(self.annotations.get(image_id))
                    else:
                        filtered_annots.append([])
            # return filtered results
            return {'images': filtered_images, 'annotations': filtered_annots}
        
        # filter condition
        filter_file_names = None
        if "file_names" in filter_conditions:
            filter_file_names = filter_conditions["file_names"]
            filter_file_names = filter_file_names.split(',')
            filter_file_names = [s.strip() for s in filter_file_names]
        filter_labels = None
        if "labels" in filter_conditions:
            if not annotation:
                print("Predict version {} does not have any labels!".format(pred_version))
                return None
            filter_labels = filter_conditions["labels"]
            filter_labels = filter_labels.split(',')
            filter_labels = [s.strip() for s in filter_labels]
        
        filtered_images = []
        filtered_annots = []
        for _, image_id in enumerate(self.images):
            image = self.images[image_id]
            imageName = image['name']
            if filter_file_names and imageName not in filter_file_names:
                continue
            if not filter_labels:
                if predict:
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
        # return filtered results
        return {'images': filtered_images, 'annotations': filtered_annots}

if __name__ == "__main__":
    print('main')
        
