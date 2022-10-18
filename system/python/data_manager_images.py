import dan
import sys, os, time
import numpy as np
import data_manager_utils
from version_utils import Versioning


head_addr = f'{dan.get_local_ip()}:'

class DataManagerImages():
    """Data Manager for Images data type"""

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

    # add new data version with 1 data dir for both images and annotations
    def addnew_data_version(self, data_path, data_version, data_target, data_description):
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
        data_type = 'Images'
        data_version = data_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': data_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert images data
        data_tname, ttype, fields, row_ids, annotation = data_manager_utils.insert_data_images(proj_id=proj_id, data_dir=data_path)
        
        # insert data version
        versioning = self.versioning
        versioning.insert_data_version_node(proj_id, dt_id, data_version, data_target, data_description, data_tname, row_ids, annotation)

    # add new data version with seperate images and annotations dir
    def addnew_data_version_2(self, image_path, annotation_path, data_version, data_target, data_description):
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
        data_source = image_path
        data_type = 'Images'
        data_version = data_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': data_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert images data
        data_tname, ttype, fields, row_ids, annotation = data_manager_utils.insert_data_images_2(proj_id=proj_id, images_dir=image_path, annotations_dir=annotation_path)
        
        # insert data version
        versioning = self.versioning
        versioning.insert_data_version_node(proj_id, dt_id, data_version, data_target, data_description, data_tname, row_ids, annotation)


    # insert new version data by updating from previous version
    def addupdate_data_version(self, data_path, data_version, data_target, from_version):
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
                
        # insert new metadata
        data_source = data_path
        data_type = 'Images'
        data_version = data_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': data_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert images data
        data_tname, ttype, fields, row_ids, annotation = data_manager_utils.insert_data_images(proj_id=proj_id, data_dir=data_path)
        
        # insert data version
        version = data_version
        pre_version = str(from_version)
        target = data_target
        pre_rowids = []
        versioning = self.versioning
        pre_rowids = versioning.select_data_points(proj_id, pre_version, target)
        row_ids = pre_rowids + row_ids
        total_datapoints = len(row_ids)
        versioning.insert_data_ver2ver(pre_version, dt_id, proj_id, version, target, data_tname, row_ids, total_datapoints, annotation)

        
    # insert new version by merging from list of versions
    def addmerge_data_version(self, data_version, data_target, merge_versions):
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

        # check merge versions exist
        merge_version_list = merge_versions.split(',')
        merge_version_list = [s.strip() for s in merge_version_list]
        for pre_version in merge_version_list:
            pre_version = str(pre_version)
            if not self.check_data_version(proj_id, pre_version, data_target):
                print("Data version {} and target {} does not exist!".format(pre_version, data_target))
                return None
                
        # insert new metadata
        data_source = 'merge_versions:{}'.format(merge_versions)
        data_type = 'Images'
        data_version = data_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': data_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert data version
        version = data_version
        target = data_target
        data_points = []
        annot_points = []
        versioning = self.versioning
        for pre_version in merge_version_list:
            pre_version = str(pre_version)
            data_node = versioning.select_data_version(proj_id, pre_version, data_target)
            data_tname = data_node.get("data_tname", "")
            annotation = data_node.get("annotation", True)
            # concat data_points of merged versions
            pre_data_points = data_node.get("data_points", [])
            data_points = data_points + pre_data_points
            # concat annotation points
            pre_annot_points = data_node.get("annotation_points", [])
            annot_points = annot_points + pre_annot_points
        versioning.insert_data_version_merged(merge_version_list, dt_id, proj_id, version, target, data_points, annot_points, data_tname, annotation)

    # update an existing version with new data
    def update_data_version(self, data_path, data_version, data_target):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
        
        # check version
        data_version = str(data_version)
        versioning = self.versioning
        data_node = versioning.select_data_version(proj_id, data_version, data_target)
        if not data_node:
            print("Updating a data version but data version {} and target {} do not exist!".format(data_version, data_target))
            return None
                
        # get data version metadata
        dt_id = data_node["metadata_id"]
        
        # insert images data
        data_tname, ttype, fields, row_ids, annotation = data_manager_utils.insert_data_images(proj_id=proj_id, data_dir=data_path)
        
        # update data version information
        version = data_version
        target = data_target
        pre_rowids = []
        pre_rowids = versioning.select_data_points(proj_id, version, target)
        row_ids = pre_rowids + row_ids # update rowids
        total_datapoints = len(row_ids)
        versioning.update_data_version_node(proj_id, version, target, row_ids, total_datapoints)


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
            data_tname = data_node.get("data_tname")
            annotation = data_node.get("annotation")
            
            annot_points_version = data_node.get("annotation_points")
            # merge 2 list without duplicates
            if annot_points_version:
                annot_points = annot_points + list(set(annot_points_version) - set(annot_points))
            
        # select data from storage
        self.data = data_manager_utils.get_data_images(proj_id, data_points, annot_points)
        
        self.process_images()
        self.process_annotations()
        self.process_categories()

    # process images data
    def process_images(self):
        self.images = {}
        for image in self.data['images']:
            image_id = image['id']
            if image_id in self.images:
                print("ERROR: Skipping duplicate image id: {}".format(image))
            else:
                self.images[image_id] = image
     
    # process annotations information
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


    # select images and annotations from data version
    def select_data_version(self, data_version, data_target):
        # process data
        self.process_data(data_version, data_target)
        
        # return images and annotations
        images = []
        annots = []
        categories = []
        for _, image_id in enumerate(self.images):
            image = self.images[image_id]
            if self.annotations.get(image_id):
                images.append(image)
                annots.append(self.annotations.get(image_id))
        for _, cat_name in enumerate(self.categories):
            categories.append(cat_name)
        return {'images': images, 'annotations': annots, 'categories': categories}


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
        # select test results from storage
        test_results = data_manager_utils.get_test_images(proj_id, data_points)
        
        # process images data
        test_images = {}
        for image in test_results['images']:
            image_id = image['id']
            test_images[image_id] = image
        
        # process predictions information
        self.test_predictions = {}
        self.test_images = {}
        for image in test_results['images']:
            image_name = image['name']
            self.test_predictions[image_name] = []
            self.test_images[image_name] = image
        for annotation in test_results['annotations']:
            image_id = annotation['image_id']
            image = test_images[image_id]
            image_name = image['name']
            self.test_predictions[image_name].append(annotation)
        
        return self.test_predictions

    def compute_test_evaluation(self, test_version, data_version, data_labels=None, data_target='train'):
        test_predictions = self.process_test_results(test_version)
        data_results = self.select_data_version(data_version, data_target)
        images = data_results["images"]
        annotations = data_results["annotations"]
        categories = data_results["categories"]
        label_dict = {}
        for label_id in range(len(categories)):
            label_name = categories[label_id]
            if data_labels is not None:
                if label_name in data_labels:
                    label_dict[label_name] = label_id
            else:
                label_dict[label_name] = label_id
        
        # create metric_fn
        from mean_average_precision import MetricBuilder
        metric_fn = MetricBuilder.build_evaluation_metric("map_2d", async_mode=False, num_classes=len(categories))
        # add samples to evaluation
        fps = []
        for (image, gt_annots) in zip(images, annotations):
            image_name = image['name']
            width, height = int(image['width']), int(image['height'])
            if image_name not in test_predictions:
                continue
            test_annots = test_predictions[image_name]
            test_image = self.test_images[image_name]
            fps.append(test_image['fps'])
            
            gt = []
            for box_data in gt_annots:
                if box_data["label"] not in label_dict:
                    continue
                if not box_data.get("bbox"):
                    continue
                gt_class_id = label_dict[box_data["label"]]
                gt.append([int(box_data["bbox"][0]*width), int(box_data["bbox"][1]*height), int(box_data["bbox"][2]*width), int(box_data["bbox"][3]*height), 
                            gt_class_id, 0, 0]) # [xmin, ymin, xmax, ymax, class_id, difficult, crowd]
            gt = np.array(gt)
            #print(gt)
            preds = []
            for box_data in test_annots:
                if box_data["label"] not in label_dict:
                    continue
                if not box_data.get("bbox"):
                    continue
                pred_class_id = label_dict[box_data["label"]]
                preds.append([int(box_data["bbox"][0]*width), int(box_data["bbox"][1]*height), int(box_data["bbox"][2]*width), int(box_data["bbox"][3]*height), 
                            pred_class_id, float(box_data["score"])]) # [xmin, ymin, xmax, ymax, class_id, confidence]
            preds = np.array(preds)
            #print(preds)
            metric_fn.add(preds, gt)

        # compute PASCAL VOC metric
        pascal_map = metric_fn.value(iou_thresholds=0.5)['mAP']
        print(f"VOC PASCAL mAP in all points: {pascal_map*100}")

        # compute metric COCO metric
        coco_map = metric_fn.value(iou_thresholds=np.arange(0.5, 1.0, 0.05), recall_thresholds=np.arange(0., 1.01, 0.01), mpolicy='soft')['mAP']
        #print(f"COCO mAP: {coco_map*100}")
        
        # average fps
        fps = fps[1:] # first fps is often not accurate
        avg_fps = sum(fps)/len(fps)
        #print("Average FPS: {}".format(avg_fps))
        
        return coco_map*100, avg_fps

    def compute_test_evaluation_classification(self, test_version, data_version, data_labels=None, data_target='train'):
        test_predictions = self.process_test_results(test_version)
        data_results = self.select_data_version(data_version, data_target)
        images = data_results["images"]
        annotations = data_results["annotations"]
        categories = data_results["categories"]
        
        # create metric_fn
        # add samples to evaluation
        fps = []
        match_sum = 0
        data_cnt = 0
        for (image, gt_annots) in zip(images, annotations):
            image_name = image['name']
            if image_name not in test_predictions:
                continue
            test_annots = test_predictions[image_name]
            test_image = self.test_images[image_name]
            
            gt_label = gt_annots[0]["label"]
            if data_labels and gt_label not in data_labels:
                continue
            fps.append(test_image['fps'])
            data_cnt += 1
                
            pred_label = test_annots[0]["label"]
            if pred_label == gt_label:
                match_sum += 1
            
        # average fps
        fps = fps[1:] # first fps is often not accurate
        avg_fps = sum(fps)/len(fps)
        #print("Average FPS: {}".format(avg_fps))
        test_acc = 0
        if data_cnt > 0:
            test_acc = (match_sum/data_cnt)*100
        return test_acc, avg_fps

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
        data_tname = data_node["data_tname"]
        annotation = data_node["annotation"]
        versioning.addfilter_data_version(proj_id, dt_id, from_versions, from_target, new_version, new_target, data_description, row_ids, annot_ids, str(filter_conditions), data_tname, annotation)


    #### Face Age dataset ####
    # add new data face-age version
    def addnew_data_version_face_age(self, image_path, label_file, data_version, data_target, data_description):
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
        data_source = image_path
        data_type = 'Images'
        data_version = data_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': data_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert images data
        data_tname, ttype, fields, row_ids, annotation = data_manager_utils.insert_data_face_age(proj_id=proj_id, images_dir=image_path, label_file=label_file)
        
        # insert data version
        version = data_version
        target = data_target
        versioning = self.versioning
        versioning.insert_data_version_node(proj_id, dt_id, version, target, data_description, data_tname, row_ids, annotation)

        print('View versioning information:')
        versioning.select_project(proj_id)
        versioning.select_data_version(proj_id, version, target)


    #### Soccer Jersey Number dataset ####
    # add new data jersey number version
    def addnew_data_version_soccer_jersey(self, data_dir, data_version, data_target, data_description):
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
        data_source = data_dir
        data_type = 'Images'
        data_version = data_version
        data_metadata = {
            'data_source': data_source,
            'data_type': data_type,
            'data_version': data_version,
        }
        dt_id = data_manager_utils.insert_data_metadata(proj_id, data_metadata)
        
        # insert images data
        data_tname, ttype, fields, row_ids, annotation = data_manager_utils.insert_data_soccer_jersey(proj_id=proj_id, data_dir=data_dir)
        
        # insert data version
        version = data_version
        target = data_target
        versioning = self.versioning
        versioning.insert_data_version_node(proj_id, dt_id, version, target, data_description, data_tname, row_ids, annotation)

        print('View versioning information:')
        versioning.select_project(proj_id)
        versioning.select_data_version(proj_id, version, target)

def main():
    pass
    
if __name__ == "__main__":
    main()
        
