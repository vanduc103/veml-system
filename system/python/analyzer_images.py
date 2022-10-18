import dan
import sys, os, time
import pandas as pd
import numpy as np
import cv2
from datetime import datetime
import random

import data_manager_utils
from version_utils import Versioning
from data_manager_images import DataManagerImages
from kg_data_knowledge import KGDataKnowledge

head_addr = f'{dan.get_local_ip()}:'

class AnalyzerImages():
    """Analyzer API for Images data type"""

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
        self.data_manager = DataManagerImages(proj_name, dan_port, head_addr)
        self.versioning = Versioning()

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
        number_of_images = 0
        images_no_annotation = []
        number_of_annots = {}
        for _, image_id in enumerate(self.data_manager.images):
            number_of_images += 1
            if self.data_manager.annotations.get(image_id):
                for _, annot in enumerate(self.data_manager.annotations[image_id]):
                    annot_format = ''
                    annot_type = annot['type']
                    if annot_type not in number_of_annots:
                        number_of_annots[annot_type] = 1
                    else:
                        number_of_annots[annot_type] += 1
                    cat_name = annot['label']
                    self.data_manager.categories[cat_name]['type'] = annot['type']
                    self.data_manager.categories[cat_name]['format'] = annot_format
                    self.data_manager.categories[cat_name]['count'] += 1
            else:
                images_no_annotation.append(image_id)
        #print('Number of images: {}'.format(number_of_images))
        #print('Number of images without annotation: {} ({})'.format(len(images_no_annotation), images_no_annotation))
        #for annot_type in list(number_of_annots.keys()):
        #    print('Number of {}: {}'.format(annot_type, number_of_annots[annot_type]))
        #    print('Average {} per image: {:2f}'.format(annot_type, number_of_annots[annot_type]/number_of_images))
        
        # list test version of this data version (if existed)
        #if not test_versions:
        #    test_versions = self.data_manager.list_test_version_of_data_version(data_version)
        test_predictions = []
        if test_versions:
            for test_version in test_versions:
                # process test version results
                test_prediction = self.data_manager.process_test_results(test_version)
                if test_prediction: test_predictions.append(test_prediction)
        # test results analysis
        for i in range(len(test_predictions)):
            test_prediction = test_predictions[i]
            number_of_test_images = 0
            number_of_preds = {}
            for _, image_id in enumerate(self.data_manager.images):
                image = self.data_manager.images[image_id]
                imageName = image['name']
                if test_prediction.get(imageName):
                    number_of_test_images += 1
                    for _,annot in enumerate(test_prediction[imageName]):
                        annot_type = annot['type']
                        if annot_type not in number_of_preds:
                            number_of_preds[annot_type] = 1
                        else:
                            number_of_preds[annot_type] += 1
            #print('Testing version: {}'.format(test_versions[i]))
            #print('Number of test images: {}'.format(number_of_test_images))
            #for annot_type in list(number_of_preds.keys()):
            #    print('Number of {}: {}'.format(annot_type, number_of_preds[annot_type]))
            #    print('Average {} per image: {:2f}'.format(annot_type, number_of_preds[annot_type]/number_of_test_images))
        
        # return objects by class
        objects_by_class = {}
        for _, cat_name in enumerate(self.data_manager.categories):
            objects_by_class[self.data_manager.categories[cat_name]['name']] = self.data_manager.categories[cat_name]['count']
        # sort by label
        sorted_d = {}
        for label in sorted(objects_by_class):
            sorted_d[label] = objects_by_class[label]
        return sorted_d
        
    
    def visualize_data_version(self, data_version, data_target, image_ids=None, image_name=None, test_versions=None, filter_labels=None, example_count=1):
        # process data
        self.data_manager.process_data(data_version, data_target)
            
        # load all data (TODO: performance problem)
        images = self.data_manager.images
        annotations = self.data_manager.annotations
        
        # list test version of this data version (if existed)
        if not test_versions:
            list_test_versions = self.data_manager.list_test_version_of_data_version(data_version)
            print('List of test versions:')
            print(list_test_versions)
        test_predictions = []
        test_nodes = []
        if test_versions:
            for test_version in test_versions:
                # process test version results
                test_prediction = self.data_manager.process_test_results(test_version)
                if test_prediction: test_predictions.append(test_prediction)
                test_nodes.append(self.versioning.select_test_version(self.proj_id, test_version))
        
        # get image id by label
        if filter_labels:
            image_ids = []
            for image_id, annots in annotations.items():
                has_label = False
                for _, annot in enumerate(annots):
                    label = annot['label']
                    if label in filter_labels:
                        has_label = True
                        break
                if has_label:
                    image_ids.append(image_id)
                    if len(image_ids) >= example_count:
                        break
        if not image_ids:
            image_ids = random.sample(list(images.keys()), example_count)
        cv2_images = []
        for i in range(len(image_ids)):
            image_id = image_ids[i]
            if not images.get(image_id, None):
                continue
            image = images[image_id]
            imageName = image['name']
            imagePath = image['path']
            # read image
            imageData = cv2.imread(imagePath)
            height, width, _ = imageData.shape
            # draw annotations (if existed)
            gt_labels = []
            if annotations.get(image_id):
                for _, annot in enumerate(annotations[image_id]):
                    label = annot['label']
                    if filter_labels is not None and label not in filter_labels:
                        continue
                    gt_labels.append(label)
                    object_type = annot['type']
                    color = (255,0,0) # blue color
                    thick = 3
                    # draw bounding box
                    if object_type == 'object':
                        bbox = annot['bbox']
                        xmin = bbox[0]
                        ymin = bbox[1]
                        xmax = bbox[2]
                        ymax = bbox[3]
                        left = int(xmin*width)
                        top = int(ymin*height)
                        right = int(xmax*width)
                        bottom = int(ymax*height)
                        cv2.rectangle(imageData, (left, top), (right, bottom), color, thick)
                        # draw label
                        cv2.putText(imageData, label, (left, top - 12), 0, 1e-3 * height, color, thick-1)
                    # draw lines
                    elif object_type == 'line':
                        # get x, y points
                        points = annot['points']
                        for point1, point2 in zip(points, points[1:]): 
                            cv2.line(imageData, point1, point2, color, thick+1)
                        # draw label
                        left, top = points[0][0], points[0][1]
                        cv2.putText(imageData, label, (left, top - 12), 0, 1e-3 * height, color, thick-1)
                    # draw image id and image name
                    #cv2.putText(imageData, '{}: {}'.format(image_id, imageName), (20, 40), 0, 1e-3 * height*1.5, (255,255,255), thick)
            
            # draw test predictions (if existed)
            pred_labels = []
            for test_prediction in test_predictions:
                if test_prediction.get(imageName):
                    for _,annot in enumerate(test_prediction[imageName]):
                        label = annot['label']
                        #if filter_labels is not None and label not in filter_labels:
                        #    continue
                        pred_labels.append(label)
                        object_type = annot['type']
                        score = annot['score']
                        label = '{}: {}%'.format(label, int(score*100))
                        color = (0,0,255) # red color
                        thick = 3
                        # draw bounding box
                        if object_type == 'object':
                            bbox = annot['bbox']
                            xmin = bbox[0]
                            ymin = bbox[1]
                            xmax = bbox[2]
                            ymax = bbox[3]
                            left = int(xmin*width)
                            top = int(ymin*height)
                            right = int(xmax*width)
                            bottom = int(ymax*height)
                            cv2.rectangle(imageData, (left, top), (right, bottom), color, thick)
                            # draw label
                            cv2.putText(imageData, label, (left, top - 12), 0, 1e-3 * height, color, thick-1)
                        # draw lines
                        elif object_type == 'line':
                            # get x, y points
                            points = annot['points']
                            for point1, point2 in zip(points, points[1:]): 
                                cv2.line(imageData, point1, point2, color, thick+1)
                            # draw label
                            left, top = points[0][0], points[0][1]
                            cv2.putText(imageData, label, (left, top - 12), 0, 1e-3 * height, color, thick-1)
            # return cv image
            cv2_image = {imageName: imageData, 'gt_labels': gt_labels, 'pred_labels': pred_labels}
            cv2_images.append(cv2_image)
        
        # Add data knowledge
        kg_data = KGDataKnowledge()
        
        data_node = self.versioning.select_data_version(self.proj_id, str(data_version).strip(), data_target)
        for i in range(len(image_ids)):
            image_id = image_ids[i]
            if not images.get(image_id, None):
                continue
            image = images[image_id]
            # create nodes for image data
            # image_node = self.versioning.add_image_data_node(data_node, image['name'], image['path'])
            
            # add data label node
            if annotations.get(image_id):
                categories = {}
                for _, annot in enumerate(annotations[image_id]):
                    label = annot['label']
                    label_type = annot['type']
                    if label not in categories:
                        categories[label] = {'type': label_type, 'count': 1}
                    else:
                        categories[label]['count'] += 1
                
                for _, cat_name in enumerate(categories):
                    label_node = kg_data.select_data_label_node(cat_name)
                    if not label_node:
                        label_node = kg_data.select_data_label_node(cat_name, data_version=data_node['version'])
                    if not label_node:
                        # create a new data label
                        label_node = kg_data.add_data_label_node(cat_name)
                    
                    type_node = kg_data.select_label_type_node(categories[cat_name]['type'])
                    if not type_node:
                        # create a new label type
                        type_node = kg_data.add_label_type_node(categories[cat_name]['type'])
                    # add relation data label and label type
                    kg_data.add_rel_data_label_type(label_node, type_node)
                    
                    # add relation image to label
                    #kg_data.add_rel_image2label(image_node, label_node, count=categories[cat_name]['count'])
                    
                    # add relation data version to label
                    kg_data.add_rel_data_version_knowledge(data_node, cat_name, "Data_Label", count=categories[cat_name]['count'])
        
            # other data knowledge
            # data location (manual)
            '''location_node = kg_data.add_data_location_node('city street')
            #kg_data.add_rel_image2location(image_node, location_node, address='Seoul Gangnam-gu')
            kg_data.add_rel_data_version_knowledge(data_node, location_node["name"], "Data_Location")
            
            # data time (infer from image timestamp)
            image_timestamp = os.path.splitext(image['name'])[0]
            timestamp = datetime.strptime(' '.join(image_timestamp.split('_')[1:3]), '%Y%m%d %H%M%S')
            if timestamp.hour >= 5 and timestamp.hour <= 19:
                time_node = kg_data.add_data_time_node('daytime')
            else:
                time_node = kg_data.add_data_time_node('nighttime')
            #kg_data.add_rel_image2time(image_node, time_node, timestamp=timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            kg_data.add_rel_data_version_knowledge(data_node, time_node["name"], "Data_Time")
            
            # data weather (manual)
            weather_node = kg_data.add_data_weather_node('clear')
            #kg_data.add_rel_image2weather(image_node, weather_node)
            kg_data.add_rel_data_version_knowledge(data_node, weather_node["name"], "Data_Weather")'''
            
            # image size (from image data)
            imagesize_node = kg_data.add_data_inputsize_node('{}x{}'.format(image['width'], image['height']))
            #kg_data.add_rel_image2inputsize(image_node, imagesize_node)
            kg_data.add_rel_data_version_knowledge(data_node, imagesize_node["name"], "Data_InputSize")
            
            ### knowledge graph for test image (if existed)
            for test_node, test_prediction in zip(test_nodes, test_predictions):
                imageName = image['name']
                if test_prediction.get(imageName):
                    # add test image node
                    #test_image_node = self.versioning.add_test_image_node(test_node, image['name'], image['path'])
                    
                    # add label information
                    categories = {}
                    for _, annot in enumerate(test_prediction[imageName]):
                        label = annot['label']
                        label_type = annot['type']
                        if label not in categories:
                            categories[label] = {'type': label_type, 'count': 1}
                        else:
                            categories[label]['count'] += 1
                    
                    for _, cat_name in enumerate(categories):
                        label_node = kg_data.select_data_label_node(cat_name)
                        if not label_node:
                            # create a new data label
                            label_node = kg_data.add_data_label_node(cat_name)
                        
                        type_node = kg_data.select_label_type_node(categories[cat_name]['type'])
                        if not type_node:
                            # create a new label type
                            type_node = kg_data.add_label_type_node(categories[cat_name]['type'])
                        # add relation data label and label type
                        kg_data.add_rel_data_label_type(label_node, type_node)
                        
                        # add relation test image to label
                        #kg_data.add_rel_test_image2label(test_image_node, label_node, count=categories[cat_name]['count'])
                        
                        # add relation test version to label
                        kg_data.add_rel_test_version_knowledge(test_node, cat_name, "Data_Label", count=categories[cat_name]['count'])
        
        return cv2_images
        

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
            stat = self.analyze_data_version(compare_data_node["version"], data_target)
            df['objects_by_class'] = str(stat)
            metadata_df.append(df)
        
        results = pd.concat(metadata_df)
        return results
        
