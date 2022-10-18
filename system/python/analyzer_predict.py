import dan
import sys, os, time
import pandas as pd
import numpy as np
import cv2
from datetime import datetime
import random

import data_manager_utils
from version_utils import Versioning
from predict_manager_images import PredictManagerImages

head_addr = f'{dan.get_local_ip()}:'

class AnalyzerPredict():
    """Analyzer API for Prediction Results"""

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
        self.pred_manager = PredictManagerImages(proj_name, dan_port, head_addr)

    # get predict metadata
    def metadata_predict_version(self, pred_version, target='predict'):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        pred_version = str(pred_version)
        versioning = Versioning()
        pred_node = versioning.select_predict_version(proj_id, pred_version, target)
        if not pred_node:
            print("Predict version {} does not exist!".format(pred_version))
            return None
            
        # get metadata and version features
        metadata_feature_names = ['data_type', 'data_timestamp', 'data_version', 'data_source']
        metadata_features = []
        version_feature_names = ['version', 'target', 'has_predict', 'total_datapoints', 'data_points']
        version_features = []
        
        metadata_id = pred_node["metadata_id"]
        metadata_values = data_manager_utils.select_data_metadata(metadata_id)
        for feature_name in metadata_feature_names:
            metadata_features.append(metadata_values[feature_name])
            
        for feature_name in version_feature_names:
            version_features.append(pred_node[feature_name])
        
        # result results as dataframe
        results = []
        results.append(metadata_feature_names + version_feature_names)
        results.append(metadata_features + version_features)
        df = pd.DataFrame(data=results[1:], columns=results[0])
        return df

    # visualize predict results
    def visualize_predict_version(self, pred_version, target='predict', image_ids=None, filter_labels=None):
        # process predict
        self.pred_manager.process_predict(pred_version, target)
            
        # view data by data points
        images = self.pred_manager.images
        annotations = self.pred_manager.annotations
            
        # view prediction by image id
        cv2_images = []
        if image_ids is None:
            image_ids = [random.choice(list(images.keys()))]
        print(image_ids)
        for i in range(len(image_ids)):
            image_id = image_ids[i]
            image = images[image_id]
            imageName = image['name']
            imagePath = image['path']
            timestamp = image['timestamp']
            # read image
            imageData = cv2.imread(imagePath)
            height, width, _ = imageData.shape
            # draw annotations (if existed)
            if annotations.get(image_id):
                for _, annot in enumerate(annotations[image_id]):
                    label = annot['label']
                    if filter_labels is not None and label not in filter_labels:
                        continue
                    object_type = annot['type']
                    score = annot['score']
                    label = '{}: {}%'.format(label, int(score*100))
                    
                    color = (0,0,255)
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
            cv2_image = {imageName: imageData}
            cv2_images.append(cv2_image)
        
        # Add predict data knowledge
        from kg_data_knowledge import KGDataKnowledge
        kg_data = KGDataKnowledge()
        
        pred_version = str(pred_version)
        versioning = Versioning()
        pred_node = versioning.select_predict_version(self.proj_id, pred_version, target)
        for i in range(len(image_ids)):
            image_id = image_ids[i]
            image = images[image_id]
            # create nodes for predict image data
            pred_image_node = versioning.add_predict_image_node(pred_node, image['name'], image['path'])
            
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
                        # create a new data label
                        label_node = kg_data.add_data_label_node(cat_name)
                    
                    type_node = kg_data.select_label_type_node(categories[cat_name]['type'])
                    if not type_node:
                        # create a new label type
                        type_node = kg_data.add_label_type_node(categories[cat_name]['type'])
                    # add relation data label and label type
                    kg_data.add_rel_data_label_type(label_node, type_node)
                    
                    # add relation predict image to label
                    #kg_data.add_rel_predict_image2label(pred_image_node, label_node, count=categories[cat_name]['count'])
                    
                    # add relation predict version to label
                    kg_data.add_rel_predict_version_knowledge(pred_node, cat_name, "Data_Label", count=categories[cat_name]['count'])
        
            # add data location (manual)
            #location_node = kg_data.add_data_location_node('city street')
            #kg_data.add_rel_predict_image2location(pred_image_node, location_node, address='Seoul Gangnam-gu')
            #kg_data.add_rel_predict_version_knowledge(pred_node, location_node["name"], "Data_Location")
            
            # add data time
            image_timestamp = image['timestamp']
            if image_timestamp.hour >= 5 and image_timestamp.hour <= 19:
                time_node = kg_data.add_data_time_node('daytime')
            else:
                time_node = kg_data.add_data_time_node('nighttime')
            #kg_data.add_rel_predict_image2time(pred_image_node, time_node, timestamp=image_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            kg_data.add_rel_predict_version_knowledge(pred_node, time_node["name"], "Data_Time")
            
            # add data weather
            #weather_node = kg_data.add_data_weather_node('clear')
            #kg_data.add_rel_predict_image2weather(pred_image_node, weather_node)
            #kg_data.add_rel_predict_version_knowledge(pred_node, weather_node["name"], "Data_Weather")
            
            # add image size
            imagesize_node = kg_data.add_data_inputsize_node('{}x{}'.format(image['width'], image['height']))
            kg_data.add_rel_predict_image2inputsize(pred_image_node, imagesize_node)
            kg_data.add_rel_predict_version_knowledge(pred_node, imagesize_node["name"], "Data_InputSize")
        
        
        return cv2_images

    # analyze predict version
    def analyze_predict_version(self, pred_version, target='predict'):
        # process predict
        self.pred_manager.process_predict(pred_version, target)
            
        # view data by data points
        images = self.pred_manager.images
        annotations = self.pred_manager.annotations
        
        # predict analysis
        number_of_images = 0
        images_no_prediction = []
        number_of_objs = 0
        fps = []
        for _, image_id in enumerate(self.pred_manager.images):
            number_of_images += 1
            image = self.pred_manager.images[image_id]
            fps.append(image['fps'])
            if self.pred_manager.annotations.get(image_id):
                for _, annot in enumerate(self.pred_manager.annotations[image_id]):
                    if annot['type'] == 'object':
                        number_of_objs += 1
                    cat_name = annot['label']
                    self.pred_manager.categories[cat_name]['count'] += 1
            else:
                images_no_prediction.append(image_id)
        print('Number of images: {}'.format(number_of_images))
        print('Number of images without prediction: {} ({})'.format(len(images_no_prediction), images_no_prediction))
        print('Number of objects: {}'.format(number_of_objs))
        print('Average objects per image: {:.2f}'.format(number_of_objs/number_of_images))
        # average fps
        if len(fps) > 0:
            print("Average FPS: {:.1f}".format(sum(fps)/len(fps)))
        
        objects_by_class = {}
        for _, cat_name in enumerate(self.pred_manager.categories):
            objects_by_class[self.pred_manager.categories[cat_name]['name']] = self.pred_manager.categories[cat_name]['count']
        # sort by label
        sorted_d = {}
        for label in sorted(objects_by_class):
            sorted_d[label] = objects_by_class[label]
        return sorted_d
        

    # compare between predict version
    def compare_predict_version(self, base_version, compare_versions, target='predict'):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        base_version = str(base_version)
        versioning = Versioning()
        base_pred_node = versioning.select_predict_version(proj_id, base_version, target)
        if not base_pred_node:
            print("Predict version {} does not exist!".format(base_version))
            return None
            
        # check exist compare versions
        compare_pred_nodes = []
        for compare_version in compare_versions.split(","):
            compare_version = str(compare_version).strip()
            compare_pred_node = versioning.select_predict_version(proj_id, compare_version, target)
            if not compare_pred_node:
                print("Predict version {} does not exist!".format(compare_version))
                return None
            else:
                compare_pred_nodes.append(compare_pred_node)
        
        # list of comparison predict versions
        compare_pred_nodes.insert(0, base_pred_node)
        compare_feature_names = ["version", "data_timestamp", "data_source", "total_datapoints"]
        
        # get metadata information
        metadata_df = []
        for compare_pred_node in compare_pred_nodes:
            df = self.metadata_predict_version(compare_pred_node["version"], target)
            df = df[compare_feature_names]
            stat = self.analyze_predict_version(compare_pred_node["version"], target)
            df['objects_by_class'] = str(stat)
            metadata_df.append(df)
        
        results = pd.concat(metadata_df)
        return results

    # compute predict evaluation
    def compute_predict_evaluation(self, pred_version, target='predict'):
        # process predict
        self.pred_manager.process_predict(pred_version, target)
            
        # view data by data points
        images = self.pred_manager.images
        annotations = self.pred_manager.annotations
        
        # predict analysis
        fps = []
        for _, image_id in enumerate(self.pred_manager.images):
            image = self.pred_manager.images[image_id]
            fps.append(image['fps'])
        # average fps
        avg_fps = 0
        if len(fps) > 0:
            avg_fps = sum(fps)/len(fps)
        
        return avg_fps
           
