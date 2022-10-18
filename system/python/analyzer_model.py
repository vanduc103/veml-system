import dan
import sys, os, time, ast
import pandas as pd
from datetime import datetime
import re

import data_manager_utils
from version_utils import Versioning
from kg_model_knowledge import KGModelKnowledge


local_head_addr = f'{dan.get_local_ip()}:'

class AnalyzerModel():
    """Analyzer API for Model analysis"""

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


    #### Model Analyzer APIs #####
    
    def metadata_model_version(self, model_version):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        model_version = str(model_version)
        versioning = Versioning()
        model_node = versioning.select_model_version(proj_id, model_version)
        if not model_node:
            print("Model version {} does not exist!".format(model_version))
            return None
            
        # get metadata and version features
        metadata_feature_names = ['timestamp', 'model_version', 'model_name', 'model_type', 'model_framework', 'model_input', 'model_classes']
        metadata_features = []
        
        metadata_id = model_node["metadata_id"]
        metadata_values = data_manager_utils.select_model_metadata(metadata_id)
        for feature_name in metadata_feature_names:
            metadata_features.append(metadata_values[feature_name])
            
        version_feature_names = ['base_model_version']
        version_features = []
        base_model_node = versioning.select_base_model_version_relationship(proj_id, model_version)
        if base_model_node:
            base_model_version = base_model_node["version"]
            version_features.append(base_model_version)
        else:
            version_features.append(None)
        
        # Add model knowledge
        kg_model = KGModelKnowledge()
        # model type (manual)
        model_type = metadata_values['model_type']
        model_od = kg_model.add_knowledge_node("Model_Type", model_type)
        kg_model.add_rel_model_version_knowledge(model_node, "Model_Type", model_od)
        
        # model architecture (infer from model name)
        model_name = metadata_values['model_name']
        if re.search('ssd', model_name, re.IGNORECASE) or re.search('efficientdet', model_name, re.IGNORECASE):
            model_ssd = kg_model.add_knowledge_node("Model_Architecture", "1-Stage SSD")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Architecture", model_ssd)
            
        if re.search('yolo', model_name, re.IGNORECASE):
            model_yolo = kg_model.add_knowledge_node("Model_Architecture", "1-Stage YOLO")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Architecture", model_yolo)
            
        if re.search('fasterrcnn', model_name, re.IGNORECASE) or re.search('faster_rcnn', model_name, re.IGNORECASE):
            model_fasterrcnn = kg_model.add_knowledge_node("Model_Architecture", "2-Stage FasterRCNN")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Architecture", model_fasterrcnn)
        
        if re.search('retina', model_name, re.IGNORECASE) or re.search('retinanet', model_name, re.IGNORECASE):
            model_retina = kg_model.add_knowledge_node("Model_Architecture", "RetinaNet")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Architecture", model_retina)
        
        if re.search('centernet', model_name, re.IGNORECASE):
            model_centernet = kg_model.add_knowledge_node("Model_Architecture", "Keypoint-based CenterNet")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Architecture", model_centernet)
            
        #if re.search('fpn', model_name, re.IGNORECASE):
        #    model_fpn = kg_model.add_knowledge_node("Model_Architecture", "Feature Pyramid Network")
        #    kg_model.add_rel_model_version_knowledge(model_node, "Model_Architecture", model_fpn)
        
        if re.search('mobilenet_v2', model_name, re.IGNORECASE) or re.search('mobilenet v2', model_name, re.IGNORECASE):
            model_mobilenet_v2 = kg_model.add_knowledge_node("Model_Backbone", "Mobilenet V2")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_mobilenet_v2)
        elif re.search('mobilenet_v1', model_name, re.IGNORECASE) or re.search('mobilenet v1', model_name, re.IGNORECASE):
            model_mobilenet_v1 = kg_model.add_knowledge_node("Model_Backbone", "Mobilenet V1")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_mobilenet_v1)
        elif re.search('mobilenet', model_name, re.IGNORECASE):
            # default to mobilenet v1
            model_mobilenet_v1 = kg_model.add_knowledge_node("Model_Backbone", "Mobilenet V1")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_mobilenet_v1)
        
        if re.search('resnet', model_name, re.IGNORECASE):
            if re.search('resnet50', model_name, re.IGNORECASE):
                model_resnet = kg_model.add_knowledge_node("Model_Backbone", "ResNet50")
                kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_resnet)
            elif re.search('resnet101', model_name, re.IGNORECASE):
                model_resnet = kg_model.add_knowledge_node("Model_Backbone", "ResNet101")
                kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_resnet)
            else:
                model_resnet = kg_model.add_knowledge_node("Model_Backbone", "ResNet50")
                kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_resnet)
        
        if re.search('efficient', model_name, re.IGNORECASE):
            if re.search('d0', model_name, re.IGNORECASE) or re.search('b0', model_name, re.IGNORECASE):
                model_efficientnet = kg_model.add_knowledge_node("Model_Backbone", "EfficientNet-B0")
                kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_efficientnet)
            elif re.search('d2', model_name, re.IGNORECASE) or re.search('b2', model_name, re.IGNORECASE):
                model_efficientnet = kg_model.add_knowledge_node("Model_Backbone", "EfficientNet-B2")
                kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_efficientnet)
            else:
                model_efficientnet = kg_model.add_knowledge_node("Model_Backbone", "EfficientNet-B0")
                kg_model.add_rel_model_version_knowledge(model_node, "Model_Backbone", model_efficientnet)
        
        if re.search('coco17', model_name, re.IGNORECASE):
            model_coco17 = kg_model.add_knowledge_node("Model_TrainingData", "COCO 2017")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_TrainingData", model_coco17)
        
        model_framework = metadata_values['model_framework']
        if 'tf2' in model_framework or 'tensorflow2' in model_framework:
            model_fw_tf2 = kg_model.add_knowledge_node("Model_Framework", "Tensorflow2")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Framework", model_fw_tf2)
        elif 'tf1' in model_framework or 'tf' in model_framework or 'tensorflow1' in model_framework or 'tensorflow' in model_framework:
            model_fw_tf1 = kg_model.add_knowledge_node("Model_Framework", "Tensorflow1")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Framework", model_fw_tf1)
        elif 'pytorch' in model_framework or 'torch' in model_framework or 'pt' in model_framework:
            model_fw_pytorch = kg_model.add_knowledge_node("Model_Framework", "Pytorch")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_Framework", model_fw_pytorch)
        
        model_input = metadata_values['model_input']
        if '320' in model_input:
            model_320 = kg_model.add_knowledge_node("Model_InputSize", "320x320")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_InputSize", model_320)
        elif '300' in model_input:
            model_300 = kg_model.add_knowledge_node("Model_InputSize", "300x300")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_InputSize", model_300)
        elif '640' in model_input:
            model_640 = kg_model.add_knowledge_node("Model_InputSize", "640x640")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_InputSize", model_640)
        elif '512' in model_input:
            model_512 = kg_model.add_knowledge_node("Model_InputSize", "512x512")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_InputSize", model_512)
        elif '768' in model_input:
            model_768 = kg_model.add_knowledge_node("Model_InputSize", "768x768")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_InputSize", model_768)
        elif '224' in model_input:
            model_224 = kg_model.add_knowledge_node("Model_InputSize", "224x224")
            kg_model.add_rel_model_version_knowledge(model_node, "Model_InputSize", model_224)
        
        # select knowledge base features
        kg_feature_names = []
        kg_feature_values = []
        model_backbones = kg_model.select_model_knowledge_by_model_version(model_version, "Model_Backbone")
        if model_backbones:
            kg_feature_names.append("Model_Backbone")
            models = []
            for model in model_backbones:
                models.append(model['name'])
            kg_feature_values.append(",".join(models))
        model_architectures = kg_model.select_model_knowledge_by_model_version(model_version, "Model_Architecture")
        if model_architectures:
            kg_feature_names.append("Model_Architecture")
            archs = []
            for model_arch in model_architectures:
                archs.append(model_arch['name'])
            kg_feature_values.append(",".join(archs))
        model_inputs = kg_model.select_model_knowledge_by_model_version(model_version, "Model_InputSize")
        if model_inputs:
            kg_feature_names.append("Model_InputSize")
            inputs = []
            for model_input in model_inputs:
                inputs.append(model_input['name'])
            kg_feature_values.append(",".join(inputs))
        
        # result results as dataframe
        results = []
        results.append(metadata_feature_names + version_feature_names + kg_feature_names)
        results.append(metadata_features + version_features + kg_feature_values)
        df = pd.DataFrame(data=results[1:], columns=results[0])
        return df

    
    # compare between model version
    def compare_model_version(self, base_version, compare_versions):
        # get proj_id
        proj_id = self.proj_id
        if not proj_id:
            print('Project {} does not exist!'.format(self.proj_name))
            return None
            
        # check exist base version
        base_version = str(base_version)
        versioning = Versioning()
        base_model_node = versioning.select_model_version(proj_id, base_version)
        if not base_model_node:
            print("Model version {} does not exist!".format(base_version))
            return None
            
        # check exist compare versions
        compare_model_nodes = []
        for compare_version in compare_versions.split(","):
            compare_version = str(compare_version).strip()
            compare_model_node = versioning.select_model_version(proj_id, compare_version)
            if not compare_model_node:
                print("Model version {} does not exist!".format(compare_version))
                return None
            else:
                compare_model_nodes.append(compare_model_node)
        
        # list of comparison data versions
        compare_model_nodes.insert(0, base_model_node)
        compare_feature_names = ["model_version", "timestamp", "model_name", "model_type", 'model_framework', 'model_input', 'model_classes', 'base_model_version']
        
        # get metadata information
        metadata_df = []
        for compare_model_node in compare_model_nodes:
            df = self.metadata_model_version(compare_model_node["version"])
            #df = df[compare_feature_names] # get all feature names
            metadata_df.append(df)
        
        df = pd.concat(metadata_df)
        return df


           
