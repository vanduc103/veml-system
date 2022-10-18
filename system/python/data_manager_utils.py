import dan
import csv
import sys, os, time
import numpy as np
import pandas as pd
from datetime import datetime
from constants import *
from dan.data_model import DanGraph

#### Project utils ######
def select_proj_by_name(proj_name):
    tname = project_tname
    proj_id = None
    ret = dan.table_scan(tname=tname)
    if ret:
        for r in ret:
            if proj_name == r[2]:
                proj_id = r[0]
    return proj_id

# create project
def create_project_table(tname=project_tname, ttype='ROW'):
    # create a row-table
    # proj_id, proj_code, proj_name, proj_timestamp, proj_owner
    fields = project_fields
    try:
        dan.create_table(tname=tname, ttype=ttype, **fields)
        dan.create_index(tname=tname, fid=0, unique=True)
    except Exception as e:
        pass
    
    return tname, fields


# insert new project
def insert_project(proj_name, proj_owner, tname=project_tname, ttype='ROW'):
    # create a project table
    tname, fields = create_project_table(tname, ttype)
    
    # fields
    proj_id = dan.get_unique_id()
    proj_code = 'P' + str(proj_id)
    proj_time = int(time.time())
    data = [[proj_id, proj_code, proj_name, proj_time, proj_owner]]
    for d in data:
        dan.insert_record(tname=tname, values=d)
    # view inserted rows
    ret = index_return(tname, proj_id, 0)
    for i, record in enumerate(ret):
        print(record)
    
    # create metadata tables
    create_data_metadata_table()
    create_model_metadata_table()
    create_training_metadata_table()
    create_inference_metadata_table()
    
    # create profiling tables
    create_profiling_table()
    
    return proj_id

#### Data utils ####
def create_data_metadata_table(tname=data_tname, ttype='ROW'):
    # create a row-table
    fields = data_fields
    try:
        dan.create_table(tname=tname, ttype=ttype, **fields)
        dan.create_index(tname=tname, fid=0, unique=True)
    except Exception as e:
        pass
    
    return tname, fields

def insert_data_metadata(proj_id, data_metadata, tname=data_tname, ttype='ROW'):
    # fields
    dt_id = dan.get_unique_id()
    dt_time = int(time.time())
    data = [[dt_id, proj_id, data_metadata['data_type'], dt_time, data_metadata['data_version'], data_metadata['data_source']]]
    for d in data:
        dan.insert_record(tname=tname, values=d)
    # validate rows
    ret = index_return(tname, dt_id, 0)
    for i, record in enumerate(ret):
        print(record)
    
    return dt_id

# import data with 1 data_dir (including image and annotation dirs)
def insert_data_images(proj_id, data_dir, save_files=False, tname=data_tname, tname2=data_annot_tname, ttype='ROW'):
    output_dir = None
    if save_files:
        os.path.join('dataset', 'files', tname + '-' + str(proj_id))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    # read data examples
    from dataset import read_annotation_pascal_voc as dataset
    data_examples, data_annots = dataset.main(data_dir, output_dir)
    
    return insert_data_images_api(proj_id, data_examples, data_annots, tname, tname2, ttype)

# insert data with both image_dir and annotation_dir
def insert_data_images_2(proj_id, images_dir, annotations_dir, save_files=False, tname=data_tname, tname2=data_annot_tname, ttype='ROW'):
    output_dir = None
    if save_files:
        os.path.join('dataset', 'files', tname + '-' + str(proj_id))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    # read data examples
    from dataset import read_annotation_pascal_voc as dataset
    data_examples, data_annots = dataset.main2(images_dir, annotations_dir, output_dir)
    
    return insert_data_images_api(proj_id, data_examples, data_annots, tname, tname2, ttype)


def insert_data_face_age(proj_id, images_dir, label_file, batch_size=1000, save_files=False, tname=data_tname, tname2=data_annot_tname, ttype='ROW'):
    output_dir = None
    if save_files:
        os.path.join('dataset', 'files', tname + '-' + str(proj_id))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    # read data examples
    from dataset import face_age_dataset as dataset
    data_examples, data_annots = dataset.main(images_dir, label_file, output_dir)
    
    return insert_data_images_api(proj_id, data_examples, data_annots, tname, tname2, ttype, batch_size)

def insert_data_soccer_jersey(proj_id, data_dir, batch_size=1000, save_files=False, tname=data_tname, tname2=data_annot_tname, ttype='ROW'):
    output_dir = None
    if save_files:
        os.path.join('dataset', 'files', tname + '-' + str(proj_id))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    # read data examples
    from dataset import soccer_jersey_dataset as dataset
    data_examples, data_annots = dataset.main(data_dir, output_dir)
    
    return insert_data_images_api(proj_id, data_examples, data_annots, tname, tname2, ttype, batch_size)

# import graph data with 1 data_dir (including nodes, edges and labels)
def insert_data_graph(proj_id, data_dir, gname=data_gname, batch_size=1000):
    # node num file
    node_num_file = os.path.join(data_dir, 'num-node-list.csv')
    # edges file
    edge_list_file = os.path.join(data_dir, 'edge.csv')
    # node_label file
    node_label_file = os.path.join(data_dir, 'node-label.csv')
    # node year file
    node_year_file = os.path.join(data_dir, 'node_year.csv')
    # node feature file
    node_feat_file = os.path.join(data_dir, 'node-feat.csv')
    
    # node list
    with open(node_num_file, 'r') as f:
        node_num = f.readline()
    num_node = int(node_num)
    node_list = [x for x in range(num_node)]
    
    # edge list
    with open(edge_list_file, 'r') as f:
        edge_list = pd.read_csv(f, header=None)
    
    # node labels
    with open(node_label_file, 'r') as f:
        node_labels = np.loadtxt(f, dtype=int)
    # node year
    with open(node_year_file, 'r') as f:
        node_years = np.loadtxt(f, dtype=int)
    # node feat
    with open(node_feat_file, 'r') as f:
        node_feat_pd = pd.read_csv(f, header=None)
    node_feat = node_feat_pd.values
    node_info = {'node_labels': node_labels, 'node_years': node_years, 'node_feat': node_feat}
    
    return insert_data_graph_api(proj_id, node_list, edge_list, node_info, gname, batch_size)

# import graph prediction (node prediction)
def insert_pred_graph(proj_id, pred_dir, gname=data_gname, batch_size=1000):
    # node num file
    node_num_file = os.path.join(pred_dir, 'num-node-list.csv')
    # node_label file
    node_label_file = os.path.join(pred_dir, 'node-label.csv')
    # node_predict file
    node_pred_file = os.path.join(pred_dir, 'node-pred.csv')
    
    # node list
    with open(node_num_file, 'r') as f:
        node_num = f.readline()
    num_node = int(node_num)
    node_list = [x for x in range(num_node)]
    
    # node labels
    with open(node_label_file, 'r') as f:
        node_labels = np.loadtxt(f, dtype=int)
    # node predict
    with open(node_pred_file, 'r') as f:
        node_preds = np.loadtxt(f, dtype=int)
    node_info = {'node_labels': node_labels, 'node_preds': node_preds}
    
    return insert_pred_graph_api(proj_id, node_list, node_info, gname, batch_size)


# incrementor function for rid
def incrementor(start_count=0):
    info = {"count": start_count}
    def number():
        info["count"] += 1
        return info["count"]
    return number

def insert_data_images_api(proj_id, data_examples, data_annots, tname=data_tname, tname2=data_annot_tname, ttype='ROW', batch_size=None):    
    # make the fields for the table Data
    fields = {}
    # row id (auto increment)
    fields['rid'] = 'bigint'
    for k, v in data_examples[0].items():
        fields[k] = v.type
    
    # make the fields for the table Annot (if existed)
    has_annotation = False
    if data_annots:
        for data_annot in data_annots:
            if data_annot and len(data_annot) > 0:
                has_annotation = True
                annot_fields = {}
                # link to row id of Data table
                annot_fields['rid'] = 'bigint'
                annot_fields['annot_id'] = 'bigint' # annotation id
                for k, v in data_annot[0].items():
                    annot_fields[k] = v.type
                break
    
    # create a table to store data (for each project)
    tname = tname + '-' + str(proj_id)
    
    if data_annots:
        tname2 = tname2 + '-' + str(proj_id)
    try:
        dan.create_table(tname=tname, ttype=ttype, **fields)
        dan.create_index(tname=tname, fid=0, unique=True)
        if has_annotation:
            dan.create_table(tname=tname2, ttype=ttype, **annot_fields)
    except Exception as e:
        pass
    
    # find start_count of rowid from current rowids
    try:
        rowids = data_return_by_fid(tname, fid=0)
        start_count1 = max(rowids)
    except:
        start_count1 = 0
    rid = incrementor(start_count=start_count1)
    
    # find start_count of annot_id from current annot_ids
    try:
        annot_ids = data_return_by_fid(tname2, fid=1)
        start_count = max(annot_ids)
    except:
        start_count = 0
    annot_id = incrementor(start_count=start_count)
    
    # row ids and annot_ids
    rids = []
    annot_ids = []
    
    # compute batch
    if batch_size is None:
        batch_size = 1
    total_examples = len(data_examples)
    num_of_batch = int(total_examples / batch_size)
    if total_examples > num_of_batch*batch_size:
        num_of_batch += 1
    print(num_of_batch)
    for b in range(num_of_batch):
        batch_img = []
        batch_annot = []
        rids_batch = []
        annot_ids_batch = []
        for i in range(b*batch_size, min(total_examples, (b+1)*batch_size)):
            # prepare image data
            example = data_examples[i]
            data_with_id = []
            data_with_id.append(rid()) #rid
            rids_batch.append(data_with_id[0])
            for k, v in example.items():
                data_with_id.append(v.value)
            batch_img.append(data_with_id)
            
            # prepare annotations
            if has_annotation:
                annot_features = data_annots[i]
                for _, annot_feature_dict in enumerate(annot_features):
                    data_annot = []
                    data_annot.append(data_with_id[0]) #rid
                    data_annot.append(annot_id()) #annot_id
                    annot_ids_batch.append(data_annot[1]) #annot_id
                    for k, v in annot_feature_dict.items():
                        data_annot.append(v.value)
                    batch_annot.append(data_annot)
        # insert by batch
        try:
            dan.insert_record_batch(tname=tname, batch=batch_img)
            if has_annotation:
                dan.insert_record_batch(tname=tname2, batch=batch_annot)
        except:
            rids_batch = []
            annot_ids_batch = []
            print(batch_img)
            print(batch_annot)
        rids = rids + rids_batch
        annot_ids = annot_ids + annot_ids_batch
    
    print("Inserted {} records.".format(len(rids)))
    
    # partition table
    try:
        nodes = dan.nodes()
        # if has worker nodes
        if len(nodes) > 1:
            nodes = [f'{host}:{port}' for _, host, port, head in nodes if not head]
            dan.table_partition(tname=tname, fid=0, nodes=nodes)
            if data_annots:
                dan.table_partition(tname=tname2, fid=0, nodes=nodes)
    except Exception as e:
        pass
    
    return tname, ttype, fields, rids, has_annotation

def insert_data_graph_api(proj_id, node_list, edge_list, node_info, gname=data_gname, batch_size=1):    
    # create general graph w/o property
    gname = gname + '-' + str(proj_id)
    vfields = dict(vid='bigint', label='int', year='int', features='doublelist')
    try:
        graph = DanGraph(name=gname, vfields=vfields, efields={}, vpfid=0)
        print(graph)
    except Exception as e:
        print(e)
        pass
    
    # insert nodes
    if batch_size is None:
        batch_size = 1
    print('batch size: {}'.format(batch_size))
    total_examples = len(node_list)
    num_of_batch = int(total_examples / batch_size)
    if total_examples > num_of_batch*batch_size:
        num_of_batch += 1

    node_labels = node_info['node_labels']
    node_years = node_info['node_years']
    node_feat = node_info['node_feat']
    for b in range(num_of_batch):
        node_batch, label_batch, year_batch, feat_batch = [], [], [], []
        min_idx = b*batch_size
        max_idx = min(total_examples, (b+1)*batch_size)
        node_batch = node_list[min_idx:max_idx]
        label_batch = node_labels[min_idx:max_idx]
        year_batch = node_years[min_idx:max_idx]
        feat_batch = node_feat[min_idx:max_idx]
        
        graph.insert_vertices(zip(node_batch, label_batch, year_batch, feat_batch))
    print('Inserted {} nodes'.format(len(node_list)))
    
    # insert edges
    edges = edge_list.values
    total_examples = edges.shape[0]
    num_of_batch = int(total_examples / batch_size)
    if total_examples > num_of_batch*batch_size:
        num_of_batch += 1

    for b in range(num_of_batch):
        s_batch, d_batch = [], []
        #for i, row in edge_list.iterrows():
        min_idx = b*batch_size
        max_idx = min(total_examples, (b+1)*batch_size)
        s_batch = edges[min_idx:max_idx, 0]
        d_batch = edges[min_idx:max_idx, 1]
        graph.insert_edges(src=s_batch, dest=d_batch)
    print('Inserted {} edges'.format(len(edge_list)))
        
    # insert graph data annotation
    '''has_annotation = True
    annot_fields = {}
    # link to node id of Data graph
    annot_fields['rid'] = 'bigint'
    annot_fields['annot_id'] = 'bigint' # annotation id
    annot_fields['label'] = 'bigint' # node label
    
    annot_tname = annot_tname + '-' + str(proj_id)
    try:
        if has_annotation:
            dan.create_table(tname=annot_tname, ttype=ttype, **annot_fields)
    except Exception as e:
        pass
        
    # find start_count of annot_id from current annot_ids
    try:
        annot_ids = data_return_by_fid(annot_tname, fid=1)
        start_count = max(annot_ids)
    except:
        start_count = 0
    annot_id = incrementor(start_count=start_count)
    
    # annot_ids
    annot_ids = []
    
    # compute batch
    if batch_size is None:
        batch_size = 1
    total_examples = len(node_labels)
    num_of_batch = int(total_examples / batch_size)
    if total_examples > num_of_batch*batch_size:
        num_of_batch += 1
    print(num_of_batch)
    for b in range(num_of_batch):
        batch_annot = []
        annot_ids_batch = []
        for i in range(b*batch_size, min(total_examples, (b+1)*batch_size)):
            # prepare annotations
            if has_annotation:
                label = node_labels[i]
                data_annot = []
                node_id = node_list[i]
                data_annot.append(node_id) #rid
                data_annot.append(annot_id()) #annot_id
                annot_ids_batch.append(data_annot[1]) #annot_id
                data_annot.append(label) # annotation
                batch_annot.append(data_annot)
        # insert by batch
        try:
            if has_annotation:
                dan.insert_record_batch(tname=annot_tname, batch=batch_annot)
        except:
            annot_ids_batch = []
            print(batch_annot)
        annot_ids = annot_ids + annot_ids_batch
    
    print("Inserted {} records.".format(len(annot_ids)))
    
    # partition table
    try:
        nodes = dan.nodes()
        # if has worker nodes
        if len(nodes) > 1:
            nodes = [f'{host}:{port}' for _, host, port, head in nodes if not head]
            if has_annotation:
                dan.table_partition(tname=annot_tname, fid=0, nodes=nodes)
    except Exception as e:
        pass'''
    
    ttype='ROW'
    annot_fields=''
    has_annotation=True
    return gname, ttype, annot_fields, node_list, has_annotation

# api to insert graph prediction
def insert_pred_graph_api(proj_id, node_list, node_info, gname=data_gname, batch_size=1):    
    # create general graph w/o property
    gname = gname + '-' + str(proj_id)
    vfields = dict(vid='bigint', label='int', pred='int')
    try:
        graph = DanGraph(name=gname, vfields=vfields, efields={}, vpfid=0)
        print(graph)
    except Exception as e:
        print(e)
        pass
    
    # insert nodes
    if batch_size is None:
        batch_size = 1
    print('batch size: {}'.format(batch_size))
    total_examples = len(node_list)
    num_of_batch = int(total_examples / batch_size)
    if total_examples > num_of_batch*batch_size:
        num_of_batch += 1

    node_labels = node_info['node_labels']
    node_preds = node_info['node_preds']
    for b in range(num_of_batch):
        node_batch, label_batch, pred_batch = [], [], []
        min_idx = b*batch_size
        max_idx = min(total_examples, (b+1)*batch_size)
        node_batch = node_list[min_idx:max_idx]
        label_batch = node_labels[min_idx:max_idx]
        pred_batch = node_preds[min_idx:max_idx]
        
        graph.insert_vertices(zip(node_batch, label_batch, pred_batch))
    print('Inserted {} nodes'.format(len(node_list)))
    
    ttype='ROW'
    annot_fields=''
    has_annotation=True
    return gname, ttype, annot_fields, node_list, has_annotation    

# insert test prediction for images data
def insert_test_images(proj_id, images_dir, predictions_dir, tname=test_tname, tname2=test_predict_tname, ttype='ROW'):
    # read testing results
    from dataset import read_testing_results as dataset
    data_examples, data_annots = dataset.main(images_dir=images_dir, predictions_dir=predictions_dir, output_dir=None)
    
    return insert_data_images_api(proj_id, data_examples, data_annots, tname, tname2, ttype)

# insert prediction result for images data
def insert_predict_images(proj_id, data_dir, save_files=False, tname=predict_tname, tname2=pred_annot_tname, ttype='ROW'):
    output_dir = None
    if save_files:
        os.path.join('dataset', 'files', tname + '-' + str(proj_id))
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    # read prediction results
    from dataset import read_prediction_results as dataset
    data_examples, data_annots = dataset.main(data_dir, output_dir)
    
    return insert_data_images_api(proj_id, data_examples, data_annots, tname, tname2, ttype)

def select_data_metadata(data_id, tname=data_tname):
    metadata_feature_names = list(data_fields.keys())
    metadata_feature_values = {}
    
    data_info = index_return(tname=tname, search_value=data_id)
    if len(data_info) > 0:
        for i, val in enumerate(data_info[0]):
            metadata_feature_values[metadata_feature_names[i]] = val
    return metadata_feature_values



#### Data Return utils ####

# return by index
def index_return(tname, search_value, fid=0):
    if fid == 0:
        ret = dan.index_search(tname=tname, fid=fid, val=search_value)
        data = []
        if ret:
            data = list(ret)
        return data
    else:
        ret = dan.table_scan(tname=tname)
        data = []
        if ret:
            df = pd.DataFrame(ret)
            data = df.loc[df[fid] == search_value].to_numpy()
        return data


# return by fid
def data_return_by_fid(tname, fid=0):
    ret = dan.table_scan(tname=tname, fids=[fid])
    data = []
    if ret:
        data = [x[0] for x in ret]
    return data

# return by partition
def data_return_partition(tname, partition=-1):
    ret = dan.table_scan(tname=tname, partition=partition)
    data = []
    if ret:
        ret = np.asarray(ret)
        data = list(ret[:,1:]) # 1st field is rowid
    return data


# return by rowids (no partition)
def data_return_by_rows(tname, rowids, ordered=False):
    start_time = time.time()
    ret = dan.table_scan(tname=tname)
    #print('data_return_by_rows table scan: {}'.format(time.time() - start_time))
    df = pd.DataFrame(ret)
    df.set_index([0], inplace=True, drop=False)
    # order rowids
    if ordered:
        rowids = sorted(rowids)        
    data = df.loc[rowids]
    data = data.to_numpy()
    
    return data

# return annotations by rowids (no partition)
def data_return_annotations(tname, rowids, ordered=False):
    start_time = time.time()
    ret = dan.table_scan(tname=tname)
    #print('data_return_annotations table scan: {}'.format(time.time() - start_time))
    df = pd.DataFrame(ret)
    # order rowids
    if ordered:
        rowids = sorted(rowids)
    # get data including rowid
    data = df[df[0].isin(rowids)]
    data = data.to_numpy()
    
    return data

# return annotations by annot_ids (no partition)
def data_return_annotations_2(tname, annot_ids, ordered=False):
    start_time = time.time()
    ret = dan.table_scan(tname=tname)
    #print('data_return_annotations table scan: {}'.format(time.time() - start_time))
    df = pd.DataFrame(ret)
    # order annot_ids
    if ordered:
        annot_ids = sorted(annot_ids)
    # get data including annot_ids
    data = df[df[1].isin(annot_ids)]
    data = data.to_numpy()
    
    return data

# read data images as images and annotations
def get_data_images(proj_id, rowids, annot_ids=None, ordered=True):
    tname = data_tname + '-' + str(proj_id)
    annot_tname = data_annot_tname + '-' + str(proj_id)
    
    # select image data by rowids
    start_time = time.time()
    data = data_return_by_rows(tname, rowids, ordered=ordered)
    #print('data_return_by_rows: {}'.format(time.time() - start_time))
    
    # select annotations by rowids
    start_time = time.time()
    if not annot_ids:
        data_annots = data_return_annotations(annot_tname, rowids, ordered=ordered)
    else:
        data_annots = data_return_annotations_2(annot_tname, annot_ids, ordered=ordered)
    #print('data_return_annotations: {}'.format(time.time() - start_time))
    has_annotation = (data_annots is not None) and (len(data_annots) > 0)
    
    images = []
    annotations = []
    start_time = time.time()
    for i, record in enumerate(data):
        image_id = int(record[0])
        height = int(record[1])
        width = int(record[2])
        imageName = record[3]
        imagePath = record[4]
        image = {'id': image_id, 'height': height, 'width': width, 'name': imageName, 'path': imagePath}
        images.append(image)
    
        if has_annotation:
            annots = data_annots[np.where(data_annots[:,0] == image_id)]
            annot_ids = annots[:,1]
            labels = annots[:,2]
            object_types = annots[:,3]
            annot_values = annots[:,4]
            
            for k in range(len(labels)):
                annot_id = annot_ids[k]
                label = labels[k]
                object_type = object_types[k]
                annotation = {'image_id': image['id'], 'annot_id': annot_id, 'label': label, 'type': object_type}
                annot_value = annot_values[k]
                # bounding box
                if object_type == 'object':
                    xmin = annot_value[0]
                    ymin = annot_value[1]
                    xmax = annot_value[2]
                    ymax = annot_value[3]
                    annotation['bbox'] = [xmin, ymin, xmax, ymax]
                # lines
                elif object_type == 'line':
                    # get x, y points
                    xs = [int(x*width) for x in annot_value[0:int(len(annot_value)/2)]]
                    ys = [int(y*height) for y in annot_value[int(len(annot_value)/2):]]
                    points = [p for p in zip(xs, ys)]
                    annotation['points'] = points
                # add each object to annotations
                annotations.append(annotation)
    #print('process time: {}'.format(time.time() - start_time))
    return {'images': images, 'annotations': annotations}

# read predict images as images and predictions
def get_predict_images(proj_id, rowids, ordered=True):
    tname = predict_tname + '-' + str(proj_id)
    annot_tname = pred_annot_tname + '-' + str(proj_id)
    
    # select image data by rowids
    start_time = time.time()
    data = data_return_by_rows(tname, rowids, ordered=ordered)
    #print('data_return_by_rows: {}'.format(time.time() - start_time))
    
    # select annotations by rowids
    start_time = time.time()
    data_annots = data_return_annotations(annot_tname, rowids, ordered=ordered)
    #print('data_return_annotations: {}'.format(time.time() - start_time))
    has_predict = (data_annots is not None) and (len(data_annots) > 0)
    
    images = []
    annotations = []
    start_time = time.time()
    for i, record in enumerate(data):
        image_id = int(record[0])
        height = int(record[1])
        width = int(record[2])
        imageName = record[3]
        imagePath = record[4]
        image_timestamp = ''
        if len(record) > 6:
            image_timestamp = datetime.strptime(record[6], '%Y%m%d_%H%M%S_%f')
        inference_fps = 0
        if len(record) > 7:
            inference_fps = record[7]
        image = {'id': image_id, 'height': height, 'width': width, 'name': imageName, 'path': imagePath, 'timestamp': image_timestamp, 'fps': inference_fps}
        images.append(image)
    
        if has_predict:
            annots = data_annots[np.where(data_annots[:,0] == image_id)]
            annot_ids = annots[:,1]
            labels = annots[:,2]
            object_types = annots[:,3]
            predict_scores = annots[:,4]
            annot_values = annots[:,5]
            
            for k in range(len(labels)):
                annot_id = annot_ids[k]
                label = labels[k]
                object_type = object_types[k]
                score = predict_scores[k]
                annotation = {'image_id': image['id'], 'annot_id': annot_id, 'label': label, 'type': object_type, 'score': score}
                annot_value = annot_values[k]
                # bounding box
                if object_type == 'object':
                    xmin = annot_value[0]
                    ymin = annot_value[1]
                    xmax = annot_value[2]
                    ymax = annot_value[3]
                    annotation['bbox'] = [xmin, ymin, xmax, ymax]
                # add each object to annotations
                annotations.append(annotation)
    #print('process time: {}'.format(time.time() - start_time))
    return {'images': images, 'annotations': annotations}

# read testing images as images and predictions
def get_test_images(proj_id, rowids, ordered=True):
    tname = test_tname + '-' + str(proj_id)
    annot_tname = test_predict_tname + '-' + str(proj_id)
    
    # select image data by rowids
    start_time = time.time()
    data = data_return_by_rows(tname, rowids, ordered=ordered)
    #print('data_return_by_rows: {}'.format(time.time() - start_time))
    
    # select annotations by rowids
    start_time = time.time()
    data_annots = data_return_annotations(annot_tname, rowids, ordered=ordered)
    #print('data_return_annotations: {}'.format(time.time() - start_time))
    has_predict = (data_annots is not None) and (len(data_annots) > 0)
    
    images = []
    annotations = []
    start_time = time.time()
    for i, record in enumerate(data):
        image_id = int(record[0])
        height = int(record[1])
        width = int(record[2])
        imageName = record[3]
        imagePath = record[4]
        image_timestamp = ''
        if len(record) > 6:
            image_timestamp = datetime.strptime(record[6], '%Y%m%d_%H%M%S_%f')
        inference_fps = 0
        if len(record) >= 7:
            inference_fps = record[7]
        image = {'id': image_id, 'height': height, 'width': width, 'name': imageName, 'path': imagePath, 'timestamp': image_timestamp, 'fps': inference_fps}
        images.append(image)
    
        if has_predict:
            annots = data_annots[np.where(data_annots[:,0] == image_id)]
            annot_ids = annots[:,1]
            labels = annots[:,2]
            object_types = annots[:,3]
            predict_scores = annots[:,4]
            annot_values = annots[:,5]
            
            for k in range(len(labels)):
                annot_id = annot_ids[k]
                label = labels[k]
                object_type = object_types[k]
                score = predict_scores[k]
                annotation = {'image_id': image['id'], 'annot_id': annot_id, 'label': label, 'type': object_type, 'score': score}
                annot_value = annot_values[k]
                # bounding box
                if object_type == 'object':
                    xmin = annot_value[0]
                    ymin = annot_value[1]
                    xmax = annot_value[2]
                    ymax = annot_value[3]
                    annotation['bbox'] = [xmin, ymin, xmax, ymax]
                # add each object to annotations
                annotations.append(annotation)
    #print('process time: {}'.format(time.time() - start_time))
    return {'images': images, 'annotations': annotations}


def get_data_graph(proj_id, gname, data_points):
    # get graph info
    graph_info = dan.get_graph(gname)
    print(graph_info)
    
    start_time = time.time()
    # get list vertex (dict={vid: (vpid, [vid, label, year, features])})
    vertex_list = dan.get_vertex_list(gname=gname)
    # get list edge
    srcs, dsts = dan.get_edge_list(gname=gname)
    print('get graph data time: {}s'.format(int(time.time() - start_time)))
    
    # sort by label
    sorted_vertex_list = {}
    for key in sorted(vertex_list):
        sorted_vertex_list[key] = vertex_list[key]
    
    # get node content from vertex list
    start_time = time.time()
    node_ids = []
    labels = []
    years = []
    feats = []
    for key, value in sorted_vertex_list.items():
        node_content = value[1]
        node_ids.append(node_content[0])
        labels.append(node_content[1])
        if len(node_content) > 2:
            years.append(node_content[2])
        if len(node_content) > 3:
            feats.append(node_content[3])
    nodes = {'node_id': node_ids, 'label': labels, 'year': years, 'feat': feats}
    
    # create annotations
    annotations = {}
    for key, value in sorted_vertex_list.items():
        node_content = value[1]
        node_id = key
        annotations[node_id] = {'label': node_content[1]}
    
    print('process time: {}s'.format(int(time.time() - start_time)))
    del vertex_list
    return nodes, srcs, dsts, annotations

# get graph prediction data
def get_prediction_graph(proj_id, gname, data_points):
    # get graph info
    graph_info = dan.get_graph(gname)
    print(graph_info)
    
    start_time = time.time()
    # get list vertex (dict={vid: (vpid, [vid, label, year, features])})
    vertex_list = dan.get_vertex_list(gname=gname)
    # get list edge
    srcs, dsts = dan.get_edge_list(gname=gname)
    print('get graph data time: {}s'.format(int(time.time() - start_time)))
    
    # sort by label
    sorted_vertex_list = {}
    for key in sorted(vertex_list):
        sorted_vertex_list[key] = vertex_list[key]
    
    start_time = time.time()
    # create annotations
    annotations = {}
    for key, value in sorted_vertex_list.items():
        node_content = value[1]
        node_id = key
        annotations[node_id] = {'label': node_content[1], 'pred': node_content[2]}
    
    print('process time: {}s'.format(int(time.time() - start_time)))
    del vertex_list
    return annotations
    
### Data View utils ####



#### Model utils ####
def create_model_metadata_table(tname=model_tname, ttype='ROW'):
    # create a row-table
    fields = model_fields
    dan.create_table(tname=tname, ttype=ttype, **fields)
    dan.create_index(tname=tname, fid=0, unique=True)
    
    return tname, fields

def insert_model_metadata(proj_id, model_version, model_metadata, tname=model_tname, ttype='ROW'):
    # add data
    model_id = dan.get_unique_id()
    model_time = int(time.time())
    data = [[model_id, proj_id, model_time, model_version, 
            model_metadata['model_name'], 
            model_metadata['model_type'],
            model_metadata['model_framework'],
            model_metadata['model_input'],
            model_metadata['model_classes'] ]]
    for d in data:
        dan.insert_record(tname=tname, values=d)
    
    # view inserted rows
    ret = index_return(tname, model_id, 0)
    for i, record in enumerate(ret):
        print(record)
    
    return model_id

def select_model_metadata(model_id, tname=model_tname):
    metadata_feature_names = list(model_fields.keys())
    metadata_feature_values = {}
    
    metadata_info = index_return(tname=tname, search_value=model_id)
    if len(metadata_info) > 0:
        for i, val in enumerate(metadata_info[0]):
            metadata_feature_values[metadata_feature_names[i]] = val
    return metadata_feature_values


#### Training utils ####
def create_training_metadata_table(tname=training_tname, ttype='ROW'):
    # create a row-table
    fields = training_fields
    dan.create_table(tname=tname, ttype=ttype, **fields)
    dan.create_index(tname=tname, fid=0, unique=True)
    
    return tname, fields

def insert_training_metadata(proj_id, training_version, training_metadata, tname=training_tname, ttype='ROW'):
    # add data
    training_id = dan.get_unique_id()
    timestamp = int(time.time())
    data = [[training_id, proj_id, timestamp, training_version, 
            training_metadata['framework'], 
            training_metadata['distributed_training'],
            training_metadata['number_of_gpus'],
            training_metadata['number_of_nodes'] ]]
    for d in data:
        dan.insert_record(tname=tname, values=d)
    
    return training_id


def insert_training_log(training_id, training_history, batch_size=None, tname=training_tname, ttype='ROW'):  
    # make the fields for the table
    fields = {}
    # row id (auto increment)
    fields['rid'] = 'bigint'
    # get fields information
    for k, v in training_history[0].items():
        fields[k] = v.type
    #print(fields)
    
    # create a table to store data (for each training)
    tname = tname + '-' + str(training_id)
    try:
        dan.create_table(tname=tname, ttype=ttype, **fields)
        dan.create_index(tname=tname, fid=0, unique=True)
    except Exception as e:
        print(e)
        pass
    
    # incrementor function for rid
    def incrementor(start_count=0):
        info = {"count": start_count}
        def number():
            info["count"] += 1
            return info["count"]
        return number
    # find actual start_count from current rowids
    try:
        rowids = data_return_by_fid(tname, fid=0)
        start_count = max(rowids)
    except:
        start_count = 0
    rid = incrementor(start_count=start_count)
    
    # insert data
    rids = []
    if batch_size is None:
        batch_size = 1
    total_data = len(training_history)
    num_of_batch = int(total_data / batch_size)
    if total_data > num_of_batch*batch_size:
        num_of_batch += 1
    for b in range(num_of_batch):
        batch = []
        for i in range(b*batch_size, min(total_data, (b+1)*batch_size)):
            his = training_history[i]
            data_with_id = []
            data_with_id.append(rid())
            rids.append(data_with_id[0])
            for k, v in his.items():
                data_with_id.append(v.value)
            batch.append(data_with_id)
        # insert by batch
        dan.insert_record_batch(tname=tname, batch=batch)
    
    print("Inserted {} records.".format(len(rids)))
    
    return tname, fields, rids

def select_training_metadata(training_id, tname=training_tname):
    metadata_feature_names = list(training_fields.keys())
    metadata_feature_values = {}
    
    metadata_info = index_return(tname=tname, search_value=training_id)
    if len(metadata_info) > 0:
        for i, val in enumerate(metadata_info[0]):
            metadata_feature_values[metadata_feature_names[i]] = val
    return metadata_feature_values


#### Inference utils ####
def create_inference_metadata_table(tname=inference_tname, ttype='ROW'):
    # create a row-table
    fields = inference_fields
    dan.create_table(tname=tname, ttype=ttype, **fields)
    dan.create_index(tname=tname, fid=0, unique=True)
    
    return tname, fields

def insert_inference_metadata(proj_id, inference_version, inference_metadata, tname=inference_tname, ttype='ROW'):
    # add data
    inference_id = dan.get_unique_id()
    timestamp = int(time.time())
    data = [[inference_id, proj_id, timestamp, inference_version, 
            inference_metadata['device'], inference_metadata['optimization'], inference_metadata['precision'] ]]
    for d in data:
        dan.insert_record(tname=tname, values=d)
    
    # view inserted rows
    ret = index_return(tname, inference_id, 0)
    for i, record in enumerate(ret):
        print(record)
    
    return inference_id

def select_inference_metadata(inference_id, tname=inference_tname):
    metadata_feature_names = list(inference_fields.keys())
    metadata_feature_values = {}
    
    metadata_info = index_return(tname=tname, search_value=inference_id)
    if len(metadata_info) > 0:
        for i, val in enumerate(metadata_info[0]):
            metadata_feature_values[metadata_feature_names[i]] = val
    return metadata_feature_values


#### Profiling utils ####
def create_profiling_table(tname=profiling_tname, ttype='ROW'):
    # create table profiling
    fields = profiling_fields
    dan.create_table(tname=tname, ttype=ttype, **fields)
    dan.create_index(tname=tname, fid=0, unique=True)
    
    # create table profiling_task
    fields = profiling_task_fields
    tname = profiling_task_tname
    dan.create_table(tname=tname, ttype=ttype, **fields)
    dan.create_index(tname=tname, fid=0, unique=True)
    
    return tname, fields
    
def insert_profiling(proj_id, pipeline, tname=profiling_tname):
    # add data
    lc_id = dan.get_unique_id()
    time_start = int(time.time())
    
    data = [[proj_id, lc_id, pipeline, time_start]]
    for d in data:
        dan.insert_record(tname=tname, values=d)
    
    return lc_id

def insert_profiling_task(lc_id, profiling, tname=profiling_task_tname):
    # prepare data
    task_id = dan.get_unique_id()
    data = [[task_id, lc_id, profiling["task_name"], profiling["time_start"], profiling["duration"], profiling["data_stat"], profiling["mem_stat"], profiling["io_stat"] ]]
    
    for d in data:
        dan.insert_record(tname=tname, values=d)
    
    return task_id

def select_profiling(proj_id, lc_pipeline, tname=profiling_tname):
    lc_id = None
    ret = dan.table_scan(tname=tname)
    if ret:
        for r in ret:
            if proj_id == r[0] and lc_pipeline == r[2]:
                lc_id = r[1]
    return lc_id

def select_profiling_task(lc_id, tname=profiling_task_tname):
    ret = []
    data = index_return(tname=tname, search_value=lc_id, fid=1)
    for d in data:
        profiling = {}
        profiling["task_name"] = d[2]
        profiling["time_start"] = d[3]
        profiling["duration"] = d[4]
        profiling["data_stat"] = d[5]
        profiling["mem_stat"] = d[6]
        profiling["io_stat"] = d[7]
        ret.append(profiling)
    
    return ret

