# project constants
project_tname='Project'
project_fields={
    'proj_id': 'bigint', 
    'proj_code': 'varchar', 
    'proj_name': 'varchar', 
    'proj_timestamp': 'bigint', 
    'proj_owner': 'varchar'
}

# data constansts
data_tname='Data' # store the row data (e.g. image, record,...) for each version
data_gname='GData' # graph data name
data_annot_tname='Annot' # store the annotation of each data row
data_fields = {
    'data_id': 'bigint', 
    'proj_id': 'bigint',
    'data_type': 'varchar', # CSV, Images, Text,...
    'data_timestamp': 'bigint',
    'data_version': 'varchar',
    'data_source': 'varchar' # data path
}
data_type_csv='CSV'
data_type_image='Images'
data_type_graph='Graph'

# predict constants
predict_tname='Predict'
pred_annot_tname='PredictAnnot'

# test constants
test_tname='Test'
test_predict_tname='TestPredict'

# model constants
model_tname='Model'
model_fields = {
    'model_id': 'bigint', 
    'proj_id': 'bigint',
    'timestamp': 'bigint',
    'model_version': 'varchar',
    'model_name': 'varchar',
    'model_type': 'varchar', # classification, detection, ...
    'model_framework': 'varchar', # tensorflow1, tensorflow2, pytorch0.9, onnx,...
    'model_input': 'varchar', # input shape (width, height)
    'model_classes': 'varchar', # list of classes
}

# training constants
training_tname='Training'
training_fields = {
    'training_id': 'bigint', 
    'proj_id': 'bigint',
    'timestamp': 'bigint',
    'training_version': 'varchar',
    'framework': 'varchar', # training framework
    'distributed_training': 'varchar', # None or name of distributed system
    'number_of_gpus': 'bigint',
    'number_of_nodes': 'bigint'
}

# inference constants
inference_tname='Inference'
inference_fields = {
    'inference_id': 'bigint', 
    'proj_id': 'bigint',
    'timestamp': 'bigint',
    'inference_version': 'varchar',
    'device': 'varchar', # deploy to devices
    'optimization': 'varchar', # quantization methods (QAT, PTQ,...)
    'precision': 'varchar' # FLOAT32, FLOAT16, INT8
}

# profiling constants
profiling_tname='Profiling'
profiling_fields = {
    'proj_id': 'bigint',
    'lc_id': 'bigint',
    'lc_pipeline': 'varchar', # life-cycle pipeline
    'time_start': 'bigint'
}
profiling_task_tname='ProfilingTask'
profiling_task_fields = {
    'task_id': 'bigint',
    'lc_id': 'bigint',
    'task_name': 'varchar',
    'time_start': 'varchar',
    'duration': 'varchar',
    'data_stat': 'varchar',
    'mem_stat': 'varchar',
    'io_stat': 'varchar'
}

