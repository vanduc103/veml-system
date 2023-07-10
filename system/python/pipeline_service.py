import dan
import sys, os, time, ast
from datetime import datetime

import data_manager_utils
from version_utils import Versioning

local_head_addr = f'{dan.get_local_ip()}:'

class PipelineService():
    """API for ML pipeline service"""

    def __init__(self, proj_name, dan_port, head_addr=local_head_addr):
        # connect dan to cluster
        dan.connect(head_addr=head_addr+str(dan_port), launch=False)
        # get proj_id
        proj_id = data_manager_utils.select_proj_by_name(proj_name)
        if not proj_id:
            print('Project {} does not exist!'.format(proj_name))
            self.proj_id = None
        self.proj_name = proj_name
        self.dan_port = dan_port
        self.proj_id = proj_id
        self.versioning = Versioning()

    def data_version_list(self, data_metadata=None):
        if not data_metadata:
            results = self.select_all_data_version()
        else:
            # search data version by data metadata
            data_label = data_metadata.get('data_label', '')
            if data_label:
                results = self.select_data_version_by_metadata(data_label)
            else:
                results = self.select_all_data_version()
        #print(results)
        return results

    # select all data versions
    def select_all_data_version(self):
        driver = self.versioning.driver
        data = []
        with driver.session() as session:
            try:
                data = session.read_transaction(self._select_all_data_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data
    
    def _select_all_data_version(self, tx):
        # select data version with data labels only
        #query = "MATCH (n:Data_Version) " \
        #"OPTIONAL MATCH (n:Data_Version)-[:HAS_LABEL]->(d:Data_Label) " \
        query = "MATCH (n:Data_Version)-[:HAS_LABEL]->(d:Data_Label) " \
                "RETURN n.version AS version, n.target AS target, d.name AS label_name"
        res = tx.run(query)
        data = []
        for record in res:
            data.append({"version": record["version"], "target": record["target"], "label": record["label_name"]})
        return data
    
    def select_data_version_by_metadata(self, label_name):
        driver = self.versioning.driver
        results = []
        with driver.session() as session:
            try:
                results = session.read_transaction(self._select_data_version_by_metadata, label_name)
            except Exception as e:
                print("[ERROR] ", e)
        self.versioning.close()
        #print(results)
        return results
    
    # select data version by metadata
    def _select_data_version_by_metadata(self, tx, label_name):
        query = "MATCH (v1:Data_Version)-[:HAS_LABEL]->(v2:Data_Label {name:$label_name}) " \
                "RETURN v1.version AS data_version, v2.name AS data_label"
        res = tx.run(query, label_name=label_name)
        data = []
        for record in res:
            data.append({
                         "version":record["data_version"],
                         "label":record["data_label"]
                       })
        return data
    
    def data_metadata_list(self):
        driver = self.versioning.driver
        results = []
        with driver.session() as session:
            try:
                results = session.read_transaction(self._select_data_metadata)
            except Exception as e:
                print("[ERROR] ", e)
        self.versioning.close()
        #print(results)
        return results
    
    # select data metadata
    def _select_data_metadata(self, tx):
        query = "MATCH (n:Data_Label) " \
                "RETURN n.name AS label_name"
        res = tx.run(query)
        data = []
        for record in res:
            data.append({
                         "label_name":record["label_name"]
                       })
        return data

    ###############################
    def model_version_list(self, model_metadata=None):
        if not model_metadata:
            results = self.versioning.select_all_model_version(self.proj_id)
        else:
            # search model version by metadata
            results = []
            architecture_name = model_metadata.get('architecture_name', '')
            backbone_name = model_metadata.get('backbone_name', '')
            neck_name = model_metadata.get('neck_name', '')
            results = self.select_model_version_by_metadata(architecture_name, backbone_name, neck_name)
            if len(results) == 0:
                results = self.versioning.select_all_model_version(self.proj_id)
        #print(results)
        return results

    # select all model versions
    def select_all_model_version(self):
        driver = self.versioning.driver
        data = []
        with driver.session() as session:
            try:
                data = session.read_transaction(self._select_all_model_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data
    
    def _select_all_model_version(self, tx):
        query = "MATCH (n:Model_Version)-[:USES_ARCHITECTURE]->(v1:Model_Architecture) " \
                "MATCH (n)-[:HAS_BACKBONE]->(v2:Model_Backbone) " \
                "MATCH (n)-[:HAS_NECK]->(v3:Model_Neck) " \
                "RETURN n.version AS version, n.name AS name, " \
                "v1.name AS architecture_name, v2.name AS backbone_name, v3.name AS neck_name"
        res = tx.run(query)
        data = []
        for record in res:
            data.append({
                        "version":record["version"],
                        "name":record["name"],
                        "architecture_name":record["architecture_name"],
                        "backbone_name":record["backbone_name"],
                        "neck_name":record["neck_name"]
                        })
        return data
    
    # select model by metadata
    def select_model_version_by_metadata(self, architecture_name, backbone_name, neck_name):
        driver = self.versioning.driver
        results = []
        with driver.session() as session:
            try:
                if architecture_name:
                    results += session.read_transaction(self._select_model_version_by_metadata1, architecture_name)
                if backbone_name:
                    results += session.read_transaction(self._select_model_version_by_metadata2, backbone_name)
                if neck_name:
                    results += session.read_transaction(self._select_model_version_by_metadata3, neck_name)
            except Exception as e:
                print("[ERROR] ", e)
        self.versioning.close()
        #print(results)
        return results
    
    # select model version by metadata
    def _select_model_version_by_metadata1(self, tx, architecture_name):
        query = "MATCH (n:Model_Version)-[:USES_ARCHITECTURE]->(v1:Model_Architecture {name:$architecture_name}) " \
                "MATCH (n)-[:HAS_BACKBONE]->(v2:Model_Backbone) " \
                "MATCH (n)-[:HAS_NECK]->(v3:Model_Neck) " \
                "RETURN n.version AS version, n.name AS name, " \
                "v1.name AS architecture_name, v2.name AS backbone_name, v3.name AS neck_name"
        res = tx.run(query, architecture_name=architecture_name)
        data = []
        for record in res:
            data.append({
                        "version":record["version"],
                        "name":record["name"],
                        "architecture_name":record["architecture_name"],
                        "backbone_name":record["backbone_name"],
                        "neck_name":record["neck_name"]
                       })
        return data
    def _select_model_version_by_metadata2(self, tx, backbone_name):
        query = "MATCH (n:Model_Version)-[:HAS_BACKBONE]->(v2:Model_Backbone {name:$backbone_name}) " \
                "MATCH (n)-[:USES_ARCHITECTURE]->(v1:Model_Architecture) " \
                "MATCH (n)-[:HAS_NECK]->(v3:Model_Neck) " \
                "RETURN n.version AS version, n.name AS name, " \
                "v1.name AS architecture_name, v2.name AS backbone_name, v3.name AS neck_name"
        res = tx.run(query, backbone_name=backbone_name)
        data = []
        for record in res:
            data.append({
                        "version":record["version"],
                        "name":record["name"],
                        "architecture_name":record["architecture_name"],
                        "backbone_name":record["backbone_name"],
                        "neck_name":record["neck_name"]
                       })
        return data
    def _select_model_version_by_metadata3(self, tx, neck_name):
        query = "MATCH (n:Model_Version)-[:HAS_NECK]->(v3:Model_Neck {name:$neck_name}) " \
                "MATCH (n)-[:USES_ARCHITECTURE]->(v1:Model_Architecture) " \
                "MATCH (n)-[:HAS_BACKBONE]->(v2:Model_Backbone) " \
                "RETURN n.version AS version, n.name AS name, " \
                "v1.name AS architecture_name, v2.name AS backbone_name, v3.name AS neck_name"
        res = tx.run(query, neck_name=neck_name)
        data = []
        for record in res:
            data.append({
                        "version":record["version"],
                        "name":record["name"],
                        "architecture_name":record["architecture_name"],
                        "backbone_name":record["backbone_name"],
                        "neck_name":record["neck_name"]
                       })
        return data
    
    def model_metadata_list(self):
        driver = self.versioning.driver
        results = []
        with driver.session() as session:
            try:
                results = session.read_transaction(self._select_model_metadata)
            except Exception as e:
                print("[ERROR] ", e)
        self.versioning.close()
        #print(results)
        return results
    
    # select model metadata
    def _select_model_metadata(self, tx):
        '''query = "MATCH (n1:Model_Architecture) " \
                "MATCH (n2:Model_Backbone) " \
                "MATCH (n3:Model_Neck) " \
                "RETURN n1.name AS architecture_name, n2.name AS backbone_name, n3.name AS neck_name"'''
        query = "MATCH (n:Model_Name) " \
                "RETURN n.name AS name"
        res = tx.run(query)
        data = []
        for record in res:
            model_name = record["name"]
            model_name = model_name.split("_")
            data.append({
                         "name": record["name"],
                         "architecture_name": model_name[0],
                         "backbone_name": model_name[1],
                         "neck_name": model_name[2]
                       })
        return data
    
    #########################
    def training_version_list(self, training_plan):
        if training_plan:
            results = self.versioning.select_training_version_by_plan(self.proj_id, training_plan)
        else:
            results = self.versioning.select_all_training_version(self.proj_id)
        #print(results)
        return results

    def training_plan_list(self):
        driver = self.versioning.driver
        results = []
        with driver.session() as session:
            try:
                results = session.read_transaction(self._select_training_plan)
            except Exception as e:
                print("[ERROR] ", e)
        self.versioning.close()
        #print(results)
        return results
    
    # select training plan
    def _select_training_plan(self, tx):
        query = "MATCH (n:Training_Plan) " \
                "RETURN n.training_plan AS training_plan"
        res = tx.run(query)
        data = []
        for record in res:
            training_plan = record["training_plan"]
            learning_rate = training_plan.split("epochs")[0].split("lr")[1]
            epochs = training_plan.split("epochs")[1].split("bsize")[0]
            batch_size = training_plan.split("epochs")[1].split("bsize")[1]
            data.append({
                         "training_plan": training_plan,
                         "learning_rate": learning_rate,
                         "epochs": epochs,
                         "batch_size": batch_size,
                       })
        return data
    
    ##########################3
    def inference_version_list(self, inference_config):
        if inference_config:
            results = self.versioning.inference_config_list(self.proj_id, inference_config)
        else:
            results = self.versioning.select_all_inference_version(self.proj_id)
        for res in results:
            res["test_data_version"] = res["inference_config"].split("_")[1]
        #print(results)
        return results
    
    def inference_config_list(self):
        driver = self.versioning.driver
        results = []
        with driver.session() as session:
            try:
                results = session.read_transaction(self._select_inference_config)
            except Exception as e:
                print("[ERROR] ", e)
        self.versioning.close()
        #print(results)
        return results
    
    # select inference config
    def _select_inference_config(self, tx):
        query = "MATCH (n:Inference_Config) " \
                "RETURN n.inference_config AS inference_config"
        res = tx.run(query)
        data = []
        for record in res:
            inference_config = record["inference_config"]
            inference_device = inference_config.split("_")[0]
            test_data_version = inference_config.split("_")[1]
            data.append({
                         "inference_config": inference_config,
                         "device": inference_device,
                         "test_data_version": test_data_version,
                       })
        return data
    
    ###############################
    def build_pipeline(self, args):
        print(args)
        dan_port = args.dan_port
        data_version = args['data_version']
        pipeline_config = args['pipeline_config']
        
        model_version = args['model_version']
        model_name = args['model_name']
        
        pre_training_version = args['pre_training_version']
        training_version = args['training_version']
        training_plan = args['training_plan']
        
        inference_version = args['inference_version']
        inference_config = args['inference_config']
        inference_device = args['inference_device']
        test_data_version = args['test_data_version']
        
        # run script
        working_directory = os.path.join('/home/duclv/workspace/project/dan/python/example/delc', 'research/bdd100k-models/det/')
        args = ['mlflow', 'run', '.', '--env-manager=local']
        args.append('-Poption=prepare')
        args.append('-Pdan_port={}'.format(dan_port))
        args.append('-Pconfig=configs/katech/{}'.format(pipeline_config))
        args.append('-Pwork_dir=../training_log/demo')
        
        args.append('-Pdata_version={}'.format(data_version))
        args.append('-Pmodel_version={}'.format(model_version))
        args.append('-Ppre_training_version={}'.format(pre_training_version))
        args.append('-Ptraining_version={}'.format(training_version))
        args.append('-Pinference_version={}'.format(inference_version))
        args.append('-Ptest_data_version={}'.format(test_data_version))
        print(args)
        
        print(working_directory)
        import subprocess
        p = subprocess.Popen(args, cwd=working_directory)
        p.wait()
        
    # build pipeline for incremental learning
    def build_pipeline_inc_learning(self, args):
        learning_method = args['learning']
        print(args)
        # full training
        if learning_method == 'full':
            data_version = args['data_version']
            # get list learning data versions
            learning_data_versions = args['learning_data_versions']
            if learning_data_versions:
                # merge data versions
                merge_versions = []
                merge_versions.append(data_version)
                merge_versions += learning_data_versions
                merge_versions = ','.join(merge_versions)
                # data manager to merge data version
                from data_manager_images import DataManagerImages
                data_manager = DataManagerImages(self.proj_name, self.dan_port)
                full_data_version = 'merge{}'.format(data_version)
                data_manager.addmerge_data_version(data_version=full_data_version, data_target='train', merge_versions=merge_versions)
                # update to the pipeline
                args['data_version'] = full_data_version
                args['test_data_version'] = full_data_version
            else:
                learning_data_versions = []
            
        elif learning_method == 'transfer':
            data_version = args['data_version']
            # get list learning data versions
            learning_data_versions = args['learning_data_versions']
            # get pre training version
            if learning_data_versions:
                pre_data_version = learning_data_versions[-1]
                from analyzer_lifecycle import AnalyzerLifecycle
                analyzer = AnalyzerLifecycle(self.proj_name, self.dan_port)
                res = analyzer.select_lifecycle_from_data_version(pre_data_version)
                training_versions = res.get("training_versions", [])
                if training_versions:
                    args['pre_training_version'] = training_versions[-1]['training_version']
                    
        elif learning_method == 'drift':
            # todo:
            data_version = args['data_version']
        elif learning_method == 'active':
            data_version = args['data_version']
            
        # call ML pipeline construction
        self.build_pipeline(args)
        
        
    ###########################
    # search pipeline
    def pipeline_list(self, args):
        # get arguments
        data_version = args['data_version']
        data_label = args['data_label']
        data_versions = args['data_versions']
        import ast
        if data_versions:
            data_versions = ast.literal_eval(data_versions)
            data_versions = [n.strip() for n in data_versions]
        
        model_version = args['model_version']
        model_name = args['model_name']
        
        training_version = args['training_version']
        training_plan = args['training_plan']
        
        inference_config = args['inference_config']
        inference_version = args['inference_version']
        #inference_device = args['inference_device']
        #test_data_version = args['test_data_version']
        
        # get list of select data version from data metadata
        select_data_versions = []
        if data_versions:
            for data_version in data_versions:
                select_data_versions.append(data_version)
        else:
            res = self.data_version_list({'data_label': data_label})
            for r in res:
                select_data_versions.append(r["version"])
        #print(select_data_versions)
        
        # get list of select model version from model metadata
        select_model_versions = []
        res = self.model_version_list(args)
        for r in res:
            select_model_versions.append(r["version"])
        #print(select_model_versions)
        
        from analyzer_lifecycle import AnalyzerLifecycle
        analyzer = AnalyzerLifecycle(self.proj_name, self.dan_port)
        results = []
        for data_version in select_data_versions:
            res = analyzer.select_lifecycle_from_data_version(data_version)
            training_versions = res.get("training_versions", [])
            model_versions = res.get("model_versions", [])
            for _, (t, m) in enumerate(zip(training_versions, model_versions)):
                inference_versions = t.get('inference_versions', [])
                for infer in inference_versions:
                    result = {'data_version': data_version}
                    if training_plan and training_plan != t['training_plan']:
                        continue
                    else:
                        result['training_version'] = t['training_version']
                        result['training_plan'] = t['training_plan']
                        result["batch_size"] = t["batch_size"]
                        result["epochs"] = t["epochs"]
                        result["learning_rate"] = t["learning_rate"]
                    if select_model_versions and m["model_version"] not in select_model_versions:
                        continue
                    else:
                        result['model_version'] = m["model_version"]
                        result['model_name'] = m["model_name"]
                    if inference_config and infer['inference_config'] != inference_config:
                        continue
                    else:
                        result['inference_version'] = infer['inference_version']
                        result['inference_config'] = infer['inference_config']
                        result['testing_accuracy'] = infer['testing_accuracy']
                        result['inference_speed'] = infer['inference_speed']
                    results.append(result)
        #print(results[0])
        return results

    ######################################
    # create pipeline graph
    def pipeline_graph(self, args):
        indices = []
        labels = []
        classes = []
        edges = []
        eprobs = []
        # get arguments
        arg_inference_version = args['inference_version']
        if (arg_inference_version):
            # get pipeline from inference version
            driver = self.versioning.driver
            results = []
            with driver.session() as session:
                try:
                    results = session.read_transaction(self._select_pipeline_from_inference, arg_inference_version)
                except Exception as e:
                    print("[ERROR] ", e)
            self.versioning.close()
            if len(results) == 0:
                return [{'indices': [], 'labels': [], 'classes': [], 'edges': [], 'eprobs': []}]
            result = results[0]
            # get pipeline information
            data_version = result['data_version']
            model_version = 'M'+result['model_version']
            training_version = 'T'+result['training_version']
            inference_version = 'I'+result['inference_version']
            #
            indices = [data_version, model_version, training_version, inference_version]
            labels = ['Dataset: '+data_version, 'Model: '+model_version, 'Training: '+training_version, 'Inference: '+inference_version]
            classes = ['dataset', 'model', 'training', 'inference']
            edges = [[data_version, training_version], [model_version, training_version], [training_version, inference_version]]
            eprobs = ['DATA_TRAINING', 'MODEL_TRAINING', 'DEPLOY']
            return [{'indices': indices, 'labels': labels, 'classes': classes, 'edges': edges, 'eprobs': eprobs}]
        
        # get arguments
        arg_data_version = args['data_version']
        if (arg_data_version):
            driver = self.versioning.driver
            results = []
            with driver.session() as session:
                try:
                    # get filter data
                    results = session.read_transaction(self._pipeline_select_filter_data, arg_data_version)
                    if not results:
                        # get merge data
                        results = session.read_transaction(self._pipeline_select_merge_data, arg_data_version)
                except Exception as e:
                    print("[ERROR] ", e)
            self.versioning.close()
            for res in results:
                pre_data_version = res['data_version']
                indices.append(pre_data_version)
                labels.append('Dataset: '+pre_data_version)
                classes.append('dataset')
                edges.append([pre_data_version, arg_data_version])
                eprobs.append(res['edge_name'])
        
            return [{'indices': indices, 'labels': labels, 'classes': classes, 'edges': edges, 'eprobs': eprobs}]
        
        # get arguments
        arg_training_version = args['training_version']
        if (arg_training_version):
            driver = self.versioning.driver
            results = []
            with driver.session() as session:
                try:
                    # get transfer training version
                    results = session.read_transaction(self._pipeline_select_transfer_training, arg_training_version)
                except Exception as e:
                    print("[ERROR] ", e)
            self.versioning.close()
            for res in results:
                pre_training_version = res['training_version']
                indices.append(pre_training_version)
                labels.append('Training: '+pre_training_version)
                classes.append('training')
                edges.append([pre_training_version, arg_training_version])
                eprobs.append(res['edge_name'])
        
            return [{'indices': indices, 'labels': labels, 'classes': classes, 'edges': edges, 'eprobs': eprobs}]

        return [{'indices': indices, 'labels': labels, 'classes': classes, 'edges': edges, 'eprobs': eprobs}]
    
    # select pipeline graph
    def _select_pipeline_from_inference(self, tx, inference_version):
        query = "MATCH (t:Training_Version)-[:DEPLOY]->(i:Inference_Version {version: $inference_version}) " \
                "MATCH (m:Model_Version)-[:MODEL_TRAINING]->(t) " \
                "MATCH (d:Data_Version)-[:DATA_TRAINING]->(t) " \
                "RETURN d.version AS data_version, m.version AS model_version, " \
                        "t.version AS training_version, i.version AS inference_version"
        res = tx.run(query, inference_version=inference_version)
        data = []
        for record in res:
            data.append({
                         "data_version":record["data_version"],
                         "model_version":record["model_version"],
                         "training_version":record["training_version"],
                         "inference_version":record["inference_version"]
                       })
        return data
    
    # select filter data
    def _pipeline_select_filter_data(self, tx, data_version):
        query = "MATCH (d1:Data_Version)-[:FILTER_DATA]->(d2:Data_Version {version: $data_version}) " \
                "RETURN d1.version AS data_version "
        res = tx.run(query, data_version=data_version)
        data = []
        for record in res:
            data.append({
                         "data_version":record["data_version"],
                         "edge_name": "FILTER_DATA",
                       })
        return data
    
    # select merge data
    def _pipeline_select_merge_data(self, tx, data_version):
        query = "MATCH (d1:Data_Version)-[:MERGE_DATA]->(d2:Data_Version {version: $data_version}) " \
                "RETURN d1.version AS data_version "
        res = tx.run(query, data_version=data_version)
        data = []
        for record in res:
            data.append({
                         "data_version":record["data_version"],
                         "edge_name": "MERGE_DATA",
                       })
        return data
    
    # select transfer training
    def _pipeline_select_transfer_training(self, tx, training_version):
        if 'T' in training_version:
            training_version = training_version[1:]
        query = "MATCH (d1:Training_Version)-[:TRAINING_TRANSFER]->(d2:Training_Version {version: $training_version}) " \
                "RETURN d1.version AS training_version "
        res = tx.run(query, training_version=training_version)
        data = []
        for record in res:
            data.append({
                         "training_version":record["training_version"],
                         "edge_name": "TRAINING_TRANSFER",
                       })
        return data
    
if __name__ == "__main__":
    proj_name = 'Self Driving Project'
    dan_port = '42157'
    s = PipelineService(proj_name, dan_port)
    s.data_version_list()
