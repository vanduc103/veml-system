from neo4j import GraphDatabase

class Versioning():

    def __init__(self):
        self.driver = GraphDatabase.driver(uri, auth=(user, pw))

    def close(self):
        self.driver.close()

    ### Project node ####
    # insert project node
    def _insert_project(self, tx, proj_id, proj_name):
        query = "MERGE (n:Project {id: $id, name: $name}) RETURN n.name AS name"
        res = tx.run(query, id=proj_id, name=proj_name)
        print(res.data())
        
    def insert_project_node(self, proj_id, proj_name):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_project, proj_id, proj_name)
            except Exception as e:
                print("[ERROR] insert node w/o feats", e)
        driver.close()

    # select project
    def _select_project(self, tx, proj_id):
        query = "MATCH (n:Project) WHERE n.id=$id RETURN n.name AS name"
        res = tx.run(query, id=proj_id)
        data = []
        for record in res:
            data.append(record.data()["name"])
        if len(data) > 0:
            return data[0]
        return None
        
    def select_project(self, proj_id):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_project, proj_id)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # remove project
    def _remove_project(self, tx, name):
        query = "MATCH (n:Project) DETACH DELETE n"
        if name:
            query = "MATCH (n:Project {name: $name}) DETACH DELETE n"
        res = tx.run(query, name=name)
        
    def remove_project(self, name=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._remove_project, name)
            except Exception as e:
                print("[ERROR] delete node", e)
        driver.close()


    ### Data version ####
    # insert data version node
    def _insert_data_version(self, tx, metadata_id, proj_id, version, target, description, data_tname, data_points, annotation):
        query = "MERGE (n:Data_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $version, target: $target, description: $description, " \
                +"data_tname: $data_tname, total_datapoints: $total_datapoints, data_points: $data_points, annotation: $annotation}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Data Version {}'.format(version), proj_id=proj_id, version=version, target=target, description=description,
                    data_tname=data_tname, total_datapoints=len(data_points), data_points=data_points, annotation=annotation)
        print(res.data())

    def _insert_edge_proj2data(self, tx, proj_id, version, target):
        query = "MATCH (p:Project {id: $proj_id}) " \
               +"MATCH (v:Data_Version {proj_id: $proj_id, version: $version, target: $target}) " \
               +"CREATE (p)-[rel:INSERT_DATA]->(v) RETURN rel "
        res = tx.run(query, proj_id=proj_id, version=version, target=target)
        print(res.data())
        
    def insert_data_version_node(self, proj_id, data_id, version, target, description, data_tname, data_points, annotation):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_data_version, data_id, proj_id, version, target, description, data_tname, data_points, annotation)
                session.write_transaction(self._insert_edge_proj2data, proj_id, version, target)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()

    # insert data version from another version
    def _insert_data_ver2ver(self, tx, metadata_id, proj_id, version, target, data_tname, data_points, total_datapoints, annotation):
        query = "MERGE (n:Data_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $version, target: $target, data_tname: $data_tname, " \
                +"total_datapoints: $total_datapoints, data_points: $data_points, is_delta: True, annotation: $annotation}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Data Version {}'.format(version), proj_id=proj_id, version=version, target=target, data_tname=data_tname, 
                    total_datapoints=total_datapoints, data_points=data_points, annotation=annotation)
        print(res.data())

    def _insert_data_edge_ver2ver(self, tx, proj_id, pre_version, version, target):
        query = "MATCH (v1:Data_Version {proj_id: $proj_id, version: $pre_version, target: $target}) " \
               +"MATCH (v2:Data_Version {proj_id: $proj_id, version: $version, target: $target}) " \
               +"CREATE (v1)-[rel:ADD_DATA]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, pre_version=pre_version, version=version, target=target)
        print(res.data())
        
    def insert_data_ver2ver(self, pre_version, data_id, proj_id, version, target, data_tname, data_points, total_datapoints, annotation):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_data_ver2ver, data_id, proj_id, version, target, data_tname, data_points, total_datapoints, annotation)
                session.write_transaction(self._insert_data_edge_ver2ver, proj_id, pre_version, version, target)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()
        
    # insert data version by merging versions
    def _insert_data_version_merged(self, tx, metadata_id, proj_id, version, target, data_points, total_datapoints, annotation_points, data_tname, annotation):
        query = "MERGE (n:Data_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $version, target: $target, data_tname: $data_tname, " \
                +"total_datapoints: $total_datapoints, data_points: $data_points, annotation_points: $annotation_points, is_delta: True, annotation: $annotation}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Data Version {}'.format(version), proj_id=proj_id, version=version, target=target,  
                    total_datapoints=total_datapoints, data_points=data_points, annotation_points=annotation_points, data_tname=data_tname, annotation=annotation)
        print(res.data())
        
    def _insert_edge_version_merged(self, tx, proj_id, pre_version, version, target):
        query = "MATCH (v1:Data_Version {proj_id: $proj_id, version: $pre_version, target: $target}) " \
               +"MATCH (v2:Data_Version {proj_id: $proj_id, version: $version, target: $target}) " \
               +"CREATE (v1)-[rel:MERGE_DATA]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, pre_version=pre_version, version=version, target=target)
        print(res.data())
    
    def insert_data_version_merged(self, merge_version_list, data_id, proj_id, version, target, data_points, annotation_points, data_tname, annotation):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_data_version_merged, data_id, proj_id, version, target, data_points, len(data_points), annotation_points, data_tname, annotation)
                for pre_version in merge_version_list:
                    session.write_transaction(self._insert_edge_version_merged, proj_id, pre_version, version, target)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()

    # add new data version by filtering from data version
    def _addfilter_data_version(self, tx, metadata_id, proj_id, version, target, description, data_points, total_datapoints, annotation_points, data_tname, annotation):
        query = "MERGE (n:Data_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $version, target: $target, description: $description, " \
                +"total_datapoints: $total_datapoints, data_points: $data_points, annotation_points: $annotation_points, data_tname: $data_tname, annotation: $annotation}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Data Version {}'.format(version), proj_id=proj_id, version=version, target=target, description=description,
                    total_datapoints=total_datapoints, data_points=data_points, annotation_points=annotation_points, data_tname=data_tname, annotation=annotation)

    def _addfilter_insert_edge(self, tx, proj_id, from_version, from_target, new_version, new_target, filter_conditions):
        query = "MATCH (v1:Data_Version {proj_id: $proj_id, version: $from_version, target: $from_target}) " \
               +"MATCH (v2:Data_Version {proj_id: $proj_id, version: $new_version, target: $new_target}) " \
               +"CREATE (v1)-[rel:FILTER_DATA {filter_conditions: $filter_conditions}]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, from_version=from_version, from_target=from_target, 
                            new_version=new_version, new_target=new_target, filter_conditions=filter_conditions)
        print(res.data())
        
    def addfilter_data_version(self, proj_id, data_id, from_versions, from_target, version, target, description, data_points, annotation_points, filter_conditions, data_tname, annotation):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._addfilter_data_version, data_id, proj_id, version, target, description, data_points, len(data_points), annotation_points, data_tname, annotation)
                for from_version in from_versions:
                    session.write_transaction(self._addfilter_insert_edge, proj_id, from_version, from_target, version, target, filter_conditions)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()

    # update data version node
    def _update_data_version(self, tx, proj_id, version, target, data_points, total_datapoints):
        query = "MATCH (n:Data_Version { " \
                +"proj_id: $proj_id, version: $version, target: $target}) " \
                +"SET n.total_datapoints = $total_datapoints, n.data_points = $data_points " \
                +"RETURN n "
        res = tx.run(query, proj_id=proj_id, version=version, target=target, total_datapoints=total_datapoints, data_points=data_points)

    def update_data_version_node(self, proj_id, version, target, data_points, total_datapoints):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._update_data_version, proj_id, version, target, data_points, total_datapoints)
            except Exception as e:
                print("[ERROR] update node", e)
        driver.close()

    # add image data node
    def _add_image_data_node(self, tx, image_name, image_path):
        query = "MERGE (n:Data_Image {name: $image_name, path: $image_path}) RETURN n"
        res = tx.run(query, image_name=image_name, image_path=image_path)
        results = []
        for record in res:
            results.append(record.data()["n"])
        if len(results) > 0:
            return results[0]
        return None

    def _add_edge_data_version2image(self, tx, proj_id, version, image_name, image_path):
        query = "MATCH (v1:Data_Version {proj_id: $proj_id, version: $version}) " \
               +"MATCH (v2:Data_Image {name: $name, path: $path}) " \
               +"MERGE (v1)-[rel:HAS_IMAGE]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, version=version, name=image_name, path=image_path)
        print(res.data())

    def add_image_data_node(self, data_node, image_name, image_path):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        image_node = None
        with driver.session() as session:
            try:
                image_node = session.write_transaction(self._add_image_data_node, image_name, image_path)
                session.write_transaction(self._add_edge_data_version2image, data_node['proj_id'], data_node['version'], image_name, image_path)
            except Exception as e:
                print("[ERROR] insert node", e)
        driver.close()
        return image_node

    # select data version
    def _select_data_version(self, tx, proj_id, version, target):
        query = "MATCH (n:Data_Version) WHERE n.proj_id=$proj_id AND n.version=$version AND n.target=$target RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version, target=target)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def select_data_version(self, proj_id, version, target):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_data_version, proj_id, version, target)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select data points from version
    def _select_data_points(self, tx, proj_id, version, target):
        query = "MATCH (n:Data_Version) WHERE n.proj_id=$proj_id AND n.version=$version AND n.target=$target RETURN n.data_points AS data_points"
        res = tx.run(query, proj_id=proj_id, version=version, target=target)
        data = []
        for record in res:
            data.append(record["data_points"])
        if len(data) > 0:
            data = data[0]
        return data
        
    def select_data_points(self, proj_id, version, target):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        data = []
        with driver.session() as session:
            try:
                data = session.read_transaction(self._select_data_points, proj_id, version, target)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data

    # select data table name
    def _select_data_tname(self, tx, proj_id, version, target):
        query = "MATCH (n:Data_Version) WHERE n.proj_id=$proj_id AND n.version=$version AND n.target=$target RETURN n.data_tname AS data_tname"
        res = tx.run(query, proj_id=proj_id, version=version, target=target)
        data = []
        for record in res:
            data.append(record["data_tname"])
        if len(data) > 0:
            return data[0]
        return None
        
    def select_data_tname(self, proj_id, version, target):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        data_tname = None
        with driver.session() as session:
            try:
                data_tname = session.read_transaction(self._select_data_tname, proj_id, version, target)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data_tname

    # select all data versions
    def _select_all_dataversion(self, tx, proj_id):
        query = "MATCH (n:Data_Version) WHERE n.proj_id=$proj_id RETURN n.version AS version, n.target AS target"
        res = tx.run(query, proj_id=proj_id)
        data = []
        for record in res:
            data.append({"version": record["version"], "target": record["target"]})
        return data
        
    def select_all_dataversion(self, proj_id):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        data = []
        with driver.session() as session:
            try:
                data = session.read_transaction(self._select_all_dataversion, proj_id)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data

    # check available data version
    def _is_available_data_version(self, tx, proj_id, version):
        query = "MATCH (n:Data_Version) WHERE n.proj_id=$proj_id AND n.version=$version RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def is_available_data_version(self, proj_id, version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._is_available_data_version, proj_id, version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # get list test version of the data version
    def _list_test_version_of_data_version(self, tx, proj_id, data_version):
        query = "MATCH (n1:Data_Version)-[r1:DATA_TRAINING]->(n2:Training_Version)-[r2:TESTING_RESULTS]->(n3:Test_Version) " \
                "WHERE n1.proj_id=$proj_id AND n1.version=$data_version " \
                "RETURN n3.version AS version"
        res = tx.run(query, proj_id=proj_id, data_version=data_version)
        data = []
        for record in res:
            data.append(record.data()["version"])
        if len(data) > 0:
            return data
        return None
        
    def list_test_version_of_data_version(self, proj_id, data_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._list_test_version_of_data_version, proj_id, data_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result
        
    # remove data version
    def _remove_data_version(self, tx, data_version):
        query = "MATCH (n:Data_Version) DETACH DELETE n"
        if data_version:
            query = "MATCH (n:Data_Version {version: $data_version}) DETACH DELETE n"
        res = tx.run(query, data_version=data_version)
    
    def _remove_image_data(self, tx, data_version):
        query = "MATCH (n:Data_Image) DETACH DELETE n"
        if data_version:
            query = "MATCH (:Data_Version {version: $data_version})-[:HAS_IMAGE]->(n:Data_Image) DETACH DELETE n"
        res = tx.run(query, data_version=data_version)
        
    def remove_data_version(self, data_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._remove_data_version, data_version)
                session.write_transaction(self._remove_image_data, data_version)
            except Exception as e:
                print("[ERROR] delete node", e)
        driver.close()


    #### Model version ####
    def _insert_model_version(self, tx, metadata_id, proj_id, model_version, model_name):
        query = "MERGE (n:Model_Version { " \
                +"metadata_id: $metadata_id, proj_id: $proj_id, version: $model_version, name: $model_name}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, proj_id=proj_id, model_version=model_version, model_name=model_name)
        print(res.data())

    def _insert_edge_proj2model(self, tx, proj_id, model_version):
        query = "MATCH (p:Project {id: $proj_id}) " \
               +"MATCH (v:Model_Version {proj_id: $proj_id, version: $model_version}) " \
               +"CREATE (p)-[rel:ADD_MODEL]->(v) RETURN rel "
        res = tx.run(query, proj_id=proj_id, model_version=model_version)
        print(res.data())
        
    def _insert_edge_model_transfer(self, tx, proj_id, transfer_model_version, model_version):
        query = "MATCH (v1:Model_Version {proj_id: $proj_id, version: $transfer_model_version}) " \
               +"MATCH (v2:Model_Version {proj_id: $proj_id, version: $model_version}) " \
               +"CREATE (v1)-[rel:MODEL_TRANSFER]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, transfer_model_version=transfer_model_version, model_version=model_version)
        print(res.data())

    def _insert_edge_model_finetune(self, tx, proj_id, from_model_version, model_version):
        query = "MATCH (v1:Model_Version {proj_id: $proj_id, version: $from_model_version}) " \
               +"MATCH (v2:Model_Version {proj_id: $proj_id, version: $model_version}) " \
               +"CREATE (v1)-[rel:MODEL_FINETUNE]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, from_model_version=from_model_version, model_version=model_version)
        print(res.data())
    
    def _insert_edge_data2model(self, tx, proj_id, data_version, model_version):
        query = "MATCH (v1:Data_Version {proj_id: $proj_id, version: $data_version}) " \
               +"MATCH (v2:Model_Version {proj_id: $proj_id, version: $model_version}) " \
               +"CREATE (v1)-[rel:DATA_FOR_MODEL]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, data_version=data_version, model_version=model_version)
        print(res.data())
    
    def insert_model_version_node(self, proj_id, model_id, model_version, model_name):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_model_version, model_id, proj_id, model_version, model_name)
                session.write_transaction(self._insert_edge_proj2model, proj_id, model_version)
            except Exception as e:
                print("[ERROR] insert model version node ", e)
        driver.close()

    def insert_model_version_transfer(self, proj_id, model_id, model_version, transfer_model_version, model_name):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_model_version, model_id, proj_id, model_version, model_name)
                if transfer_model_version:
                    session.write_transaction(self._insert_edge_model_transfer, proj_id, transfer_model_version, model_version)
            except Exception as e:
                print("[ERROR] insert model transfer ", e)
        driver.close()

    def insert_model_version_finetune(self, proj_id, model_id, model_version, from_model_version, model_name, data_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_model_version, model_id, proj_id, model_version, model_name)
                if from_model_version:
                    session.write_transaction(self._insert_edge_model_finetune, proj_id, from_model_version, model_version)
                if data_version:
                    session.write_transaction(self._insert_edge_data2model, proj_id, data_version, model_version)
            except Exception as e:
                print("[ERROR] insert model finetune ", e)
        driver.close()

    # add model information (checkpoint, graph,...) to model version
    def _add_model_info(self, tx, proj_id, model_version, kwargs):
        query = "MATCH (n:Model_Version { " \
                +"proj_id: $proj_id, version: $model_version}) " \
                +"SET n.checkpoint=$checkpoint, n.graph=$graph " \
                +"RETURN n "
        res = tx.run(query, proj_id=proj_id, model_version=model_version, 
                    checkpoint=kwargs.get('checkpoint'), graph=kwargs.get('graph'))

    def add_model_info(self, proj_id, model_version, **kwargs):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._add_model_info, proj_id, model_version, kwargs)
            except Exception as e:
                print("[ERROR] update node", e)
        driver.close()
        

    # select model version
    def _select_model_version(self, tx, proj_id, model_version):
        query = "MATCH (n:Model_Version) WHERE n.proj_id=$proj_id AND n.version=$model_version RETURN n"
        res = tx.run(query, proj_id=proj_id, model_version=model_version)
        results = []
        for record in res:
            results.append(record.data()["n"])
        if len(results) > 0:
            return results[0]
        return None
        
    def select_model_version(self, proj_id, model_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_model_version, proj_id, model_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select base model version of this model version
    def _select_base_model_version_relationship(self, tx, proj_id, model_version):
        query = "MATCH (d:Model_Version)-[rel:MODEL_FINETUNE]->(n:Model_Version) "\
                "WHERE n.proj_id=$proj_id AND n.version=$model_version RETURN d"
        res = tx.run(query, proj_id=proj_id, model_version=model_version)
        results = []
        for record in res:
            results.append(record.data()["d"])
        if len(results) > 0:
            return results[0]
        return results
        
    def select_base_model_version_relationship(self, proj_id, model_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_base_model_version_relationship, proj_id, model_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select all model versions
    def _select_all_modelversion(self, tx, proj_id):
        query = "MATCH (n:Model_Version) WHERE n.proj_id=$proj_id RETURN n.version AS version"
        res = tx.run(query, proj_id=proj_id)
        data = []
        for record in res:
            data.append(record["version"])
        return data
        
    def select_all_modelversion(self, proj_id):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        data = []
        with driver.session() as session:
            try:
                data = session.read_transaction(self._select_all_modelversion, proj_id)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data

    # check available model version
    def _is_available_model_version(self, tx, proj_id, version):
        query = "MATCH (n:Model_Version) WHERE n.proj_id=$proj_id AND n.version=$version RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def is_available_model_version(self, proj_id, version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._is_available_model_version, proj_id, version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # remove model version
    def _remove_model_version(self, tx, model_version=None):
        query = "MATCH (n:Model_Version) DETACH DELETE n"
        if model_version:
            query = "MATCH (n:Model_Version {version:$model_version}) DETACH DELETE n"
        res = tx.run(query, model_version=model_version)
        
    def remove_model_version(self, model_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._remove_model_version, model_version)
            except Exception as e:
                print("[ERROR] delete node", e)
        driver.close()


    #### Training version ####
    def _insert_training_version(self, tx, metadata_id, proj_id, training_version, training_metadata):
        query = "MERGE (n:Training_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $training_version, " \
                +"batch_size: $batch_size, epochs: $epochs, learning_rate: $learning_rate}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Training Version {}'.format(training_version), proj_id=proj_id, training_version=training_version,
                            batch_size=training_metadata['batch_size'], 
                            epochs=training_metadata['epochs'],
                            learning_rate=training_metadata['learning_rate'])
        print(res.data())
        
    def _insert_edge_data2training(self, tx, proj_id, data_version, training_version):
        query = "MATCH (v1:Data_Version {proj_id: $proj_id, version: $data_version}) " \
               +"MATCH (v2:Training_Version {proj_id: $proj_id, version: $training_version}) " \
               +"CREATE (v1)-[rel:DATA_TRAINING]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, data_version=data_version, training_version=training_version)
        print(res.data())

    def _insert_edge_model2training(self, tx, proj_id, model_version, training_version):
        query = "MATCH (v1:Model_Version {proj_id: $proj_id, version: $model_version}) " \
               +"MATCH (v2:Training_Version {proj_id: $proj_id, version: $training_version}) " \
               +"CREATE (v1)-[rel:MODEL_TRAINING]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, model_version=model_version, training_version=training_version)
        print(res.data())

    def _insert_edge_training2training(self, tx, proj_id, pre_training_version, training_version):
        query = "MATCH (v1:Training_Version {proj_id: $proj_id, version: $pre_training_version}) " \
               +"MATCH (v2:Training_Version {proj_id: $proj_id, version: $training_version}) " \
               +"CREATE (v1)-[rel:TRAINING_TRANSFER]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, pre_training_version=pre_training_version, training_version=training_version)
    
    def insert_training_version(self, metadata_id, proj_id, training_version, data_version, model_version, training_metadata, pre_training_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_training_version, metadata_id, proj_id, training_version, training_metadata)
                session.write_transaction(self._insert_edge_data2training, proj_id, data_version, training_version)
                session.write_transaction(self._insert_edge_model2training, proj_id, model_version, training_version)
                if pre_training_version:
                    session.write_transaction(self._insert_edge_training2training, proj_id, pre_training_version, training_version)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()

    # add training logs to training version
    def _add_training_logs(self, tx, proj_id, training_version, tname, fields, training_logs):
        query = "MATCH (n:Training_Version { " \
                +"proj_id: $proj_id, version: $training_version}) " \
                +"SET n.tname=$tname, n.fields=$fields, n.training_logs=$training_logs " \
                +"RETURN n "
        res = tx.run(query, proj_id=proj_id, training_version=training_version, tname=tname, fields=fields, training_logs=training_logs)

    def add_training_logs(self, proj_id, training_version, tname, fields, training_logs):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._add_training_logs, proj_id, training_version, tname, fields, training_logs)
            except Exception as e:
                print("[ERROR] update node", e)
        driver.close()

    # add training results (checkpoint and evaluation) to training version
    def _add_training_results(self, tx, proj_id, training_version, checkpoint, evaluation):
        query = "MATCH (n:Training_Version { " \
                +"proj_id: $proj_id, version: $training_version}) " \
                +"SET n.checkpoint=$checkpoint, n.evaluation=$evaluation " \
                +"RETURN n "
        res = tx.run(query, proj_id=proj_id, training_version=training_version, checkpoint=checkpoint, evaluation=evaluation)

    def add_training_results(self, proj_id, training_version, checkpoint, evaluation):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._add_training_results, proj_id, training_version, checkpoint, evaluation)
            except Exception as e:
                print("[ERROR] update node", e)
        driver.close()
        
    # select training version
    def _select_training_version(self, tx, proj_id, training_version):
        query = "MATCH (n:Training_Version) WHERE n.proj_id=$proj_id AND n.version=$training_version RETURN n"
        res = tx.run(query, proj_id=proj_id, training_version=training_version)
        results = []
        for record in res:
            results.append(record.data()["n"])
        if len(results) > 0:
            return results[0]
        return None
        
    def select_training_version(self, proj_id, training_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_training_version, proj_id, training_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select data version that has relationship with training version
    def _select_training_version_data_relationship(self, tx, proj_id, training_version):
        query = "MATCH (d:Data_Version)-[rel:DATA_TRAINING]->(n:Training_Version) "\
                "WHERE n.proj_id=$proj_id AND n.version=$training_version RETURN d"
        res = tx.run(query, proj_id=proj_id, training_version=training_version)
        results = []
        for record in res:
            results.append(record.data()["d"])
        if len(results) > 0:
            return results[0]
        return results
        
    def select_training_version_data_relationship(self, proj_id, training_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_training_version_data_relationship, proj_id, training_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select model version that has relationship with training version
    def _select_training_version_model_relationship(self, tx, proj_id, training_version):
        query = "MATCH (d:Model_Version)-[rel:MODEL_TRAINING]->(n:Training_Version) "\
                "WHERE n.proj_id=$proj_id AND n.version=$training_version RETURN d"
        res = tx.run(query, proj_id=proj_id, training_version=training_version)
        results = []
        for record in res:
            results.append(record.data()["d"])
        if len(results) > 0:
            return results[0]
        return results
        
    def select_training_version_model_relationship(self, proj_id, training_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_training_version_model_relationship, proj_id, training_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # check available training version
    def _is_available_training_version(self, tx, proj_id, version):
        query = "MATCH (n:Training_Version) WHERE n.proj_id=$proj_id AND n.version=$version RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def is_available_training_version(self, proj_id, version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._is_available_training_version, proj_id, version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # remove training version
    def _remove_training_version(self, tx, training_version=None):
        query = "MATCH (n:Training_Version) DETACH DELETE n"
        if training_version:
            query = "MATCH (n:Training_Version {version: $training_version}) DETACH DELETE n"
        res = tx.run(query, training_version=training_version)
        
    def remove_training_version(self, training_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._remove_training_version, training_version)
            except Exception as e:
                print("[ERROR] delete node", e)
        driver.close()


    #### Inference (deploy) version ####
    def _insert_inference_version(self, tx, metadata_id, proj_id, inference_version, inference_metadata):
        query = "MERGE (n:Inference_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $inference_version, device: $device, inference_model: $inference_model}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Inference Version {}'.format(inference_version), proj_id=proj_id, 
                    inference_version=inference_version, device=inference_metadata['device'], inference_model=inference_metadata['inference_model'])
        print(res.data())
        
    def _insert_edge_training2inference(self, tx, proj_id, training_version, inference_version, inference_metadata):
        query = "MATCH (v1:Training_Version {proj_id: $proj_id, version: $training_version}) " \
               +"MATCH (v2:Inference_Version {proj_id: $proj_id, version: $inference_version}) " \
               +"CREATE (v1)-[rel:DEPLOY {optimization: $optimization}]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, training_version=training_version, inference_version=inference_version,
                            optimization=inference_metadata['optimization'])
        print(res.data())

    def insert_inference_version(self, metadata_id, proj_id, inference_version, training_version, inference_metadata):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_inference_version, metadata_id, proj_id, inference_version, inference_metadata)
                session.write_transaction(self._insert_edge_training2inference, proj_id, training_version, inference_version, inference_metadata)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()

    # select inference version
    def _select_inference_version(self, tx, proj_id, inference_version):
        query = "MATCH (n:Inference_Version) WHERE n.proj_id=$proj_id AND n.version=$inference_version RETURN n"
        res = tx.run(query, proj_id=proj_id, inference_version=inference_version)
        results = []
        for record in res:
            results.append(record.data()["n"])
        if len(results) > 0:
            return results[0]
        return None
        
    def select_inference_version(self, proj_id, inference_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_inference_version, proj_id, inference_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select training version that has relationship with inference version
    def _select_inference_version_training_relationship(self, tx, proj_id, inference_version):
        query = "MATCH (d:Training_Version)-[rel:DEPLOY]->(n:Inference_Version) "\
                "WHERE n.proj_id=$proj_id AND n.version=$inference_version RETURN d"
        res = tx.run(query, proj_id=proj_id, inference_version=inference_version)
        results = []
        for record in res:
            results.append(record.data()["d"])
        if len(results) > 0:
            return results[0]
        return results
        
    def select_inference_version_training_relationship(self, proj_id, inference_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_inference_version_training_relationship, proj_id, inference_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select predict version that has relationship with inference version
    def _select_inference_version_predict_relationship(self, tx, proj_id, inference_version):
        query = "MATCH (n:Inference_Version)-[rel:PREDICT]->(d:Predict_Version) "\
                "WHERE n.proj_id=$proj_id AND n.version=$inference_version RETURN d"
        res = tx.run(query, proj_id=proj_id, inference_version=inference_version)
        results = []
        for record in res:
            results.append(record.data()["d"])
        return results
        
    def select_inference_version_predict_relationship(self, proj_id, inference_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_inference_version_predict_relationship, proj_id, inference_version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # check available inference version
    def _is_available_inference_version(self, tx, proj_id, version):
        query = "MATCH (n:Inference_Version) WHERE n.proj_id=$proj_id AND n.version=$version RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def is_available_inference_version(self, proj_id, version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._is_available_inference_version, proj_id, version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # remove inference version
    def _remove_inference_version(self, tx, inference_version=None):
        query = "MATCH (n:Inference_Version) DETACH DELETE n"
        if inference_version:
            query = "MATCH (n:Inference_Version {version: $inference_version}) DETACH DELETE n"
        res = tx.run(query, inference_version=inference_version)
        
    def remove_inference_version(self, inference_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._remove_inference_version, inference_version)
            except Exception as e:
                print("[ERROR] delete node", e)
        driver.close()


    #### Test version ####
    # insert test version node
    def _insert_test_version(self, tx, metadata_id, proj_id, version, data_tname, data_points):
        query = "MERGE (n:Test_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $version, data_tname: $data_tname, " \
                +"total_datapoints: $total_datapoints, data_points: $data_points}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Test Version {}'.format(version), proj_id=proj_id, version=version, data_tname=data_tname, 
                    total_datapoints=len(data_points), data_points=data_points)
        print(res.data())

    def _insert_edge_training2test(self, tx, proj_id, training_version, test_version, test_params):
        query = "MATCH (d1:Training_Version {proj_id: $proj_id, version: $training_version}) " \
               +"MATCH (d2:Test_Version {proj_id: $proj_id, version: $test_version}) " \
               +"CREATE (d1)-[rel:TESTING_RESULTS {score_threshold: $threshold, test_device: $device}]->(d2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, training_version=training_version, test_version=test_version,
                            threshold=test_params['threshold'], device=test_params['device'])
        print(res.data())
        
    def _insert_edge_test2data(self, tx, proj_id, test_version, data_version):
        query = "MATCH (d1:Test_Version {proj_id: $proj_id, version: $test_version}) " \
               +"MATCH (d2:Data_Version {proj_id: $proj_id, version: $data_version}) " \
               +"CREATE (d1)-[rel:TEST_ON_DATA]->(d2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, test_version=test_version, data_version=data_version)
        print(res.data())
        
    def insert_test_version_node(self, metadata_id, proj_id, training_version, version, data_tname, data_points, test_params, data_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_test_version, metadata_id, proj_id, version, data_tname, data_points)
                session.write_transaction(self._insert_edge_training2test, proj_id, training_version, version, test_params)
                if data_version:
                    session.write_transaction(self._insert_edge_test2data, proj_id, version, data_version)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()

    # add test image node
    def _add_test_image_node(self, tx, test_version, image_name, image_path):
        query = "MERGE (n:Test_Image {test_version: $test_version, name: $image_name, path: $image_path}) RETURN n"
        res = tx.run(query, test_version=test_version, image_name=image_name, image_path=image_path)
        results = []
        for record in res:
            results.append(record.data()["n"])
        if len(results) > 0:
            return results[0]
        return None

    def _add_edge_test_version2image(self, tx, proj_id, version, image_name, image_path):
        query = "MATCH (v1:Test_Version {proj_id: $proj_id, version: $version}) " \
               +"MATCH (v2:Test_Image {name: $name, path: $path}) " \
               +"MERGE (v1)-[rel:HAS_IMAGE]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, version=version, name=image_name, path=image_path)
        print(res.data())

    # create relationship between test image node and data image node
    def _add_edge_test_image2data_image(self, tx, image_name, image_path):
        query = "MATCH (v1:Test_Image {name: $name, path: $path}) " \
               +"MATCH (v2:Data_Image {name: $name, path: $path}) " \
               +"MERGE (v1)-[rel:SAME_IMAGE]->(v2) RETURN rel "
        res = tx.run(query, name=image_name, path=image_path)
        print(res.data())

    def add_test_image_node(self, test_image_node, image_name, image_path):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        image_node = None
        with driver.session() as session:
            try:
                image_node = session.write_transaction(self._add_test_image_node, test_image_node['version'], image_name, image_path)
                session.write_transaction(self._add_edge_test_version2image, test_image_node['proj_id'], test_image_node['version'], image_name, image_path)
                session.write_transaction(self._add_edge_test_image2data_image, image_name, image_path)
            except Exception as e:
                print("[ERROR] insert node", e)
        driver.close()
        return image_node
        
    # select test version
    def _select_test_version(self, tx, proj_id, version):
        query = "MATCH (n:Test_Version) WHERE n.proj_id=$proj_id AND n.version=$version RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def select_test_version(self, proj_id, version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_test_version, proj_id, version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select data points from test version
    def _select_test_data_points(self, tx, proj_id, version):
        query = "MATCH (n:Test_Version) WHERE n.proj_id=$proj_id AND n.version=$version RETURN n.data_points AS data_points"
        res = tx.run(query, proj_id=proj_id, version=version)
        data = []
        for record in res:
            data.append(record["data_points"])
        if len(data) > 0:
            data = data[0]
        return data
        
    def select_test_data_points(self, proj_id, version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        data = []
        with driver.session() as session:
            try:
                data = session.read_transaction(self._select_test_data_points, proj_id, version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data

    # update test version node
    def _update_test_version(self, tx, proj_id, version, data_points, total_datapoints):
        query = "MATCH (n:Test_Version { " \
                +"proj_id: $proj_id, version: $version}) " \
                +"SET n.total_datapoints = $total_datapoints, n.data_points = $data_points " \
                +"RETURN n "
        res = tx.run(query, proj_id=proj_id, version=version, total_datapoints=total_datapoints, data_points=data_points)

    def update_test_version_node(self, proj_id, version, data_points, total_datapoints):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._update_test_version, proj_id, version, data_points, total_datapoints)
            except Exception as e:
                print("[ERROR] update node ", e)
        driver.close()

    # check available testing version
    def _is_available_testing_version(self, tx, proj_id, version):
        query = "MATCH (n:Test_Version) WHERE n.proj_id=$proj_id AND n.version=$version RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def is_available_testing_version(self, proj_id, version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._is_available_testing_version, proj_id, version)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # remove test version
    def _remove_test_version(self, tx, test_version=None):
        query = "MATCH (n:Test_Version) DETACH DELETE n"
        if test_version:
            query = "MATCH (n:Test_Version {version: $test_version}) DETACH DELETE n"
        res = tx.run(query, test_version=test_version)

    def _remove_test_image_version(self, tx, test_version=None):
        query = "MATCH (n:Test_Image) DETACH DELETE n"
        if test_version:
            query = "MATCH (:Test_Version {version: $test_version})-[:HAS_IMAGE]->(n:Test_Image) DETACH DELETE n"
        res = tx.run(query, test_version=test_version)
        
    def remove_test_version(self, test_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._remove_test_version, test_version)
                session.write_transaction(self._remove_test_image_version, test_version)
            except Exception as e:
                print("[ERROR] delete node", e)
        driver.close()


    #### Predict data version ####
    # insert predict version node
    def _insert_predict_version(self, tx, metadata_id, proj_id, version, target, data_tname, data_points, has_predict):
        query = "MERGE (n:Predict_Version { " \
                +"metadata_id: $metadata_id, name: $name, proj_id: $proj_id, version: $version, target: $target, data_tname: $data_tname, " \
                +"total_datapoints: $total_datapoints, data_points: $data_points, is_delta: False, has_predict: $has_predict}) " \
                +"RETURN n.version AS version"
        res = tx.run(query, metadata_id=metadata_id, name='Predict Version {}'.format(version), proj_id=proj_id, version=version, target=target, data_tname=data_tname, 
                    total_datapoints=len(data_points), data_points=data_points, has_predict=has_predict)
        print(res.data())

    def _insert_edge_inference2pred(self, tx, proj_id, inference_version, pred_version, target, pred_params):
        query = "MATCH (m:Inference_Version {proj_id: $proj_id, version: $inference_version}) " \
               +"MATCH (v:Predict_Version {proj_id: $proj_id, version: $pred_version, target: $target}) " \
               +"CREATE (m)-[rel:PREDICT {top_k: $top_k, score_threshold: $score_threshold}]->(v) RETURN rel "
        res = tx.run(query, proj_id=proj_id, inference_version=inference_version, pred_version=pred_version, target=target,
                            top_k=pred_params['top_k'],
                            score_threshold=pred_params['score_threshold'])
        print(res.data())
        
    def _insert_edge_pred2data(self, tx, proj_id, pred_version, data_version):
        query = "MATCH (m:Predict_Version {proj_id: $proj_id, version: $pred_version}) " \
               +"MATCH (v:Data_Version {proj_id: $proj_id, version: $data_version}) " \
               +"CREATE (m)-[rel:PREDICT_ON_DATA]->(v) RETURN rel "
        res = tx.run(query, proj_id=proj_id, pred_version=pred_version, data_version=data_version)
        print(res.data())
        
    def insert_predict_version_node(self, metadata_id, proj_id, inference_version, version, target, data_tname, data_points, has_predict, pred_params, data_version):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._insert_predict_version, metadata_id, proj_id, version, target, data_tname, data_points, has_predict)
                session.write_transaction(self._insert_edge_inference2pred, proj_id, inference_version, version, target, pred_params)
                session.write_transaction(self._insert_edge_pred2data, proj_id, version, data_version)
            except Exception as e:
                print("[ERROR] insert node ", e)
        driver.close()

    # add predict image node
    def _add_predict_image_node(self, tx, image_name, image_path):
        query = "MERGE (n:Predict_Image {name: $image_name, path: $image_path}) RETURN n"
        res = tx.run(query, image_name=image_name, image_path=image_path)
        results = []
        for record in res:
            results.append(record.data()["n"])
        if len(results) > 0:
            return results[0]
        return None

    def _add_edge_predict_version2image(self, tx, proj_id, version, image_name, image_path):
        query = "MATCH (v1:Predict_Version {proj_id: $proj_id, version: $version}) " \
               +"MATCH (v2:Predict_Image {name: $name, path: $path}) " \
               +"MERGE (v1)-[rel:HAS_IMAGE]->(v2) RETURN rel "
        res = tx.run(query, proj_id=proj_id, version=version, name=image_name, path=image_path)
        print(res.data())

    def add_predict_image_node(self, pred_node, image_name, image_path):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        image_node = None
        with driver.session() as session:
            try:
                image_node = session.write_transaction(self._add_predict_image_node, image_name, image_path)
                session.write_transaction(self._add_edge_predict_version2image, pred_node['proj_id'], pred_node['version'], image_name, image_path)
            except Exception as e:
                print("[ERROR] insert node", e)
        driver.close()
        return image_node
        
    # select predict version
    def _select_predict_version(self, tx, proj_id, version, target):
        query = "MATCH (n:Predict_Version) WHERE n.proj_id=$proj_id AND n.version=$version AND n.target=$target RETURN n"
        res = tx.run(query, proj_id=proj_id, version=version, target=target)
        data = []
        for record in res:
            data.append(record.data()["n"])
        if len(data) > 0:
            return data[0]
        return None
        
    def select_predict_version(self, proj_id, version, target):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        result = None
        with driver.session() as session:
            try:
                result = session.read_transaction(self._select_predict_version, proj_id, version, target)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return result

    # select data points from predict version
    def _select_predict_points(self, tx, proj_id, version, target):
        query = "MATCH (n:Predict_Version) WHERE n.proj_id=$proj_id AND n.version=$version AND n.target=$target RETURN n.data_points AS data_points"
        res = tx.run(query, proj_id=proj_id, version=version, target=target)
        data = []
        for record in res:
            data.append(record["data_points"])
        if len(data) > 0:
            data = data[0]
        return data
        
    def select_predict_points(self, proj_id, version, target):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        data = []
        with driver.session() as session:
            try:
                data = session.read_transaction(self._select_predict_points, proj_id, version, target)
            except Exception as e:
                print("[ERROR] select node", e)
        driver.close()
        return data

    # update predict version node
    def _update_predict_version(self, tx, proj_id, version, target, data_points, total_datapoints):
        query = "MATCH (n:Predict_Version { " \
                +"proj_id: $proj_id, version: $version, target: $target}) " \
                +"SET n.total_datapoints = $total_datapoints, n.data_points = $data_points " \
                +"RETURN n "
        res = tx.run(query, proj_id=proj_id, version=version, target=target, total_datapoints=total_datapoints, data_points=data_points)

    def update_predict_version_node(self, proj_id, version, target, data_points, total_datapoints):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._update_predict_version, proj_id, version, target, data_points, total_datapoints)
            except Exception as e:
                print("[ERROR] update node ", e)
        driver.close()

    # remove predict version
    def _remove_predict_version(self, tx, predict_version=None):
        query = "MATCH (n:Predict_Version) DETACH DELETE n"
        if predict_version:
            query = "MATCH (n:Predict_Version {version:$predict_version}) DETACH DELETE n"
        res = tx.run(query, predict_version=predict_version)

    def _remove_predict_image_version(self, tx, predict_version=None):
        query = "MATCH (n:Predict_Image) DETACH DELETE n"
        if predict_version:
            query = "MATCH (:Predict_Version {version:$predict_version})-[:HAS_IMAGE]->(n:Predict_Image) DETACH DELETE n"
        res = tx.run(query, predict_version=predict_version)
        
    def remove_predict_version(self, predict_version=None):
        driver = GraphDatabase.driver(uri, auth=(user, pw))
        with driver.session() as session:
            try:
                session.write_transaction(self._remove_predict_version, predict_version)
                session.write_transaction(self._remove_predict_image_version, predict_version)
            except Exception as e:
                print("[ERROR] delete node", e)
        driver.close()

class TestNeo4j():
    def __init__(self, uri, user, pw):
        self.uri = uri
        self.user = user
        self.pw = pw
    
    def delete_all(self, tx):
        res = tx.run("MATCH (n:Person) DETACH DELETE n")
        print(res)
    
    def insert_person(self, tx, nodes):
        st = time.time()
        for node in nodes:
            node_tx = "CREATE (n:Person {name: $node_prop})"
            res = tx.run(node_tx, node_prop=node["node_prop"])
            print(res)
        print("Insertion time: %.2fs"% (time.time() - st))
        
    def insert_edge(self, tx, nodes, rel_name):
        edge_tx = "MATCH (n0:Person {name: $prop1}) " \
                 +"MATCH (n1:Person {name: $prop2}) " \
                 +"CREATE (n0)-[rel:%s]->(n1) RETURN rel" % (rel_name)
        st = time.time()
        res = tx.run(edge_tx, prop1=nodes[0]["node_prop"], prop2=nodes[1]["node_prop"])
        print(res.data())
        print("Insertion time: %.2fs"% (time.time() - st))

    def select_person(self, tx, nodes):
        match_tx = "MATCH (n:Person) RETURN n.name as name"
        res = tx.run(match_tx)
        print(res.data())


