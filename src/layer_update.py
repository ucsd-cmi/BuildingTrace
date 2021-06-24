import os
from trace import autoPilot, autoPilotManhole
from env_setup import getArcCredentials
from arcgis.gis import GIS
import sys
import datetime
import json

class ArcgisError(Exception):
    """Base class for other exceptions"""
    pass
class InvalidLayerIdError(ArcgisError):
    """Raised when the layer id is invalid"""
    pass

class ArcgisOperation:
    def __init__(self):
        try:
            self.arc_username = os.environ['ARC_USER']
            self.arc_password = os.environ['ARC_PASS']
        except KeyError:
            getArcCredentials()
            self.arc_username = os.environ['ARC_USER']
            self.arc_password = os.environ['ARC_PASS']
        self.gis = GIS("https://www.arcgis.com", self.arc_username, self.arc_password)
        print("login success")
    def getItemById(self,layer_id):
        gis_item = self.gis.content.get(layer_id)
        if not gis_item: raise InvalidLayerIdError
        return gis_item
    def cloneItem(self, gis_item):
        results = self.gis.content.clone_items(items=[gis_item],
                                        owner=self.arc_username)
        return results
    def contentSearch(self,owner,query=""):
        """
        return a list of items that match the query
        """
        query_string = query + " " + "owner:" + owner
        return self.gis.content.search(query_string)
    def getFeatures(self,layer):
        features = layer.query().features
        return features
    def addField(self,field_objects,layer):
        """
        add a field or fields to a table with a list of dictionaries
        """
        message = layer.manager.add_to_definition({"fields":field_objects})
        return message
    def updateTable(self,layer,features):
        """
        update a list of features on a layer table
        """
        results = layer.edit_features(updates=features)
        return results
    def addToTable(self,layer,features):
        """
        add a list of features to a layer table
        """
        results = layer.edit_features(adds=features)
        return results

def write_json(date_value, filename="../data/historical_date.json"):
    # with open(filename) as json_file:
    #     data = json.load(json_file)
    #     data.append(date_value)
      
    # with open(filename,'w') as f:
    #     json.dump(data, f, indent=4)
    return

def check_exist(date_value, filename="../data/historical_date.json"):
    with open(filename) as json_file:
        data = json.load(json_file)
    return date_value in data

def updateBuilding(date_val,trace_mode="single"):
    arcgis = ArcgisOperation()
    # loop to update, default detection mode
    if trace_mode == "single":
        building_layer = arcgis.contentSearch(arcgis.arc_username,"TracedBuildings_oneday")[0].layers[1]
        features = arcgis.getFeatures(building_layer)
        error_message, affected_buildings = autoPilot(date_val)
        affected_set = set(affected_buildings)
        if not error_message:
            for feat in features:
                binary_check =  "Yes" if feat.attributes['CAANtext'] in affected_set else "No"
                feat.attributes['PossibleSource'] = binary_check
                feat.attributes['CASE_DATE'] = date_val
            update_result = building_layer.edit_features(updates=features)
            success_cnt = sum(int(elem['success']) for elem in update_result['updateResults'])
            fail_cnt = len(update_result['updateResults']) - success_cnt
            report = {'update_success_count':success_cnt,'update_fail_count':fail_cnt}
            return error_message, report
        return error_message,{}
    else:
        # check three modes
        building_layer = arcgis.contentSearch(arcgis.arc_username,"multi_trace_layer")[0].layers[0]
        features = arcgis.getFeatures(building_layer)
        mode_col_map = {'detection':'Detection','monitoring':'Monitoring','sampling':'Sampling'}
        status_types = ["Not Currently Monitored","Currently Monitored + Not Sampled","Currently Monitored + Sampled + Not Detected","Currently Monitored + Sampled + Detected"]
        status_sign_cnt = [0]*len(features)
        for mode, name in mode_col_map.items():
            error_message, affected_buildings = autoPilot(date_val,mode=mode)
            affected_set = set(affected_buildings)
            if not error_message:
                for idx,feat in enumerate(features):
                    binary_check =  "Yes" if feat.attributes['CAANtext'] in affected_set else "No"
                    if binary_check == "Yes":status_sign_cnt[idx] += 1
                    feat.attributes[name] = binary_check
                    feat.attributes['Date'] = datetime.datetime.strptime(date_val,'%m/%d/%y')+datetime.timedelta(days=1) # add one day to counter UTC To PST difference
            else:
                return error_message, {}
        for idx,feat in enumerate(features):
            feat.attributes['Status'] = status_types[status_sign_cnt[idx]]
        if trace_mode == "multi":
            update_result = building_layer.edit_features(updates=features)
            success_cnt = sum(int(elem['success']) for elem in update_result['updateResults']) # assumption: elem would always have key "success"
            fail_cnt = len(update_result['updateResults']) - success_cnt
            report = {'update_success_count':success_cnt,'update_fail_count':fail_cnt}
            print(report)
        elif trace_mode == "historical":
            if check_exist(date_val):
                return None, {"message":"already updated!"}
            else:
                write_json(date_val)
            historical_layer = arcgis.contentSearch(arcgis.arc_username,"historical_data_layer")[0].layers[0]
            add_result = arcgis.addToTable(historical_layer,features)
            success_cnt = sum(int(elem['success']) for elem in add_result['addResults']) # assumption: elem would always have key "success"
            fail_cnt = len(add_result['addResults']) - success_cnt
            report = {'add_success_count':success_cnt,'add_fail_count':fail_cnt}
            print(report)
        return None, report

if __name__ == "__main__":
    targets = sys.argv
    print(targets)
    if len(targets) > 1:
        error_message, affected_buildings = updateBuilding(targets[1])
    else:
        error_message, affected_buildings = updateBuilding('5/3/21')
    print(error_message,affected_buildings)