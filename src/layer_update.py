import os
from trace import autoPilot
from env_setup import getArcCredentials
from arcgis.gis import GIS
import sys

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


def updateBuilding(date_val):
    arcgis = ArcgisOperation()
    building_layer = arcgis.contentSearch(arcgis.arc_username,"TracedBuildings_oneday")[0].layers[1]
    features = arcgis.getFeatures(building_layer)
    # loop to update
    error_message, affected_buildings = autoPilot(date_val)
    affected_set = set(affected_buildings)
    if not error_message:
        for feat in features:
            binary_check =  "Yes" if feat.attributes['CAANtext'] in affected_set else "No"
            feat.attributes['PossibleSource'] = binary_check
            feat.attributes['CASE_DATE'] = date_val
        update_result = building_layer.edit_features(updates=features)
    return error_message, update_result

if __name__ == "__main__":
    targets = sys.argv
    print(targets)
    if len(targets) > 1:
        error_message, affected_buildings = updateBuilding(targets[1])
    else:
        error_message, affected_buildings = updateBuilding('2/17')
    print(error_message,affected_buildings)