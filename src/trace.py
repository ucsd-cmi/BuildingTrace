import pandas as pd 
import gspread 
from traceGraph import TraceGraph
from oauth2client.service_account import ServiceAccountCredentials
from pandas.io.json import json_normalize
import datetime
import sys
class TraceError(Exception):
    """Base class for other exceptions"""
    pass
class InvalidDateError(TraceError):
    """Raised when the input date is invalid"""
    pass

class Trace:
    def __init__(self):
        self.df = self.read_sheet()
        self.mh_graph = TraceGraph()
        self.mh_graph.buildGraph()
    def read_sheet(self):
        """
        return a panda dataframe of the newest spread sheet
        """
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('../.env/google_credentials.json', scope)
        gc = gspread.authorize(credentials)
        spreadsheet_key = '1mKOeKWf8f_mUmxbDQeHMA-P6lk6SfZf4Q9CRBH44EHU'
        book = gc.open_by_key(spreadsheet_key)
        worksheet = book.worksheet("Results_clean")
        table = worksheet.get_all_values()
        return pd.DataFrame(table[3:], columns=table[2])

    def get_manhole_map(self, date_value):
        """
        get a map of manholes to whether they are positive given a particular date,
        throw an InvalidDateError if the date is invalid
        """
        day_map = {}
        try:
            day_data = self.df[[date_value,"ManholeID"]]
        except:
            raise InvalidDateError

        for _,row in day_data.iterrows():
            try:
                day_map[row["ManholeID"]] = int(float(row[date_value]) > 0)
            except:
                day_map[row["ManholeID"]] = 0
        return day_map
    
    def get_positive_manholes(self, date_value):
        """
        get a list of positive manholes from a given date
        return error message if any
        """
        error_message,pos_mh_list = None,[]
        try:
            day_map = self.get_manhole_map(date_value)
            pos_mh_list = [mh for mh,val in day_map.items() if val > 0]
        except InvalidDateError:
            error_message = "Invalid date, please choose a date that exists in the wastewater sheet"
        return error_message, pos_mh_list

    def get_affected_buildings(self,date_value):
        """
        get a list of affected buildings
        return error message if any
        """
        affected_buildings = []
        error_message, pos_mh_list = self.get_positive_manholes(date_value)
        if error_message: return error_message, affected_buildings
        for mh_case in pos_mh_list:
            try:
                affected_buildings += list(self.mh_graph.trace_graph[mh_case])
            except KeyError:
                pass
        return error_message,list(set(affected_buildings))

    def exportDropIn(self,date_value):
        error,pos_manholes = self.get_positive_manholes(date_value)
        if error: return
        potential_affected_mhs = set(mh for elem in pos_manholes for mh in self.mh_graph.manhole_graph[elem]).union(set(pos_manholes))
        year = datetime.date.today().year
        waste_df = self.read_sheet()
        drop_in = waste_df[['SampleID','ManholeID','Building(s)']]
        drop_in['CQ'] = waste_df[date_value]
        date_str =  "/".join([date_value,str(year)])
        drop_in['TEST_DATE'] = date_str
        drop_in = drop_in[drop_in['ManholeID'].isin(potential_affected_mhs)]
        drop_in.columns = ['SAMPLE_ID','MANHOLE_ID','BUILDING','CQ','TEST_DATE']
        drop_in.to_csv('../data/dropin%s.csv'%("".join(date_value.split("/"))+str(year)),index=False)
        print("dropin csv has been saved to the tmp folder")
def autoPilot(date_value):
    tracing = Trace()
    return tracing.get_affected_buildings(date_value)
if __name__ == "__main__":
    targets = sys.argv
    if len(targets) > 1:
        error_message, affected_buildings = autoPilot(targets[1])
    else:
        error_message, affected_buildings = autoPilot('2/17')
    print(error_message,affected_buildings)