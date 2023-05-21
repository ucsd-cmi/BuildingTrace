import pandas as pd
import gspread
from traceGraph import TraceGraph
from oauth2client.service_account import ServiceAccountCredentials
from pandas.io.json import json_normalize
import requests
import json
from datetime import datetime, timedelta, date
import numpy as np
import sys
import os

DB_CLUSTER_IP = "10.8.4.97"

class TraceError(Exception):
    """Base class for other exceptions"""
    pass


class InvalidDateError(TraceError):
    """Raised when the input date is invalid"""
    pass


class Trace:
    def __init__(self, date_value):
        self.read_db(date_value)
        self.mh_graph = TraceGraph()
        self.residential_map = self.get_residential_map()
        self.manhole_caan_mapping = self.get_manhole_caan_map()
        self.manhole_residential_map = {manhole:sum(self.residential_map[caan] for caan in caans) > 0 for manhole, caans in self.manhole_caan_mapping.items()}
    
    def get_manhole_caan_map(self):
        ip = f'http://{DB_CLUSTER_IP}:8080/query'
        data_string = '{"query": "query getManholeCaanMappings {getManholeCaanMappings { manholeID internalCaan }}"}'
        r = requests.post(ip, data=data_string, headers={"Content-Type":"application/json"})
        r_json = r.json()
        manhole_map = {elem['manholeID']:set(elem['internalCaan']) for elem in r_json['data']['getManholeCaanMappings']}
        return manhole_map

    def get_residential_map(self):
        ip = f'http://{DB_CLUSTER_IP}:8080/query'
        data_string = '{"query": "query getBuildingInfo {getBuildingInfo { internalCaan isResidential }}"}'
        r = requests.post(ip, data=data_string, headers={"Content-Type":"application/json"})
        r_json = r.json()
        return {elem['internalCaan']:elem['isResidential'] for elem in r_json['data']['getBuildingInfo']}

    def read_sheet(self, tab_name="Results_for_test"):
        """
        return a panda dataframe of the newest spread sheet
        """
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            '../.env/google_credentials.json', scope)
        gc = gspread.authorize(credentials)
        spreadsheet_key = '1mKOeKWf8f_mUmxbDQeHMA-P6lk6SfZf4Q9CRBH44EHU'
        book = gc.open_by_key(spreadsheet_key)
        worksheet = book.worksheet(tab_name)
        table = worksheet.get_all_values()
        self.df = pd.DataFrame(table[3:], columns=table[2])
    
    def read_db(self, date_value):
        ip = f'http://{DB_CLUSTER_IP}:8080/query'
        date_formatted = datetime.strptime(date_value, "%m/%d/%y").isoformat() + "Z"
        data_string = '{"query": "query getQpcrCqs($startDate: Time!, $endDate: Time!) { getQpcrCqs(startDate: $startDate, endDate: $endDate) { date manholeID samplerID cqValue } }", "variables": {"startDate": "' + date_formatted + '", "endDate": "' + date_formatted + '"}}'
        # Exception will be thrown if the request failed
        r = requests.post(ip, data=data_string, headers={"Content-Type":"application/json"})
        r.raise_for_status()
        r_json = r.json()
        r_json = r_json['data']['getQpcrCqs']
        db_df = pd.DataFrame.from_dict(r_json)
        try:
            dates = db_df['date'].unique()
        except KeyError:
            raise InvalidDateError
        df = pd.pivot_table(db_df,index=['manholeID'], columns='date',values='cqValue', fill_value=0)
        df.columns = [datetime.fromisoformat(i.replace("Z", "+00:00")).strftime("%-m/%-d/%y") for i in df.columns]
        df = df.reset_index()
        df.rename(columns = {"manholeID": "ManholeID"}, inplace=True)
        
        self.df = df

    def getPositivityCounts(self, day):
        try:
            self.read_db(day)
        except requests.exceptions.RequestException:
            return "failed to read from DB", {}
        except InvalidDateError:
            return "invalid date", {"r_total_cnt": 0, "nr_total_cnt":0, "r_pos_cnt":0, "nr_pos_cnt":0, "total_cnt":0, "total_pos_cnt":0}

        mh_map = self.get_manhole_map(day)
        total_cnt = len(mh_map)
        total_pos_cnt = sum([mh_map[val] > 0 for val in mh_map])
        r_total_cnt = sum([self.manhole_residential_map[val] for val in mh_map])
        nr_total_cnt = sum([not self.manhole_residential_map[val] for val in mh_map])
        r_pos_cnt = sum([(mh_map[val] > 0) and (self.manhole_residential_map[val]) for val in mh_map])
        nr_pos_cnt = sum([(mh_map[val] > 0) and (not self.manhole_residential_map[val]) for val in mh_map])
        return None, {"r_total_cnt": r_total_cnt, "nr_total_cnt":nr_total_cnt, "r_pos_cnt":r_pos_cnt, "nr_pos_cnt":nr_pos_cnt, "total_cnt":total_cnt, "total_pos_cnt":total_pos_cnt}

    def getMovingAverage(self, day):
        result = {
        "7-day total positivity rate avg": -1, 
        "7-day residential positivity rate avg": -1, 
        "7-day non-residential positivity rate avg": -1,
        "total positivity rate": -1, 
        "residential positivity rate": -1, 
        "non-residential positivity rate": -1}
        
        #Calculate 7 day averages.
        cnts = []
        start_date = datetime.strptime(day, "%m/%d/%y")
        counter = 0
        success_counter = 0
        while success_counter < 7 :
            current_date = start_date - timedelta(days=counter)
            current_day_string = current_date.strftime("%-m/%-d/%y")
            error_message, current_day_stats = self.getPositivityCounts(current_day_string)
            counter += 1
            if error_message:
                if error_message == "invalid date":
                    continue
                else:
                    return error_message, result
            cnts.append(current_day_stats)
            success_counter += 1
        
        result["non-residential positivity rate"] = '{:.2f}%'.format((cnts[0]["nr_pos_cnt"]/cnts[0]["nr_total_cnt"])*100) if cnts[0]["nr_total_cnt"] > 0 else "N/A"
        result["residential positivity rate"] = '{:.2f}%'.format((cnts[0]["r_pos_cnt"]/cnts[0]["r_total_cnt"])*100) if cnts[0]["r_total_cnt"] > 0 else "N/A"
        result["total positivity rate"] = '{:.2f}%'.format((cnts[0]["total_pos_cnt"]/cnts[0]["total_cnt"])*100) if cnts[0]["total_cnt"] > 0 else "N/A"
        
        
        total_pos_case_7 = sum([cnt["total_pos_cnt"] for cnt in cnts])
        total_r_pos_case_7 = sum([cnt["r_pos_cnt"] for cnt in cnts])
        total_nr_pos_case_7 = sum([cnt["nr_pos_cnt"] for cnt in cnts])
        total_case_7 = sum([cnt["total_cnt"] for cnt in cnts])
        total_r_case_7 = sum([cnt["r_total_cnt"] for cnt in cnts])
        total_nr_case_7 = sum([cnt["nr_total_cnt"] for cnt in cnts])
        result["7-day non-residential positivity rate avg"] = '{:.2f}%'.format((total_nr_pos_case_7/total_nr_case_7)*100) if total_nr_case_7 > 0 else "N/A"
        result["7-day residential positivity rate avg"] = '{:.2f}%'.format((total_r_pos_case_7/total_r_case_7)*100) if total_r_case_7 > 0 else "N/A"
        result["7-day total positivity rate avg"] = '{:.2f}%'.format((total_pos_case_7/total_case_7)*100) if total_case_7 > 0 else "N/A"
        return None, result

    def get_manhole_map(self, date_value, mode="detection"):
        """
        get a map of manholes to whether they are positive given a particular date,
        throw an InvalidDateError if the date is invalid

        three modes:
        detection, monitoring, sampling
        """
        day_map = {}
        try:
            day_data = self.df[[date_value, "ManholeID"]]
        except:
            raise InvalidDateError
        empty_count = 0
        for _, row in day_data.iterrows():
            try:
                # if sampler/monitor mode, then treat samplers as positive if they are negative
                sm_tag = -1 if mode == "detection" else 1
                day_map[row["ManholeID"]] = int(
                    float(row[date_value]) > 0) or sm_tag
            except:
                empty_count += 1
                # if monitoring or sampling mode => treat all samplers as positive
                if mode == "monitoring":
                    day_map[row["ManholeID"]] = 1
                else:
                    day_map[row["ManholeID"]] = 0
        # if the current sheet doesn't contain the data, raise error
        if empty_count == len(day_data):
            raise InvalidDateError
        return day_map

    def get_positive_manholes(self, date_value, mode="detection"):
        """
        get a list of positive manholes from a given date
        return error message if any
        """
        error_message, pos_mh_list = None, []
        try:
            day_map = self.get_manhole_map(date_value, mode)
            pos_mh_list = [mh for mh, val in day_map.items() if val > 0]
        except InvalidDateError:
            error_message = "Invalid date, please choose a date that exists in the wastewater sheet"
        return error_message, pos_mh_list

    def get_negative_barriers(self, date_value, mode="detection"):
        """
        get a list of barriers for stop condition of graph traversal
        return error message if any
        """
        error_message, barriers = None, None
        try:
            day_map = self.get_manhole_map(date_value, mode)
            barriers = set(mh for mh, val in day_map.items() if val < 0)
        except InvalidDateError:
            error_message = "Invalid date, please choose a date that exists in the wastewater sheet"
        return error_message, barriers

    def get_paused_manholes(self):
        # now it is fall quarter. TODO: might need to do this programmatically
        paused_manholes_in_float = []
        return set(paused_manholes_in_float)

    def get_affected_buildings(self, date_value, mode="detection"):
        """
        get a list of affected buildings
        return error message if any
        """
        affected_buildings = []
        if mode == "paused monitoring":
            error_message = None
            pos_mh_list = self.get_paused_manholes()
        else:
            error_message, pos_mh_list = self.get_positive_manholes(
                date_value, mode)
        if error_message:
            return error_message, affected_buildings
        error_message, barriers = self.get_negative_barriers(date_value, mode)
        if error_message:
            return error_message, affected_buildings
        print("barriers, ", barriers)
        self.mh_graph.barriers = barriers
        if mode != "paused monitoring":
            self.mh_graph.barriers = self.mh_graph.barriers.union(
                self.get_paused_manholes())
        self.mh_graph.buildGraph()
        for mh_case in pos_mh_list:
            try:
                affected_buildings += list(self.mh_graph.trace_graph[mh_case])
            except KeyError:
                pass
        return error_message, list(set(affected_buildings))

    def get_affected_manholes(self, date_value, mode="detection"):
        """
        get a list of affected manholes
        return error message if any
        """
        affected_manholes = []
        error_message, pos_mh_list = self.get_positive_manholes(
            date_value, mode)
        if error_message:
            return error_message, affected_manholes
        error_message, barriers = self.get_negative_barriers(date_value, mode)
        if error_message:
            return error_message, affected_manholes
        self.mh_graph.barriers = barriers.union(self.get_paused_manholes())
        self.mh_graph.buildGraph()
        for mh_case in pos_mh_list:
            try:
                affected_manholes += list(self.mh_graph.manhole_graph[mh_case])
            except KeyError:
                pass
        return error_message, list(set(affected_manholes+pos_mh_list))

    def exportDropIn(self, date_value):
        year = date.today().year
        saved_path = '/tmp/dropin%s.csv' % (
            "".join(date_value.split("/"))+str(year))
        error_message, barriers = self.get_negative_barriers(date_value)
        if error_message:
            return error_message, saved_path
        self.mh_graph.barriers = barriers
        self.mh_graph.buildGraph()
        waste_df = self.read_sheet()
        drop_in = waste_df[['SamplerID', 'ManholeID', 'Building(s)', 'Area', 'Residential']]
        drop_in.columns = ['SAMPLE_ID', 'MANHOLE_ID', 'BUILDING', 'AREA', 'RESIDENTIAL']
        _, manhole_trace_list = self.MultiTraceManholes(date_value)
        full_mh_trace_df = pd.DataFrame(manhole_trace_list)
        full_mh_trace_df['TEST_DATE'] = date_value
        drop_in = pd.merge(full_mh_trace_df, drop_in,
                           on='MANHOLE_ID', how='left')
        drop_in.to_csv(saved_path, index=False)
        print("dropin csv has been saved to the tmp folder")
        return None, saved_path

    def getCQManholeMap(self, date_val):
        waste_df = self.read_sheet()
        manhole_ids = list(waste_df['ManholeID'])
        manhole_cqs = list(waste_df[date_val])
        manhole_cq_map = dict(zip(manhole_ids, manhole_cqs))
        return manhole_cq_map

    def MultiTraceManholes(self, date_val):
        mode_col_map = {'detection': 'Detection',
                        'monitoring': 'Monitoring', 'sampling': 'Sampling'}
        status_types = ["Not Currently Monitored", "Currently Monitored + Not Sampled",
                        "Currently Monitored + Sampled + Not Detected", "Currently Monitored + Sampled + Detected"]
        self.mh_graph.buildGraph()
        status_sign_cnt = [0]*len(self.mh_graph.manhole_graph)
        manhole_ids = list(self.mh_graph.manhole_graph.keys())
        manhole_cq_map = self.getCQManholeMap(date_val)
        for mode, _ in mode_col_map.items():
            error_message, affected_manholes = self.get_affected_manholes(
                date_val, mode=mode)
            affected_set = set(affected_manholes)
            if not error_message:
                for idx, manhole_id in enumerate(manhole_ids):
                    binary_check = "Yes" if manhole_id in affected_set else "No"
                    if binary_check == "Yes":
                        status_sign_cnt[idx] += 1
            else:
                return error_message, {}
        result = []
        for idx, manhole_id in enumerate(manhole_ids):
            info_json = {}
            info_json['MANHOLE_ID'] = manhole_id
            info_json['STATUS'] = status_types[status_sign_cnt[idx]]
            info_json['CQ'] = manhole_cq_map.get(manhole_id, "")
            result.append(info_json)
        print(result)
        return None, result


def autoPilotManhole(date_value):
    tracing = Trace()
    return tracing.MultiTraceManholes(date_value)


def autoPilot(date_value, drop=False, mode="detection"):
    tracing = Trace(date_value)
    if drop:
        error_message, path = tracing.exportDropIn(date_value)
        if error_message:
            return error_message, None
        with open(path, 'r') as file:
            message = file.read()
            print(message)
        try:
            os.remove(path)
            print("% s removed successfully" % path)
        except OSError as error:
            print(error)
            print("File path can not be removed")
            error_message = "File path can not be removed"
        return error_message, message
    return tracing.get_affected_buildings(date_value, mode)

def traceStats(date):
    tracing = Trace(date)
    return tracing.getMovingAverage(date)

if __name__ == "__main__":
    targets = sys.argv
    if len(targets) == 2:
        print(traceStats("3/20/23"))
    elif len(targets) == 3:
        error_message, affected_buildings = autoPilot(targets[1], False)
        print(affected_buildings)
    else:
        error_message, affected_buildings = autoPilotManhole('6/7/21')
