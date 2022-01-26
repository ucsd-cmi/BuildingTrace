import pandas as pd
import gspread
from traceGraph import TraceGraph
from oauth2client.service_account import ServiceAccountCredentials
from pandas.io.json import json_normalize
import datetime
import sys
import os
from drop_in import drop_in


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

    def read_sheet(self):
        """
        return a panda dataframe of the newest spread sheet
        """
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            '../.env/google_credentials.json', scope)
        gc = gspread.authorize(credentials)
        spreadsheet_key = '1mKOeKWf8f_mUmxbDQeHMA-P6lk6SfZf4Q9CRBH44EHU'
        book = gc.open_by_key(spreadsheet_key)
        worksheet = book.worksheet("Results_for_test")
        table = worksheet.get_all_values()
        return pd.DataFrame(table[3:], columns=table[2])

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
        print(barriers)
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
        year = datetime.date.today().year
        saved_path = '/tmp/dropin%s.csv' % (
            "".join(date_value.split("/"))+str(year))
        error_message, barriers = self.get_negative_barriers(date_value)
        if error_message:
            return error_message, saved_path
        self.mh_graph.barriers = barriers
        self.mh_graph.buildGraph()
        waste_df = self.read_sheet()
        drop_in = waste_df[['SampleID', 'ManholeID', 'Building(s)']]
        drop_in.columns = ['SAMPLE_ID', 'MANHOLE_ID', 'BUILDING']
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
    tracing = Trace()
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


if __name__ == "__main__":
    targets = sys.argv
    if len(targets) == 2:
        error_message, affected_buildings = autoPilot(targets[1])
    elif len(targets) == 3:
        error_message, affected_buildings = autoPilot(targets[1], True)
    else:
        error_message, affected_buildings = autoPilotManhole('6/7/21')
    print(error_message, affected_buildings)
