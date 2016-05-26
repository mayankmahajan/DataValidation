#!/usr/bin/python

from __future__ import with_statement   # For jython compatibility
import os
import re
import subprocess
import sys
import platform as py_platform
import ConfigParser
import xml.etree.cElementTree as eTree

def csv_diff(csv1_file, csv2_file, delimiter=",", error_percent_plus=1, error_percent_minus=1, key_columns=[1]):
    #   Calculates the diff between any two CSVs, based on their values
    diff_str = ""
    # Load the first file in a dict
    csv1_dict = {}

    ##### ACUME CSV ######
    with open(csv1_file, "rU") as csv1_fd:
        for line in csv1_fd:
            line = line.strip()
            fields = line.split(delimiter)
            if "itrate" in line or "rowth" in line  or "imestamp" in line or  "Total" in line or "Traffic" in line or "(bps)" in line or  not line:
                continue
            elif line.startswith("All") or line.startswith("Total"):
                key = "TOTAL_ROW"
            else:
                key = tuple([fields[kc-1].upper() for kc in key_columns])

            values = [str(val) for idx, val in enumerate(fields) if idx not in key_columns]
            csv1_dict[key] = (values, line)

    # Read the second file and start comparing with the first one
    ##### UI CSV #####
    with open(csv2_file, "rU") as csv2_fd:
        for line in csv2_fd:
            line = line.strip()
            fields = line.split(delimiter)
            if "itrate" in line or '%' in line or "rowth" in line or "imestamp" in line or  "Total" in line or "Traffic" in line or "(bps)" in line or not line:
                continue
            elif line.startswith("All") or line.startswith("Total"):
                key = "TOTAL_ROW"
            else:
                key = tuple([fields[kc-1].upper() for kc in key_columns])

            csv2_values = []

            for idx, val in enumerate(fields):
                if idx not in key_columns :
                    if '%' not in val :
                        csv2_values.append((val))
                    else:
                        csv2_values.append(str("NA"))
            try:
                csv1_values, csv1_line = csv1_dict[key]
                del csv1_dict[key]  # If the values are found, remove this key. The remaining values will be displayed later
            except KeyError:
                diff_str += "+%s\n" % line    # Key is present in csv2, but not in csv1
                continue

            for csv1_val, csv2_val in zip(csv1_values, csv2_values):
                try:
                    csv1_val = float(csv1_val)
                    csv2_val = float(csv2_val)
                except:
                    if csv1_val == csv2_val:
                        difference = 0
                        continue
                    else:
                        difference = 100
                        continue
		if type(csv1_val) is str or type(csv2_val) is str:
                                continue
                else:
                    if float(csv1_val) != 0 :
                        difference = (float(csv1_val) - float(csv2_val)) * 100 / float(csv1_val)
                    elif float(csv2_val) != 0 :
                        difference = (float(csv1_val) - float(csv2_val)) * 100 / float(csv2_val)
                    else:
                        difference = 0

                if (difference > 0 and difference > error_percent_plus) or \
                    (difference < 0 and abs(difference) > error_percent_minus):
                    # Difference in the values is greater than allowed difference
                    diff_str += "<%s\n" % csv1_line
                    diff_str += ">%s\n" % line
                    break

    # Display the lines present in CSV1 but not present in CSV2
    for k, v in csv1_dict.iteritems():
       	diff_str += "-%s\n" % v[1]
    return diff_str

if __name__ == '__main__':
    # csv1 = 'destsiteid_destsitetypeid=1;sourcesitetypeid=1;sourcesiteid=18/PEAK_uplinkbitbuffer_PEAK_uplinkbytebuffer.csv'
    # csv2 = 'Results_2016-01-14T05-44-38.csv'


    from datetime import datetime
    from dateutil import tz
    import time

    cstTZ = tz.gettz('CST6CDT')
    utcTZ = tz.gettz('UTC')
    epoch = 1451196000

    date = datetime.utcfromtimestamp(epoch).replace(tzinfo=utcTZ).astimezone(cstTZ)
    date.strftime("%b %d %a %Y %H:%M %Z")


    parquetCSVs = '/Users/mayank.mahajan/rec_f_nrmca_60min_3600_siteflowdatacube_24Dec_0500_CST'
    uiCSVs = '/Users/mayank.mahajan/Downloads/'
    peakUplink1 = parquetCSVs+'PEAK_1455075948.741335116199.csv'
    peakUplink2 = uiCSVs+'Peak_Uplink.csv'



    # diff_str  = csv_diff('/Users/mayank.mahajan/PEAK_downlink_1455681441.3353475398151.csv','/Users/mayank.mahajan/Downloads/Results_2016-02-17T04-02-05.csv',error_percent_plus=5,error_percent_minus=2)
    # diff_str  = csv_diff('/Users/mayank.mahajan/PEAK_downlink_1455680561.4770931839874.csv','/Users/mayank.mahajan/Downloads/Results_2016-02-17T03-42-03.csv',error_percent_plus=5,error_percent_minus=5)
    diff_str  = csv_diff('/Users/mayank.mahajan/PEAK_downlink_1455678807.754437833988.csv','/Users/mayank.mahajan/Downloads/Results_2016-02-17T02-52-47.csv',error_percent_plus=7,error_percent_minus=5)
    print diff_str