import bs4
import re
import pandas as pd
import numpy as np
import httplib2
from functions import clean_data, reset_index
import time
from datetime import datetime
import urllib.request as urllib

start_time = time.time()

# Show all columns in the dataframe
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 100)
# Establishes 'the http.' prefix as a web connector.
http = httplib2.Http()

financial_years = ['2019-20']
# , '2020-21']
months = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6, 'July': 7,
          'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

links = {}
reports = {}

# keys - websites with requisite download links
for year in financial_years:
    rtt = f"https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-{year}"
    for month in months:
        admitted = rf"^(https?://)?(www\.)?england.nhs.uk/statistics/wp-content/uploads/sites/2/.*/" \
                   rf"Admitted-Provider-{month[:3]}"
        nonadmitted = rf"^(https?://)?(www\.)?england.nhs.uk/statistics/wp-content/uploads/sites/2/.*/" \
                      rf"NonAdmitted-Provider-{month[:3]}"
        incomplete = rf"^(https?://)?(www\.)?england.nhs.uk/statistics/wp-content/uploads/sites/2/.*/" \
                     rf"Incomplete-Provider-{month[:3]}"

        links[rtt] = links.get(rtt, []) + [admitted, nonadmitted, incomplete]

        # dictionary - for each download link we pull the data for multiple indicators ("reports")
        reports_new = {admitted: ["18AdmBench", "ZeroRTTAPBench"],
                       nonadmitted: ["18NonAdmBench", "ZeroRTTNPBench"],
                       incomplete: ["18IncompBench", "ZeroRTTIPBench"], }

        reports.update(reports_new)

    edwta = "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/" \
            f"ae-attendances-and-emergency-admissions-{year}/"
    for month in months:
        ed = rf"^(https?://)?(www\.)?england.nhs.uk/statistics/wp-content/uploads/sites/2/.*/{month}.*.xls"

        links[edwta] = links.get(edwta, []) + [ed]

        reports_new = {ed: ["AESitrep4Bench", "AEAttendBench"], }
        reports.update(reports_new)

    for month in months:
        if month == 'January' or month == 'February' or month == 'March':
            cancerwt = f"https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/" \
                       f"monthly-prov-cwt/{year}-monthly-provider-cancer-waiting-times-statistics/provider-based-" \
                       f"cancer-waiting-times-for-{month}-20{year[-2:]}-provisional/"
            cancer = rf"^(https?://)?(www\.)?england.nhs.uk/statistics/wp-content/uploads/sites/2/" \
                     rf".*{month.upper()}.*.xls.*"
            links[cancerwt] = links.get(cancerwt, []) + [cancer]
        else:
            cancerwt2 = f"https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/monthly-" \
                        f"prov-cwt/{year}-monthly-provider-cancer-waiting-times-statistics/provider-based-cancer-" \
                        f"waiting-times-for-{month}-{year[:4]}-provisional/"
            cancer = rf"^(https?://)?(www\.)?england.nhs.uk/statistics/wp-content/uploads/sites/2/.*{month.upper()}" \
                     rf".*.xls.*"
            links[cancerwt2] = links.get(cancerwt2, []) + [cancer]

        reports_new = {
            cancer: ["CancerUrgBench", "CanNatScr0Bench", "CancerAll0Bench", "CanSurg0Bench", "Cancanti0Bench",
                     "CancerRad0Bench", "CancUrgF0Bench", "CancBreastBench"], }
        reports.update(reports_new)

# as each download is different, we pass the param_select values when converting data from csv to frame
param_select = {"18AdmBench": {"delim_whitespace": True, "sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                               "header": 0},
                "ZeroRTTAPBench": {"delim_whitespace": True, "sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                   "header": 0},
                "18NonAdmBench": {"delim_whitespace": True, "sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                  "header": 0},
                "ZeroRTTNPBench": {"delim_whitespace": True, "sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                   "header": 0},
                "18IncompBench": {"delim_whitespace": True, "sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                  "header": 0},
                "ZeroRTTIPBench": {"delim_whitespace": True, "sheet_name": 'Provider', "skiprows": 13, "index_col": 0,
                                   "header": 0},
                "CancerUrgBench": {"delim_whitespace": True, "sheet_name": '62-DAY (ALL CANCER)', "skiprows": 9,
                                   "index": 0,
                                   "header": 0},
                "CanNatScr0Bench": {"delim_whitespace": True, "sheet_name": '62-DAY (SCREENING)', "skiprows": 9,
                                    "index": 0, "header": 0},
                "CancerAll0Bench": {"delim_whitespace": True, "sheet_name": '31-DAY FIRST TREAT (ALL CANCER)',
                                    "skiprows": 9, "index": 0, "header": 0},
                "CanSurg0Bench": {"delim_whitespace": True, "sheet_name": '31-DAY SUB TREAT (SURGERY)', "skiprows": 9,
                                  "index": 0, "header": 0},
                "Cancanti0Bench": {"delim_whitespace": True, "sheet_name": '31-DAY SUB TREAT (DRUGS)', "skiprows": 9,
                                   "index": 0, "header": 0},
                "CancerRad0Bench": {"delim_whitespace": True, "sheet_name": '31-DAY SUB TREAT (RADIOTHERAPY)',
                                    "skiprows": 9, "index": 0, "header": 0},
                "CancUrgF0Bench": {"delim_whitespace": True, "sheet_name": 'TWO WEEK WAIT-ALL CANCER', "skiprows": 6,
                                   "index": 0, "header": 0},
                "CancBreastBench": {"delim_whitespace": True, "sheet_name": 'TWO WEEK WAIT-BREAST SYMPTOMS',
                                    "skiprows": 8, "index": 0, "header": 0},
                "AESitrep4Bench": {"delim_whitespace": True, "sheet_name": 'Provider Level Data', "skiprows": 15,
                                   "index": 0, "header": 0},
                "AEAttendBench": {"delim_whitespace": True, "sheet_name": 'Provider Level Data', "skiprows": 15,
                                  "index": 0, "header": 0},
                }


def download_bench_data():
    to_download = []
    file_name = []
    target = []
    for k in links:
        for x in links[k]:
            status, response = http.request(k)
            for link in bs4.BeautifulSoup(response, 'html.parser', parse_only=bs4.SoupStrainer('a', href=True)) \
                    .find_all(attrs={'href': re.compile(str(x))}, limit=1):
                for y in reports[x]:
                    print("FOUND: " + link.get('href') + " (" + y + ")")
                    if link.get('href') not in to_download:
                        to_download.append(link.get('href'))
                    file_name.append(link.get('href').rsplit('/', 1)[-1])
                    target.append(y)

    for url_link in to_download:
        urllib.urlretrieve(url_link, f"data/{url_link.rsplit('/', 1)[-1]}")

    df_list = pd.DataFrame(list(zip(file_name, target)), columns=['file_name', 'target'])
    df_list.to_csv("data/list.csv")
    return "Finished Downloading Files"


print(download_bench_data())

frame = pd.DataFrame(columns=['Append_Date', 'Indicator_ID', 'Data_Month', 'Section_Code', 'org', 'Grouping_2',
                              'Grouping_3', 'User_Name', 'Submitted_By', 'Numerator', 'Denominator', 'Numeric_Value'])


def calc_period(target):
    dispatch = {
        "18AdmBench": 'C4',
        "ZeroRTTAPBench": 'C4',
        "18NonAdmBench": 'C4',
        "ZeroRTTNPBench": 'C4',
        "18IncompBench": 'C4',
        "ZeroRTTIPBench": 'C4',
        "AESitrep4Bench": 'C5',
        "AEAttendBench": 'C5',
        "CancerUrgBench": 'A2',
        "CanNatScr0Bench": 'A2',
        "CancerAll0Bench": 'A2',
        "CanSurg0Bench": 'A2',
        "Cancanti0Bench": 'A2',
        "CancerRad0Bench": 'A2',
        "CancUrgF0Bench": 'A2',
        "CancBreastBench": 'A2',
    }
    cell_ref = dispatch[target]
    return cell_ref


# print(calc_period(master, "ZeroRTTIPBench")[:1])
# print(calc_period(master, "ZeroRTTIPBench")[1:])


def etl_data(master):
    lookup = pd.read_csv(r"data\list.csv")
    for row in lookup.itertuples():
        print(getattr(row, "file_name"), getattr(row, "target"))
        data = pd.read_excel(f"data/{getattr(row, 'file_name')}", **param_select.get(getattr(row, "target")))

        date_cell_ref = calc_period(getattr(row, "target"))
        data_month = pd.read_excel(f"data/{getattr(row, 'file_name')}", index_col=None, usecols=date_cell_ref[:1],
                                   header=int(date_cell_ref[1:]), nrows=0)
        data_month = data_month.columns.values[0]
        data_month = pd.to_datetime(data_month)
        period = data_month

        # create new columns with dynamic data
        new_columns = {'Append_Date': datetime.now(), 'Indicator_ID': getattr(row, "target"), 'User_Name': 'Gareth',
                       'Submitted_By': 'Gareth', 'Section_Code': 0,
                       'Data_Month': period}
        # for each column abote, create the column in the dataframe - get values from the dictionary
        for i in new_columns:
            data[i] = new_columns.get(i)
        # pass the defined functions to clean the data and then reset the index if required before appending
        # to the empty master dataframe
        data = clean_data(data, getattr(row, "target"))
        data = reset_index(data, getattr(row, "target"))
        master = master.append(data, ignore_index=True, sort=False)
    master.to_excel("data/master.xlsx")
    return master


print(etl_data(frame))

print(f"Program took {time.time() - start_time} s to run")
