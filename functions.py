import pandas as pd


def reset_index(data, name):
    # if the object we pass is already a dataframe we keep the index, else we reset the index.
    if isinstance(data, pd.DataFrame):
        data.name = name
    else:
        data = data.to_frame().reset_index()
        data.name = name
    return data


def rename_columns(data, name):
    rtt_columns = {
        'Provider Code': 'Grouping_2',
        'Total number of completed pathways (all)': 'Denominator',
        'Total number of incomplete pathways': 'Denominator',
        'Total within 18 weeks': 'Numerator',
        '52 plus': 'Numeric_Value',
    }
    cancer_62_columns = {
        'WITHIN 62 DAYS': 'Numerator',
        'TOTAL': 'Denominator',
        'ODS CODE (1)': 'Grouping_2',
    }
    cancer_31_columns = {
        'WITHIN 31 DAYS': 'Numerator',
        'TOTAL': 'Denominator',
        'ODS CODE (1)': 'Grouping_2',
    }
    cancer_14_columns = {
        'WITHIN 14 DAYS': 'Numerator',
        'TOTAL': 'Denominator',
        'ODS CODE (1)': 'Grouping_2',
    }
    ed_denominator_columns = {
        'Code': 'Grouping_2',
    }
    ed_numeric_columns = {
        'Code': 'Grouping_2',
    }

    dispatch = {
        "18AdmBench": rtt_columns,
        "ZeroRTTAPBench": rtt_columns,
        '18NonAdmBench': rtt_columns,
        'ZeroRTTNPBench': rtt_columns,
        '18IncompBench': rtt_columns,
        'ZeroRTTIPBench': rtt_columns,
        'CancerUrgBench': cancer_62_columns,
        'CanNatScr0Bench': cancer_62_columns,
        'CancerAll0Bench': cancer_31_columns,
        'CanSurg0Bench': cancer_31_columns,
        'Cancanti0Bench': cancer_31_columns,
        'CancerRad0Bench': cancer_31_columns,
        'CancUrgF0Bench': cancer_14_columns,
        'CancBreastBench': cancer_14_columns,
        'AESitrep4Bench': ed_denominator_columns,
        'AEAttendBench': ed_numeric_columns,

    }

    data.rename(columns=dict(zip(list(dispatch[name].keys()), dispatch[name].values())), inplace=True)
    return data


def group_by(data, name):
    def group_by_denominator(data):
        data = data.groupby(
            ['Grouping_2', 'Numerator', 'Append_Date', 'Indicator_ID', 'Data_Month', 'Section_Code', 'User_Name',
             'Submitted_By'])['Denominator'].sum()
        return data

    def group_by_numeric(data):
        data = data.groupby(
            ['Grouping_2', 'Append_Date', 'Indicator_ID', 'Data_Month', 'Section_Code', 'User_Name', 'Submitted_By'])[
            'Numeric_Value'].sum()
        return data

    def group_by_ed_denominator(data):
        data = data.groupby(
            ['Grouping_2', 'Grouping_3', 'Numerator', 'Append_Date', 'Indicator_ID', 'Data_Month', 'Section_Code',
             'User_Name', 'Submitted_By'])[
            'Denominator'].sum()
        return data

    def group_by_ed_numeric(data):
        data = data.groupby(
            ['Grouping_2', 'Grouping_3', 'Append_Date', 'Indicator_ID', 'Data_Month', 'Section_Code', 'User_Name',
             'Submitted_By'])['Numeric_Value'].sum()
        return data

    dispatch = {
        "18AdmBench": group_by_denominator,
        "ZeroRTTAPBench": group_by_numeric,
        '18NonAdmBench': group_by_denominator,
        'ZeroRTTNPBench': group_by_numeric,
        '18IncompBench': group_by_denominator,
        'ZeroRTTIPBench': group_by_numeric,
        'CancerUrgBench': group_by_denominator,
        'CanNatScr0Bench': group_by_denominator,
        'CancerAll0Bench': group_by_denominator,
        'CanSurg0Bench': group_by_denominator,
        'Cancanti0Bench': group_by_denominator,
        'CancerRad0Bench': group_by_denominator,
        'CancUrgF0Bench': group_by_denominator,
        'CancBreastBench': group_by_denominator,
        'AESitrep4Bench': group_by_ed_denominator,
        'AEAttendBench': group_by_ed_numeric,

    }
    data = dispatch[name](data)
    return data


def transform_data(data, name):
    def rtt_add_numerator(data):
        data['Numerator'] = data.values[:, 5:23].sum(axis=1)
        data = data[data['Treatment Function'].str.contains('Total', na=False)].copy()
        return data

    def rtt_treatment_function_total(data):
        data = data[data['Treatment Function'].str.contains('Total', na=False)].copy()
        return data

    def cancer_all_care(data):
        data = data[data['CARE SETTING (2)'] == "ALL CARE"].copy()
        return data

    def cancer_no_action(data):
        return data

    def create_AESitrep4Bench(data):
        data = data[data.Grouping_2 != "-"]
        t1 = data.copy()
        t1.rename(columns={'Type 1 Departments - Major A&E': 'Denominator'}, inplace=True)
        t1.rename(columns={'Type 1 Departments - Major A&E.1': 'Numerator'}, inplace=True)
        t1['Grouping_3'] = 'T1'
        t1['Numerator'].replace(['-'], '0', inplace=True)
        t1['Denominator'].replace(['-'], '0', inplace=True)
        t2 = data.copy()
        t2.rename(columns={'Type 2 Departments - Single Specialty': 'Denominator'}, inplace=True)
        t2.rename(columns={'Type 2 Departments - Single Specialty.1': 'Numerator'}, inplace=True)
        t2['Grouping_3'] = 'T2'
        t2['Numerator'].replace(['-'], '0', inplace=True),
        t2['Denominator'].replace(['-'], '0', inplace=True)
        t3 = data.copy()
        t3.rename(columns={'Type 3 Departments - Other A&E/Minor Injury Unit': 'Denominator'}, inplace=True)
        t3.rename(columns={'Type 3 Departments - Other A&E/Minor Injury Unit.1': 'Numerator'}, inplace=True)
        t3['Grouping_3'] = 'T3'
        t3['Numerator'].replace(['-'], '0', inplace=True)
        t3['Denominator'].replace(['-'], '0', inplace=True)
        frames = [t1, t2, t3]
        data = pd.concat(frames, sort=False)
        return data

    def create_AEAttendBench(data):
        data = data[data.Grouping_2 != "-"]
        t1 = data.copy()
        t1.rename(columns={'Type 1 Departments - Major A&E': 'Numeric_Value'}, inplace=True)
        t1['Grouping_3'] = 'T1'
        t2 = data.copy()
        t2.rename(columns={'Type 2 Departments - Single Specialty': 'Numeric_Value'}, inplace=True)
        t2['Grouping_3'] = 'T2'
        t3 = data.copy()
        t3.rename(columns={'Type 3 Departments - Other A&E/Minor Injury Unit': 'Numeric_Value'}, inplace=True)
        t3['Grouping_3'] = 'T3'
        frames = [t1, t2, t3]
        data = pd.concat(frames, sort=False)
        return data

    dispatch = {
        "18AdmBench": rtt_add_numerator,
        "ZeroRTTAPBench": rtt_treatment_function_total,
        '18NonAdmBench': rtt_add_numerator,
        'ZeroRTTNPBench': rtt_treatment_function_total,
        '18IncompBench': rtt_treatment_function_total,
        'ZeroRTTIPBench': rtt_treatment_function_total,
        'CancerUrgBench': cancer_all_care,
        'CanNatScr0Bench': cancer_all_care,
        'CancerAll0Bench': cancer_all_care,
        'CanSurg0Bench': cancer_all_care,
        'Cancanti0Bench': cancer_all_care,
        'CancerRad0Bench': cancer_all_care,
        'CancUrgF0Bench': cancer_no_action,
        'CancBreastBench': cancer_no_action,
        'AESitrep4Bench': create_AESitrep4Bench,
        'AEAttendBench': create_AEAttendBench,

    }
    data = dispatch[name](data)
    return data


def clean_data(data, name):
    # print("Renaming columns...")
    rename_columns(data, name)
    # print("Columns renamed, transforming data...")
    data = transform_data(data, name)
    # print("Data transformed, grouping data...")
    data = group_by(data, name)
    # print("Data grouped")
    return data
