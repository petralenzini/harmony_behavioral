import os
import pandas as pd
import numpy as np
from functions import *

rmstructure = ['fmriresults01', 'imagingcollection01', 'image03']

# root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
root_dir = "/Users/yuanyuanxiaowang/PycharmProjects/pythonProject/Harmony/harmony_behavioral/Harmony_non_imaging_data_sharing"

ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
DAM_dir = os.path.join(root_dir, "ANXPE_Sheline")
MDD_dir = os.path.join(root_dir, "MDD_Narr")
# ADA
mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']
droplist = ['collection_id', 'collection_title', 'dataset_id']

# get list of files to process
ADA_files = os.listdir(ADA_dir)  # txt
ADA_files = [file for file in ADA_files if 'pdf' not in file
             and ('dataset_collection' not in file)
             and ('package_info' not in file)
             and ('md5_values' not in file)
             and ('fmriresults01' not in file)
             and ('imagingcollection01' not in file)]
if '.DS_Store' in ADA_files:
    ADA_files.remove('.DS_Store')

# process
merged_df = pd.DataFrame(columns=mergelist)
AllADA = pd.DataFrame(columns=['element', 'variable', 'structure'])
for file in ADA_files:
    prefix = os.path.splitext(file)[0]
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    df = df.drop(columns=droplist).copy()
    print(file)
    # some have the structure id housekeeping variable:
    try:
        df = df.drop(columns=[prefix + '_id']).copy()
    except:
        print(prefix + "_id NOT FOUND")
    try:
        df = df.iloc[1:]
        # find columns that are all 999 or nan or combo of the two and remove
        df = drop999cols(df, verbose=True)
        # rename the variables and store the map
        map, df, AllADA = partialcrosswalkmap(mergelist, df, prefix, AllADA)
        # clean up empty rows
        df = droprows(df, mergelist)
        print(prefix, "final :", df.shape)
        if merged_df.empty:
            merged_df = df
            print("*********")
        else:
            merged_df = pd.merge(merged_df, df, on=mergelist, how='outer')
            print("Merged:", merged_df.shape)
            print("*********")
    except:
        pass
if not merged_df.empty:
    merged_df['study'] = 'ADA'

# reorder
cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_df if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_df = merged_df[cols]

#############
# DAM
DAM_files = os.listdir(DAM_dir)
DAM_files = [file for file in DAM_files if "2019.02.27_FullDataSet_RedCap_824132.csv" not in file
             and ('xlsx' not in file)
             and ('docx' not in file)
             and ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' not in file)
             and ('u01_neuropsych_summary_v1.csv' not in file)]
if '.DS_Store' in DAM_files:
    DAM_files.remove('.DS_Store')

AllVDAM = pd.DataFrame(columns=['element', 'variable', 'structure'])
mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']
droplist = ['collection_id', 'collection_title']
merged_dfDAM = pd.DataFrame(columns=['src_subject_id', 'subjectkey'])
for file in DAM_files:
    prefix = os.path.splitext(file)[0]
    df = pd.read_csv(os.path.join(DAM_dir, file), header=1, dtype=str)
    df = drop999cols(df, verbose=True)
    try:
        map, df, AllVDAM = partialcrosswalkmap(mergelist, df, prefix, AllVDAM)
        df = droprows(df, mergelist)
        print(prefix, " final :", df.shape)
        if merged_dfDAM.empty:
            merged_dfDAM = df
            print("#########")
        else:
            merged_dfDAM = pd.merge(merged_dfDAM, df, on=mergelist, how='outer')
            print("Merged:", merged_dfDAM.shape)
            print("#########")
    except:
        pass

if not merged_dfDAM.empty:
    merged_dfDAM['study'] = 'DAM'

# reorder cols
cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_dfDAM if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_dfDAM = merged_dfDAM[cols]

#################
# MDD
MDD_files = os.listdir(MDD_dir)
MDD_files = [file for file in MDD_files if "definitions" not in file
             and ('xlsx' not in file)
             and ('docx' not in file)
             and ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' not in file)
             and ('u01_neuropsych_summary_v1.csv' not in file)]
if '.DS_Store' in MDD_files:
    MDD_files.remove('.DS_Store')

AllVMDD = pd.DataFrame(columns=['element', 'variable', 'structure'])
mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']
droplist = ['collection_id', 'collection_title']
merged_dfMDD = pd.DataFrame(columns=['src_subject_key', 'subjectkey'])

for file in MDD_files:
    prefix = os.path.splitext(file)[0]
    print("processing ", prefix)
    if 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=';', header=1, dtype=str)
    else:
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
    if 'gender' in df.columns:
        df['sex'] = df['gender']
        df = df.drop(columns='gender')
    df = drop999cols(df, verbose=True)
    map, df, AllVMDD = partialcrosswalkmap(mergelist, df, prefix, AllVMDD)
    # fix the cases where interview dates are same but ages change.
    df = MDDdupfix(df)
    # drop empties and duplicates
    df, extra = droprows(df, mergelist)
    # track ids that are different betweeen subjcts
    set1 = set(df['subjectkey'])
    set2 = set(merged_dfMDD['subjectkey'])
    print('number of different subjectkey :', len(set1 ^ set2))
    print("if still duplicated, check:")
    duplicate_rows_df = df[df[mergelist + extra].duplicated(keep=False)]
    print('duplicate', prefix, ":", duplicate_rows_df.shape)  # ,duplicate_rows_df)
    print(prefix, "final :", df.shape)
    if merged_dfMDD.empty:
        merged_dfMDD = df
        print("Merged:", merged_dfMDD.shape)
        print("************")
    else:
        merged_dfMDD = pd.merge(merged_dfMDD, df, on=mergelist, how='outer')
        print("Merged:", merged_dfMDD.shape)
        print("************")

if not merged_dfMDD.empty:
    merged_dfMDD['study'] = 'MDD'

# merged_dfMDD['sex'] = merged_dfMDD['gender']
# merged_dfMDD = merged_dfMDD.drop(columns='gender')
cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_dfMDD if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_dfMDD = merged_dfMDD[cols]

# STACT(DES)
stact_root = os.path.join(root_dir, 'STACT_Williams')

baseline_dir = os.path.join(stact_root, "baseline")
followup_dir = os.path.join(stact_root, "followups (3,6,9,12 months)")

baseline_files = os.listdir(baseline_dir)
followup_files = os.listdir(followup_dir)

mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']
merged_dfDES = pd.DataFrame(columns=mergelist)
if '.DS_Store' in baseline_files:
    baseline_files.remove('.DS_Store')
for file in baseline_files:
    prefix = os.path.splitext(file)[0]
    if 'ndar01.csv' in file:
        df = pd.read_csv(os.path.join(baseline_dir, file), header=0, encoding='ISO-8859-1')
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        print(prefix, ":", df.shape)
        if merged_dfDES.empty:
            merged_dfDES = df
        else:
            merged_dfDES = pd.merge(merged_dfDES, df, on=mergelist, how='outer')
            print("Merged:", merged_dfDES.shape)
    else:
        df = pd.read_csv(os.path.join(baseline_dir, file), header=0)
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        print(prefix, ":", df.shape)
        if merged_dfDES.empty:
            merged_dfDES = df
        else:
            merged_dfDES = pd.merge(merged_dfDES, df, on=mergelist, how='outer')
            print("Merged:", merged_dfDES.shape)

if not merged_dfDES.empty:
    merged_dfDES['study'] = 'STACT'

cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_dfDES if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_dfDES = merged_dfDES[cols]


def rename_columns(df):
    new_columns = []
    for col in df.columns:
        if 'gad' in col:
            new_columns.append(f'gad701_{col}')
        elif 'phq' in col:
            new_columns.append(f'phq01_{col}')
        elif 'swls' in col:
            new_columns.append(f'swls01_{col}')
        else:
            new_columns.append(col)
    df.columns = new_columns
    return df


mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'sex']
for file in followup_files:
    prefix = os.path.splitext(file)[0]
    if 'webneuro01_F' in file:
        df = pd.read_csv(os.path.join(followup_dir, file), header=0)
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        print(prefix, ":", df.shape)
        if merged_dfDES.empty:
            merged_dfDES = df
        else:
            merged_dfDES = pd.merge(merged_dfDES, df, on=mergelist, how='outer')
            print("Merged:", merged_dfDES.shape)
    else:
        df = pd.read_csv(os.path.join(followup_dir, file), header=0)
        df = rename_columns(df)
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        df = df.dropna(axis=1, how='all').copy()
        print(file, ":", df.shape)
        if merged_dfDES.empty:
            merged_dfDES = df
        else:
            # merged_dfDES = pd.merge(merged_dfDES, df, on=mergelist, how='outer')
            merged_dfDES = pd.concat([merged_dfDES, df], ignore_index=True)
            print("Merged:", merged_dfDES.shape)

# stack three study
# merged_temp = pd.merge(merged_dfMDD, merged_dfDAM,
#                       on=['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'], how='outer')
# merged_all = pd.merge(merged_temp, merged_df, on=['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'], how='outer')
merged_all = pd.concat([merged_dfMDD, merged_dfDAM, merged_df, merged_dfDES], axis=0)

merged_all.to_csv(os.path.join(root_dir, 'NDA_structures_table_combined.csv'))

#######################################################
# change name of output to prevent confusion over REDCap data dictionary
col_all = list(set(list(merged_df.columns)).union(list(merged_dfDAM.columns), list(merged_dfMDD.columns)))
varsall = pd.DataFrame(col_all)
varsall.to_csv(os.path.join(root_dir, "NDA_structures_variables_combined.csv"), index=False, header=False)
