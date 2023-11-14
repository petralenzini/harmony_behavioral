import os
import pandas as pd

rmstructure = ['fmriresults01', 'imagingcollection01', 'image03']

# root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
root_dir = "/Users/yuanyuanxiaowang/PycharmProjects/pythonProject/Harmony/harmony_behavioral/Harmony_non_imaging_data_sharing"

ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
DAM_dir = os.path.join(root_dir, "ANXPE_Sheline")
MDD_dir = os.path.join(root_dir, "MDD_Narr")

ADA_files = os.listdir(ADA_dir)  # txt
DAM_files = os.listdir(DAM_dir)
MDD_files = os.listdir(MDD_dir)

# corelist=['subjectkey','src_subject_id','interview_date','interview_age','sex','gender']
# droplist=['collection_id']

# ADA
# mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex', 'collection_id',
#             'collection_title']
mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']
droplist = ['collection_id', 'collection_title']
ADA_files = [file for file in ADA_files if 'pdf' not in file
             and ('dataset_collection' not in file)
             and ('package_info' not in file)
             and ('md5_values' not in file)
             and ('fmriresults01' not in file)
             and ('imagingcollection01' not in file)]
if '.DS_Store' in ADA_files:
    ADA_files.remove('.DS_Store')

###############check if need to add other variables to distinguish rows
for file in ADA_files:
    prefix = os.path.splitext(file)[0]
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    df = df.iloc[1:]
    df = df.drop(columns=droplist)
    df = df.dropna(axis=1, how='all').copy()
    duplicate_rows_df = df[df[mergelist].duplicated()]
    print(prefix, ":", duplicate_rows_df.shape)
    # print(duplicate_rows_df)
########################################################################

merged_df = pd.DataFrame(columns=mergelist)
for file in ADA_files:
    prefix = os.path.splitext(file)[0]
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    # print(file)
    # print(mergelist)
    try:
        df = df.iloc[1:]
        # mergelist = list(set(list(merged_df.columns)).intersection(list(df.columns)))
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        # df = df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False).copy()
        print(prefix, ":", df.shape)
        # print(df['interview_date'])
        if merged_df.empty:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on=mergelist, how='outer')
            print("Merged:", merged_df.shape)
    except:
        pass
if not merged_df.empty:
    merged_df['study'] = 'ADA'

cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_df if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_df = merged_df[cols]

# DAM
mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']
droplist = ['collection_id', 'collection_title']
merged_dfDAM = pd.DataFrame(columns=['src_subject_id', 'subjectkey'])
DAM_files = [file for file in DAM_files if "2019.02.27_FullDataSet_RedCap_824132.csv" not in file
             and ('xlsx' not in file)
             and ('docx' not in file)
             and ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' not in file)
             and ('u01_neuropsych_summary_v1.csv' not in file)]
if '.DS_Store' in DAM_files:
    DAM_files.remove('.DS_Store')

for file in DAM_files:
    prefix = os.path.splitext(file)[0]
    df = pd.read_csv(os.path.join(DAM_dir, file), header=1, dtype=str)
    # mergelist = list(set(list(merged_dfDAM.columns)).intersection(list(df.columns)))
    try:
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        # df = df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False).copy()
        print(prefix, ":", df.shape)
        if merged_dfDAM.empty:
            merged_dfDAM = df
        else:
            merged_dfDAM = pd.merge(merged_dfDAM, df, on=mergelist, how='outer')
            print("Merged:", merged_dfDAM.shape)
    except:
        pass

if not merged_dfDAM.empty:
    merged_dfDAM['study'] = 'DAM'

cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_dfDAM if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]

merged_dfDAM = merged_dfDAM[cols]


# MDD

# For the wierd case of data/age mismatches above, once you can confirm that it's not something we introduced (e.g.
# by checking the files they uploaded), lets do the following
# 1. create a separate df, which is just the merge of subjectkey, interview_date, and age.
def convert_date(date_str):
    # Split the date string by '/'
    parts = date_str.split('/')
    # Check if the year part (last part) has 2 digits
    if len(parts[-1]) == 2:
        return pd.to_datetime(date_str, format='%m/%d/%y').strftime('%m/%d/%Y')
    else:
        return pd.to_datetime(date_str, format='%m/%d/%Y').strftime('%m/%d/%Y')


AgeCorrect = pd.DataFrame()
for file in MDD_files:
    if 'definitions' in file:
        pass
    elif 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=';', header=1, dtype=str)
        df['interview_date'] = df['interview_date'].apply(convert_date)
        age_df = df[['subjectkey', 'interview_date', 'interview_age']]
        AgeCorrect = pd.concat([AgeCorrect, age_df], axis=0)
    else:
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
        df['interview_date'] = df['interview_date'].apply(convert_date)
        age_df = df[['subjectkey', 'interview_date', 'interview_age']]
        AgeCorrect = pd.concat([AgeCorrect, age_df], axis=0)

AgeCorrect = AgeCorrect.sort_values(by=['interview_date', 'interview_age'], ascending=[True, True])
AgeCorrect = AgeCorrect.drop_duplicates(subset=['subjectkey', 'interview_date'], keep='first')

# 2. Sort by date and age, such that the youngest age is always the first one that shows up for a date.
# 3. Drop duplicates by date (e.g. effectively keeping only the youngest age)
# 4. Set this df aside (lets call it AgeCorrect for simplicity) and go back to your regular merging, only this time drop
# age before any merges, and remove age from the mergelist.
# 5. When you get your final large merged file, go back and merge in AgeCorrect by subjectkey and date.  In this way,
# you should only get one row per date, unless rows are duplicated for some other reason.

mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'sex']  # need to fix sex
droplist = ['collection_id', 'collection_title']
MDD_files = [file for file in MDD_files if "definitions" not in file
             and ('xlsx' not in file)
             and ('docx' not in file)
             and ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' not in file)
             and ('u01_neuropsych_summary_v1.csv' not in file)]
if '.DS_Store' in MDD_files:
    MDD_files.remove('.DS_Store')

merged_dfMDD = pd.DataFrame(columns=['src_subject_key', 'subjectkey'])
nih_pin_remove_list = ['flanker01.csv', 'orrt01.csv', 'lswmt01.csv', 'dccs01.csv', 'pcps01.csv', 'psm01.csv',
                       'tpvt01.csv']

for file in MDD_files:
    prefix = os.path.splitext(file)[0]
    if 'definitions' in file:
        pass
    elif 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=';', header=1, dtype=str)
        # df = df.drop(columns='interview_age')
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%Y').dt.strftime('%m/%d/%Y')
        df['sex'] = df['gender']
        df = df.drop(columns='gender')
        columns_to_check = df.columns.difference(mergelist)
        df = df.dropna(subset=columns_to_check, axis=0, how='all').copy()
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        duplicate_rows_df = df[df[mergelist].duplicated(keep=False)]
        set1 = set(df['subjectkey'])
        set2 = set(merged_dfMDD['subjectkey'])
        print(prefix, ":", df.shape)
        # print(duplicate_rows_df)
        print('number of different subjectkey :', len(set1 ^ set2))
        print('duplicate', prefix, ":", duplicate_rows_df.shape)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, on=mergelist, how='outer')
            print("Merged:", merged_dfMDD.shape)
    elif 'ndar_subject01' in file:
        ###do not need to rename the sex column
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
        # df = df.drop(columns='interview_age')
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%Y').dt.strftime('%m/%d/%Y')
        df['sex'] = df['sex'].replace({'Male': 'M', 'Female': 'F'})
        df = df.drop(columns=['phenotype'])
        columns_to_check = df.columns.difference(mergelist)
        df = df.dropna(subset=columns_to_check, axis=0, how='all').copy()
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        df = df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False).copy()
        duplicate_rows_df = df[df[mergelist].duplicated(keep=False)]
        set1 = set(df['subjectkey'])
        set2 = set(merged_dfMDD['subjectkey'])
        # print(duplicate_rows_df)
        print(prefix, ":", df.shape)
        # print(duplicate_rows_df)
        print('number of different subjectkey :', len(set1 ^ set2))
        print('duplicate', prefix, ":", duplicate_rows_df.shape)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, on=mergelist, how='outer')
            print("Merged:", merged_dfMDD.shape)
    elif any(substring in file for substring in nih_pin_remove_list):
        ###need to remove 'nih_pin'
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
        # df = df.drop(columns='interview_age')
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        df = df.drop(columns=['nih_pin']).copy()
        df['sex'] = df['gender']
        df = df.drop(columns='gender')
        columns_to_check = df.columns.difference(mergelist)
        df = df.dropna(subset=columns_to_check, axis=0, how='all').copy()
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        df = df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False).copy()
        duplicate_rows_df = df[df[mergelist].duplicated(keep=False)]
        set1 = set(df['subjectkey'])
        set2 = set(merged_dfMDD['subjectkey'])
        print(prefix, ":", df.shape)
        # print(duplicate_rows_df)
        print('number of different subjectkey :', len(set1 ^ set2))
        print('duplicate', prefix, ":", duplicate_rows_df.shape)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, on=mergelist, how='outer')
            print("Merged:", merged_dfMDD.shape)
    else:
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
        # df = df.drop(columns='interview_age')
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        df['sex'] = df['gender']
        df = df.drop(columns='gender')
        columns_to_check = df.columns.difference(mergelist)
        df = df.dropna(subset=columns_to_check, axis=0, how='all').copy()
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        df = df.drop_duplicates(subset=None, keep='first', inplace=False, ignore_index=False).copy()
        duplicate_rows_df = df[df[mergelist].duplicated(keep=False)]
        set1 = set(df['subjectkey'])
        set2 = set(merged_dfMDD['subjectkey'])
        print(prefix, ":", df.shape)
        print('number of different subjectkey :', len(set1 ^ set2))
        print('duplicate', prefix, ":", duplicate_rows_df.shape)
        print(df.shape, merged_dfMDD.shape)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, on=mergelist, how='outer')
            print("Merged:", merged_dfMDD.shape)
        print(df.shape, merged_dfMDD.shape)

if not merged_dfMDD.empty:
    merged_dfMDD['study'] = 'MDD'

# merged_dfMDD['sex'] = merged_dfMDD['gender']
# merged_dfMDD = merged_dfMDD.drop(columns='gender')
cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_dfMDD if
                                                               col not in ['src_subject_id',
                                                                           'interview_date', 'sex',
                                                                           'interview_age', 'study']]
merged_dfMDD = merged_dfMDD[cols]

merged_dfMDD_final = pd.merge(merged_dfMDD, AgeCorrect,on=['subjectkey', 'interview_date'], how='inner')

merged_dfMDD_final = merged_dfMDD_final[cols]

# stack three study merged_temp = pd.merge(merged_dfMDD, merged_dfDAM, on=['src_subject_id', 'interview_date',
# 'interview_age', 'study', 'sex'], how='outer') merged_all = pd.merge(merged_temp, merged_df, on=['src_subject_id',
# 'interview_date', 'interview_age', 'study', 'sex'], how='outer')
merged_all = pd.concat([merged_dfMDD, merged_dfDAM, merged_df], axis=0)

merged_all.to_csv('NDA_structures_combined.csv')

#######################################################
col_all = list(set(list(merged_df.columns)).union(list(merged_dfDAM.columns), list(merged_dfMDD.columns)))
dictionary = pd.DataFrame(col_all)
dictionary.to_csv(os.path.join(root_dir, "DataDictionaryDraft.csv"), index=False, header=False)
