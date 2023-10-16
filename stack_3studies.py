import os
import pandas as pd

root_dir = "/Users/yuanyuanxiaowang/PycharmProjects/pythonProject/Harmony/harmony_behavioral" \
           "/Harmony_non_imaging_data_sharing"

ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
DAM_dir = os.path.join(root_dir, "ANXPE_Sheline")
MDD_dir = os.path.join(root_dir, "MDD_Narr")

ADA_files = os.listdir(ADA_dir)  # txt
DAM_files = os.listdir(DAM_dir)
MDD_files = os.listdir(MDD_dir)

# ADA
merged_df = pd.DataFrame()
if '.DS_Store' in ADA_files:
    ADA_files.remove('.DS_Store')
for file in ADA_files:
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    df = df.iloc[1:]
    if merged_df.empty:
        merged_df = df
    else:
        # merged_df = pd.merge(merged_df, df, on=['src_subject_id', 'interview_date', 'sex', 'interview_age'],how='outer')
        merged_df = pd.merge(merged_df, df, how='outer')

if not merged_df.empty:
    merged_df['study'] = 'ADA'

cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_df if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_df = merged_df[cols]

# DAM
merged_dfDAM = pd.DataFrame()
if '.DS_Store' in DAM_files:
    DAM_files.remove('.DS_Store')

for file in DAM_files:
    df = pd.read_csv(os.path.join(DAM_dir, file), header=1, dtype=str)
    print(df)
    if merged_dfDAM.empty:
        merged_dfDAM = df
    else:
        merged_dfDAM = pd.merge(merged_dfDAM, df, how='outer')

if not merged_dfDAM.empty:
    merged_dfDAM['study'] = 'DAM'

cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_dfDAM if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_dfDAM = merged_dfDAM[cols]

# MDD
merged_dfMDD = pd.DataFrame()
if '.DS_Store' in MDD_files:
    MDD_files.remove('.DS_Store')

for file in MDD_files:
    if 'definitions' in file:
        pass
    elif 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=';', header=1, dtype=str)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, how='outer')
    else:
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, how='outer')

if not merged_dfMDD.empty:
    merged_dfMDD['study'] = 'MDD'

merged_dfMDD['sex'] = merged_dfMDD['gender']
merged_dfMDD = merged_dfMDD.drop(columns='gender')
cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_dfMDD if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_dfMDD = merged_dfMDD[cols]

# stack three study
# merged_temp = pd.merge(merged_dfMDD, merged_dfDAM,
#                       on=['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'], how='outer')
# merged_all = pd.merge(merged_temp, merged_df, on=['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'], how='outer')
merged_all = pd.concat([merged_dfMDD, merged_dfDAM, merged_df], axis=0)

merged_all.to_csv('NDA_structures_combined.csv')
