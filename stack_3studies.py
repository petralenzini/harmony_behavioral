import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

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
for file in ADA_files:
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    df = df.iloc[1:]
    if merged_df.empty:
        merged_df = df
    else:
        merged_df = pd.merge(merged_df, df, on=['src_subject_id', 'interview_date'], how='outer')

if not merged_df.empty:
    merged_df['study'] = 'ADA'

# DAM
merged_dfDAM = pd.DataFrame()
if '.DS_Store' in DAM_files:
    DAM_files.remove('.DS_Store')

for file in DAM_files:
    df = pd.read_csv(os.path.join(DAM_dir, file), header=1)
    if merged_dfDAM.empty:
        merged_dfDAM = df
    else:
        merged_dfDAM = pd.merge(merged_dfDAM, df, on=['src_subject_id', 'interview_date'], how='outer')

if not merged_dfDAM.empty:
    merged_dfDAM['study'] = 'DAM'

# MDD
merged_dfMDD = pd.DataFrame()
if '.DS_Store' in MDD_files:
    MDD_files.remove('.DS_Store')

for file in MDD_files:
    if 'definitions' in file:
        pass
    elif 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=';', header=1)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, on=['src_subject_id', 'interview_date'], how='outer')
    else:
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1)
        if merged_dfMDD.empty:
            merged_dfMDD = df
        else:
            merged_dfMDD = pd.merge(merged_dfMDD, df, on=['src_subject_id', 'interview_date'], how='outer')

if not merged_dfMDD.empty:
    merged_dfMDD['study'] = 'MDD'

merged_temp = pd.merge(merged_dfMDD, merged_dfDAM, on=['src_subject_id', 'interview_date'], how='outer')
merged_all = pd.merge(merged_temp, merged_df, on=['src_subject_id', 'interview_date'], how='outer')

merged_all.columns
merged_all=merged_all.drop(columns=['gender_y', 'subjectkey_y_x','interview_age_x_x'])


merged_all.to_csv('NDA_structures_combined.csv')