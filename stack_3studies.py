import os
import pandas as pd
import numpy as np
import os
import sys

#for this to work you need to right click on harmony and 'mark directory' as 'sources root':
from functions import *

rmstructure = ['fmriresults01', 'imagingcollection01', 'image03']
mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']

root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
#root_dir = "/Users/yuanyuanxiaowang/PycharmProjects/pythonProject/Harmony/harmony_behavioral/Harmony_non_imaging_data_sharing"

ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
DAM_dir = os.path.join(root_dir, "ANXPE_Sheline")
MDD_dir = os.path.join(root_dir, "MDD_Narr")
STACT_baseline_dir = os.path.join(root_dir, "STACT_Williams/baseline")
STACT_followup_dir = os.path.join(root_dir, "STACT_Williams/followups (3,6,9,12 months)")

# ADA
droplist = ['collection_id', 'collection_title','dataset_id']

#get list of files to process
ADA_files = os.listdir(ADA_dir)  # txt
ADA_files = [file for file in ADA_files if 'pdf' not in file
             and ('dataset_collection' not in file)
             and ('package_info' not in file)
             and ('md5_values' not in file)
             and ('fmriresults01' not in file)
             and ('imagingcollection01' not in file)]
if '.DS_Store' in ADA_files:
    ADA_files.remove('.DS_Store')

#process
AllADA=pd.DataFrame(columns=['element','variable','structure'])
merged_dfADA = pd.DataFrame(columns=mergelist)
for file in ADA_files:
    prefix = os.path.splitext(file)[0]
    print("processing ",prefix)
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    df = df.drop(columns=droplist).copy()
    df = drop999cols(df, verbose=True)
    #some have the structure id housekeeping variable:
    try:
        df = df.drop(columns=[prefix +'_id']).copy()
    except:
        print(prefix + "_id NOT FOUND")
    df = df.iloc[1:]
    #rename the variables and store the map
    map, df, AllADA = partialcrosswalkmap(mergelist, df, prefix, AllADA)
    #clean up empty rows
    df,extra=droprows(df,mergelist)
    #get the merged dataset
    merged_dfADA=created_merged(prefix,df,merged_dfADA,mergelist)
if not merged_dfADA.empty:
    merged_dfADA['study'] = 'ADA'
    AllADA['study']='ADA'
#reorder
cols = mergelist +['study'] + [col for col in merged_dfADA if col not in mergelist + ['study']]
merged_dfADA = merged_dfADA[cols].reset_index()

###############################
# DAM
DAM_files = os.listdir(DAM_dir)
DAM_files = [file for file in DAM_files if "2019.02.27_FullDataSet_RedCap_824132.csv" not in file
             and ('xlsx' not in file)
             and ('docx' not in file)
             and ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' not in file)
             and ('u01_neuropsych_summary_v1.csv' not in file)]
if '.DS_Store' in DAM_files:
    DAM_files.remove('.DS_Store')

#process
AllVDAM=pd.DataFrame(columns=['element','variable','structure'])
merged_dfDAM = pd.DataFrame(columns=['src_subject_id', 'subjectkey'])
for file in DAM_files:
    prefix = os.path.splitext(file)[0]
    print("processing ",prefix)
    df = pd.read_csv(os.path.join(DAM_dir, file), header=1, dtype=str)
    df = drop999cols(df, verbose=True)
    map, df, AllVDAM = partialcrosswalkmap(mergelist, df, prefix, AllVDAM)
    df,extra = droprows(df, mergelist)
    merged_dfDAM = created_merged(prefix, df, merged_dfDAM,mergelist)

if not merged_dfDAM.empty:
    merged_dfDAM['study'] = 'DAM'
    AllVDAM['study']='DAM'

#reorder cols
cols = mergelist +['study'] + [col for col in merged_dfDAM if col not in mergelist + ['study']]
merged_dfDAM = merged_dfDAM[cols].reset_index()

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

AllVMDD=pd.DataFrame(columns=['element','variable','structure'])
merged_dfMDD = pd.DataFrame(columns=['src_subject_key', 'subjectkey'])

for file in MDD_files:
    prefix = os.path.splitext(file)[0]
    print("processing ",prefix)
    if 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=';', header=1, dtype=str)
    else:
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
    if 'gender' in df.columns:
        df['sex'] = df['gender']
        df = df.drop(columns='gender')
    df=MDDdupfix(df,mergelist)
    df = drop999cols(df, verbose=True)
    map, df, AllVMDD = partialcrosswalkmap(mergelist, df, prefix, AllVMDD)
    df,extra=droprows(df,mergelist)
    ##track ids that are different betweeen subjcts
    #set1 = set(df['subjectkey'])
    #set2 = set(merged_dfMDD['subjectkey'])
    #print('number of different subjectkey :', len(set1 ^ set2))
    #print("if still duplicated, check:")
    #duplicate_rows_df = df[df[mergelist + extra].duplicated(keep=False)]
    #print('duplicate', prefix, ":", duplicate_rows_df.shape)#,duplicate_rows_df)
    ## get the merged dataset
    merged_dfMDD = created_merged(prefix, df, merged_dfMDD,mergelist)
if not merged_dfMDD.empty:
    merged_dfMDD['study'] = 'MDD'
    AllVMDD['study']='MDD'

cols = mergelist+['study'] + [col for col in merged_dfMDD if col not in mergelist + ['study']]
merged_dfMDD = merged_dfMDD[cols].reset_index()

################################
# #####STACT ##
baseline_files = os.listdir(STACT_baseline_dir)
followup_files = os.listdir(STACT_followup_dir)
if '.DS_Store' in baseline_files:
    baseline_files.remove('.DS_Store')

merged_dfSTACT = pd.DataFrame(columns=mergelist).reset_index()
AllVSTACT=pd.DataFrame(columns=['element','variable','structure'])

for file in baseline_files:
    prefix = os.path.splitext(file)[0]
    print("processing ", prefix)
    df = pd.read_csv(os.path.join(STACT_baseline_dir, file), header=0, encoding='ISO-8859-1')
    df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
    df = drop999cols(df, verbose=True)
    map, df, AllVSTACT = partialcrosswalkmap(mergelist, df, prefix, AllVSTACT)
    df, extra = droprows(df, mergelist)
    merged_dfSTACT=created_merged(prefix,df,merged_dfSTACT,mergelist)
if not merged_dfSTACT.empty:
    merged_dfSTACT['study'] = 'STACT'
    AllVSTACT['study']='STACT'

##followup files are directly stackable.  No merging necessary
#for file in followup_files:
cols = mergelist +['study'] + [col for col in merged_dfSTACT if col not in mergelist + ['study']]
merged_dfSTACT = merged_dfSTACT[cols].reset_index()


for i in [merged_dfMDD, merged_dfDAM, merged_dfADA,merged_dfSTACT]:
    print(i.shape)
for i in [AllVMDD,AllVDAM,AllADA,AllVSTACT]:
    print(i.shape)
# stack four studies data
merged_all = pd.concat([merged_dfMDD, merged_dfDAM, merged_dfADA], axis=0)
merged_all = pd.concat([merged_all,merged_dfSTACT],axis=0)
merged_all.to_csv(os.path.join(root_dir, 'NDA_structures_table_combined.csv'))

# variables
allstructs=mergelist+['study']
col_all = pd.concat([AllADA,AllVDAM,AllVMDD,AllVSTACT],axis=0)
col_all.to_csv(os.path.join(root_dir, "NDA_structures_variables_combined.csv"), index=False)

print("************************")
print("ALL FINISHED")