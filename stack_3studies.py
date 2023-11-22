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

# ADA-BANDA
droplist = ['collection_id', 'collection_title','dataset_id','wcst_ni','interview_language']

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
merged_dfADA_parents = pd.DataFrame(columns=mergelist)
for file in ADA_files:
    prefix = os.path.splitext(file)[0]
    print("processing ",prefix)
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    df = df.iloc[1:]
    for d in droplist:
        try:
            df = df.drop(columns=d).copy()
        except:
            pass
    df = drop999cols(df, verbose=True)
    #some have the structure id housekeeping variable:
    try:
        df = df.drop(columns=[prefix + '_id']).copy()
    except:
        print(prefix + "_id NOT FOUND")
    #rename the variables and store the map
    map, df, AllADA = partialcrosswalkmap(mergelist, df, prefix, AllADA)
    #clean up empty rows
    df,extra=droprows(df,mergelist)
    #get the merged dataset
    #separate parents from children
    respondentvar=prefix+'_respondent'
    if respondentvar in df.columns:
        parents=df.loc[df[respondentvar].str.upper()=='PARENT'].drop(columns=respondentvar)
        if not parents.empty:
            if prefix=="demographics02":
                exceptcols=[i for i in mergelist if i not in ['interview_date','interview_age']]
                parents = parents.drop_duplicates(subset=exceptcols, keep='last', inplace=False, ignore_index=True).copy()
        df=df.loc[df[respondentvar].str.upper()=='CHILD'].drop(columns=respondentvar)
    merged_dfADA=created_merged(prefix,df,merged_dfADA,mergelist)
    merged_dfADA_parents=created_merged(prefix,parents,merged_dfADA_parents,mergelist)
if not merged_dfADA.empty:
    merged_dfADA['study'] = 'BANDA'
    AllADA['study']='BANDA'
AllADA=AllADA.drop_duplicates()
check=[a for a in AllADA.variable if a not in merged_dfADA.columns]
AllADA=AllADA.loc[AllADA.variable.str.contains('respondent')==False]

#reorder
cols = mergelist +['study'] + [col for col in merged_dfADA if col not in mergelist + ['study'] and 'version' not in col and 'wcst' not in col and 'language' not in col and 'pin' not in col]
merged_dfADA = merged_dfADA.reset_index().drop(columns='index')
merged_dfADA[cols].to_csv("CheckBANDA_Sanity_Child.csv",index=False)
merged_dfADA_parents.to_csv("CheckBANDA_Parents_Sanity.csv",index=False)
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
cols = mergelist +['study'] + [col for col in merged_dfDAM if col not in mergelist + ['study'] and 'version' not in col and 'wcst' not in col and 'language' not in col and 'pin' not in col]
merged_dfDAM = merged_dfDAM.reset_index()
merged_dfDAM[cols].to_csv("CheckDAM_Sanity.csv",index=False)
AllVDAM=AllVDAM.drop_duplicates()
check=[a for a in AllVDAM.variable if a not in merged_dfDAM.columns]

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
    print("processing ", prefix)
    if 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=';', header=1, dtype=str)
        df['interview_date'] = pd.to_datetime(df['interview_date']).dt.strftime('%m/%d/%Y')
    else:
        df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
        df['interview_date'] = pd.to_datetime(df['interview_date']).dt.strftime('%m/%d/%Y')
    if 'gender' in df.columns:
        df['sex'] = df['gender']
        df = df.drop(columns='gender')
    df.loc[df.sex.str.upper() == 'FEMALE', 'sex'] = 'F'
    df.loc[df.sex.str.upper() == 'MALE', 'sex'] = 'M'
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
AllVMDD=AllVMDD.drop_duplicates()
check=[a for a in AllVMDD.variable if a not in merged_dfMDD.columns]

cols = mergelist+['study'] + [col for col in merged_dfMDD if col not in mergelist + ['study'] and 'version' not in col and 'wcst' not in col and 'language' not in col and 'pin' not in col and 'index' not in col]
merged_dfMDD = merged_dfMDD.reset_index()
merged_dfMDD[cols].to_csv("CheckMDD_Sanity.csv",index=False)

################################
# #####STACT ##
baseline_files = os.listdir(STACT_baseline_dir)
followup_files = os.listdir(STACT_followup_dir)
if '.DS_Store' in baseline_files:
    baseline_files.remove('.DS_Store')
if '.DS_Store' in followup_files:
    followup_files.remove('.DS_Store')

merged_dfSTACT = pd.DataFrame(columns=mergelist).reset_index()
AllVSTACT=pd.DataFrame(columns=['element','variable','structure'])

for file in baseline_files:
    prefix = os.path.splitext(file)[0]
    if 'webneuro01' in prefix:
        prefix='webneuro01'
    print("processing ", prefix)
    df = pd.read_csv(os.path.join(STACT_baseline_dir, file), header=0, encoding='ISO-8859-1')
    df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
    df = drop999cols(df, verbose=True)
    map, df, AllVSTACT = partialcrosswalkmap(mergelist, df, prefix, AllVSTACT)
    df, extra = droprows(df, mergelist)
    if merged_dfSTACT.empty:
        merged_dfSTACT = df
    else:
        merged_dfSTACT=created_merged(prefix,df,merged_dfSTACT,mergelist)
merged_dfSTACT=merged_dfSTACT.loc[merged_dfSTACT.interview_date.isnull()==False]

merged_dfDES = pd.DataFrame(columns=mergelist).reset_index()
for file in followup_files:
    prefix = os.path.splitext(file)[0]
    df = pd.read_csv(os.path.join(STACT_followup_dir, file), header=0)
    if 'webneuro01' in file:
        prefix = 'webneuro01'
        mergelistw = [i for i in mergelist if i not in ['interview_age']]
        df = drop999cols(df, verbose=True)
        map, df, AllVSTACT = partialcrosswalkmap(mergelistw, df, prefix, AllVSTACT)
        df, extra = droprows(df, mergelist)
        print(prefix, ":", df.shape)
        if merged_dfDES.empty:
            merged_dfDES = df
        else:
            merged_dfDES = pd.merge(merged_dfDES, df, on=mergelistw, how='outer')
            print("Merged:", merged_dfDES.shape)
    else:
        df,AllVSTACT = rename_columns(df,AllVSTACT)
        df,extra=droprows(df, mergelist)
        #df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        print(file, ":", df.shape)
        if merged_dfDES.empty:
            merged_dfDES = df
        else:
            merged_dfDES = pd.concat([merged_dfDES, df], ignore_index=True, join='outer')
            print("Merged:", merged_dfDES.shape)
        merged_dfDES=merged_dfDES.loc[merged_dfDES.interview_date.isnull()==False]
AllVSTACT=AllVSTACT.drop_duplicates()

AllVSTACT=AllVSTACT.loc[AllVSTACT.element.isin(mergelist)==False]

merged_dfDESSTACT=pd.concat([merged_dfSTACT,merged_dfDES],axis=0)
merged_dfDESSTACT = merged_dfDESSTACT.reset_index()
if not merged_dfDESSTACT.empty:
    merged_dfDESSTACT['study'] = 'STACT'
    AllVSTACT['study']='STACT'

check=[a for a in AllVSTACT.variable if a not in merged_dfDESSTACT.columns]
check=[a for a in merged_dfDESSTACT.columns if a not in AllVSTACT.variable]


cols = mergelist+['study'] + [col for col in merged_dfDESSTACT if col not in mergelist + ['study','index']]
merged_dfDESSTACT[cols].to_csv("CheckDES_Sanity.csv",index=False)



for i in [merged_dfMDD, merged_dfDAM, merged_dfADA,merged_dfSTACT]:
    print(i.shape)
for i in [AllVMDD,AllVDAM,AllADA,AllVSTACT]:
    print(i.shape)

# stack four study
merged_all = pd.concat([merged_dfMDD, merged_dfDAM, merged_dfADA, merged_dfDESSTACT], axis=0)
merged_all.to_csv(os.path.join(root_dir, 'NDA_structures_table_combined.csv'))

for study in [merged_dfMDD, merged_dfDAM, merged_dfADA, merged_dfDESSTACT]:
    for col
# variables
##this leading to discrepancies
#allstructs=mergelist+['study']
#col_all = pd.concat([AllADA,AllVDAM,AllVMDD,AllVSTACT],axis=0)
#col_all = col_all.drop_duplicates()


col_all.to_csv(os.path.join(root_dir, "NDA_structures_variables_combined.csv"), index=False)

print("************************")
print("ALL FINISHED")