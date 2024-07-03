import os
import pandas as pd
import numpy as np
import os
import sys
import shutil


# TO DO filter with curated subject lists (imaging and phenotypes)
datestr=datetime.date.today().strftime("%b%d_%Y")
freezenum=5

# for this to work you need to right click on harmony and 'mark directory' as 'sources root':
from functions import *

mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']

root_invent = "/Users/petralenzini/work/harmony/harmony_behavioral"
root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral_"+str(datestr)
#root_dir = "/Users/yuanyuanxiaowang/PycharmProjects/pythonProject/Harmony/harmony_behavioral/Harmony_non_imaging_data_sharing"
DESsubs='DES_subjectsCuratedJuly2024.txt'

ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
DAM_dir = os.path.join(root_dir, "DAM_ANXPE_Sheline")
MDD_dir = os.path.join(root_dir, "MDD_Narr")
STACT_baseline_dir = os.path.join(root_dir, "DES_STACT_Williams/baseline")
STACT_followup_dir = os.path.join(root_dir, "DES_STACT_Williams/followups (3,6,9,12 months)")

# ADA-BANDA
droplist = ['collection_id', 'collection_title', 'dataset_id', 'wcst_ni', 'interview_language']

# get list of files to process
ADA_files = os.listdir(ADA_dir)  # txt
Notin_ADA_files = [file for file in ADA_files if 'pdf' in file
             or ('dataset_collection' in file)
             or ('package_info' in file)
             or ('md5_values' in file)
             or ('fmriresults01' in file)
             or ('imagingcollection01' in file)]
ADA_files = [file for file in ADA_files if 'pdf' not in file
             and ('dataset_collection' not in file)
             and ('package_info' not in file)
             and ('md5_values' not in file)
             and ('fmriresults01' not in file)
             and ('imagingcollection01' not in file)]
if '.DS_Store' in ADA_files:
    ADA_files.remove('.DS_Store')


# process
AllADA = pd.DataFrame(columns=['element', 'variable', 'structure'])
merged_dfADA = pd.DataFrame(columns=mergelist)
merged_dfADA_parents = pd.DataFrame(columns=mergelist)
for file in ADA_files:
    prefix = os.path.splitext(file)[0]
    print("processing ", prefix)
    df = pd.read_csv(os.path.join(ADA_dir, file), delimiter='\t', header=0)
    df = df.iloc[1:]
    for d in droplist:
        try:
            df = df.drop(columns=d).copy()
        except:
            pass
    df = drop999cols(df, verbose=True)
    # some have the structure id housekeeping variable:
    try:
        df = df.drop(columns=[prefix + '_id']).copy()
    except:
        print(prefix + "_id NOT FOUND")
    # rename the variables and store the map
    map, df, AllADA = partialcrosswalkmap(mergelist, df, prefix, AllADA)
    # clean up empty rows
    df, extra = droprows(df, mergelist)
    # get the merged dataset
    # separate parents from children
    respondentvar = prefix + '_respondent'
    if respondentvar in df.columns:
        parents = df.loc[df[respondentvar].str.upper() == 'PARENT'].drop(columns=respondentvar)
        if not parents.empty:
            if prefix == "demographics02":
                exceptcols = [i for i in mergelist if i not in ['interview_date', 'interview_age']]
                parents = drop999cols(parents, verbose=True)
                parents = parents.drop_duplicates(subset=exceptcols, keep='last', inplace=False,
                                                  ignore_index=True).copy()
        df = df.loc[df[respondentvar].str.upper() == 'CHILD'].drop(columns=respondentvar)
        if not df.empty:
            df = drop999cols(df, verbose=True)
    merged_dfADA = created_merged(prefix, df, merged_dfADA, mergelist)
    merged_dfADA_parents = created_merged(prefix, parents, merged_dfADA_parents, mergelist)

if not merged_dfADA.empty:
    merged_dfADA['study'] = 'BANDA'

if not merged_dfADA_parents.empty:
    merged_dfADA_parents['study'] = 'BANDA PARENTS'

merged_dfADA_parents = drop999cols(merged_dfADA_parents, verbose=True)
merged_dfADA = drop999cols(merged_dfADA, verbose=True)

#all the parent structures had respondent variables which are in AllADA but not the stacked structure.  remove them and later create a single one
AllADA = AllADA.loc[AllADA.variable.str.contains('respondent') == False]

#here we're just checking that the number of variables in AllADA
# is the same as the set of variables in the combined list of columns in parents and children
AllADA = AllADA.drop_duplicates()
print("AllADA.shape:",AllADA.shape)
check = [a for a in AllADA.variable if a not in merged_dfADA.columns]  #only in parents
print("Check:",len(check))
check2 = [a for a in check if a not in merged_dfADA_parents.columns]
print("Check2:",len(check2))
pcheck = [p for p in AllADA.variable if p not in merged_dfADA_parents.columns] #only in children
print("pcheck2:",len(pcheck))
pcheck2 = [p for p in pcheck if p not in merged_dfADA.columns]
print("pheck2:",len(pcheck2))
check3=set(list(merged_dfADA.columns)+list(merged_dfADA_parents.columns))
print("Check3:",len(check3))

#now set study for these variables
AllADA_C=AllADA.loc[AllADA.variable.isin(list(merged_dfADA.columns))].copy()
AllADA_C['study'] = 'BANDA'
AllADA_P=AllADA.loc[AllADA.variable.isin(list(merged_dfADA_parents.columns))].copy()
AllADA_P['study'] = 'BANDA PARENTS'

merged_dfADA = merged_dfADA.reset_index().drop(columns='index')
merged_dfADA_parents = merged_dfADA_parents.reset_index().drop(columns='index')

#DON"T ADD THIS BACK>  REDUNDANT
##i.e. don't add back in a parent respondent variable for stacking.
#merged_dfADA_parents['respondent']='parent'
##note that respondent wont be defined in the data dictionary as of this moment


# reorder
cols = mergelist + ['study'] + [col for col in merged_dfADA if col not in mergelist + [
    'study'] and 'version' not in col and 'wcst' not in col and 'language' not in col and 'pin' not in col]
colsparents =  mergelist + ['study'] + [col for col in merged_dfADA_parents if col not in mergelist + [
    'study'] and 'version' not in col and 'wcst' not in col and 'language' not in col and 'pin' not in col]

merged_dfADA[cols].to_csv(os.path.join(root_dir, "CheckBANDA_Sanity_Child.csv"), index=False)
merged_dfADA_parents.to_csv(os.path.join(root_dir, "CheckBANDA_Parents_Sanity.csv"), index=False)

###############################
# DAM (ANXPE)
DAM_files = os.listdir(DAM_dir)
if '.DS_Store' in DAM_files:
    DAM_files.remove('.DS_Store')

Notin_DAM_files = [file for file in DAM_files if "2019.02.27_FullDataSet_RedCap_824132.csv" in file
             or ('xlsx' in file)
             or ('Copy' in file)
             or ('docx' in file)
             or ('Data' in file)
             or ('ethx' in file)
             or ('shapstotals' in file)
             or ('Baseline Tx_U01_824132_am' in file)
             or ('Database_CNB&NIH_CompletetdPts.csv' in file)
             or ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' in file)
             or ('u01_neuropsych_summary_v1.csv' in file)]
DAM_files = [file for file in DAM_files if "2019.02.27_FullDataSet_RedCap_824132.csv" not in file
             and ('xlsx' not in file)
             and ('docx' not in file)
             and ('Data' not in file)
             and ('ethx' not in file)
             and ('shapstotals' not in file)
             and ('Baseline Tx_U01_824132_am' not in file)
             and ('Copy' not in file)
             and ('Database_CNB&NIH_CompletetdPts.csv' not in file)
             and ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' not in file)
             and ('genomics' not in file)
             and ('u01_neuropsych_summary_v1.csv' not in file)]

# process
# dates are hacked.  Kept only first instance when multiple visits present.
mergelist=['subjectkey', 'interview_date', 'interview_age', 'sex']
AllVDAM = pd.DataFrame(columns=['element', 'variable', 'structure'])
merged_dfDAM = pd.DataFrame(columns=['subjectkey'])
#merge totals in with items for shaps01
#shaps totals do not equal sum of shaps components.  Exclude unless it can be clarified.
#df1 = pd.read_csv(os.path.join(DAM_dir, 'shaps01.csv'), header=0, dtype=str)
#df2 = pd.read_csv(os.path.join(DAM_dir, 'shapstotals.csv'), header=0, dtype=str)
#df3=pd.merge(df1,df2,on=['subjectkey','visit'],how='outer')
#df3.to_csv('ANXPEShapsTest.csv')

for file in DAM_files:
    prefix = os.path.splitext(file)[0]
    print("processing ", prefix)
    if prefix in ['pcl501','masq01','handedness','shapstotals','ethx','hamd','h_r_lsi','scid','madrs01','soadjs01','shaps01','rrs01']:
        df = pd.read_csv(os.path.join(DAM_dir, file), header=0, dtype=str)
    else:
        df = pd.read_csv(os.path.join(DAM_dir, file), header=1, dtype=str)
    try:
        df=df.drop(columns=['cohort'])
    except:
        pass
    try:
        df = df.drop(columns=['src_subject_id'])
    except:
        pass
    df = drop999cols(df, verbose=False)
    if 'visit' in list(df.columns):
        print('visit in this structure')
        #keep only the first instance of a phenotype, since we don't know the visit and interview date correspondences
        #this poor man's solution will need to be investigated.  Were images only at Baseline?
        df=df.sort_values(['subjectkey', 'visit'])
        df=df.drop_duplicates(subset=['subjectkey'],keep='first').drop(columns=('visit'))
    map, df, AllVDAM = partialcrosswalkmap(mergelist, df, prefix, AllVDAM)
    df, extra = droprows(df, mergelist)
    if 'interview_date' in df.columns:
        if 'interview_date' in merged_dfDAM.columns:
            merged_dfDAM = created_merged(prefix, df, merged_dfDAM, mergelist)
        else:
            merged_dfDAM = created_merged(prefix, df, merged_dfDAM, ['subjectkey'])
    else:
        merged_dfDAM = created_merged(prefix, df, merged_dfDAM, ['subjectkey'])

#merged_dfDAM.to_csv('testDAM.csv',index=False)

if not merged_dfDAM.empty:
    merged_dfDAM['study'] = 'DAM'
    AllVDAM['study'] = 'DAM'

# reorder cols
cols = mergelist + ['study'] + [col for col in merged_dfDAM if col not in mergelist + [
    'study'] and 'version' not in col and 'wcst' not in col and 'language' not in col and 'pin' not in col and 'patient_id' not in col and 'sample_id' not in col]
merged_dfDAM = merged_dfDAM.reset_index()
merged_dfDAM = merged_dfDAM.drop(columns='index')
merged_dfDAM[cols].to_csv(os.path.join(root_dir, "CheckDAM_Sanity.csv"), index=False)
AllVDAM = AllVDAM.drop_duplicates()
check = [a for a in AllVDAM.variable if a not in merged_dfDAM.columns]

#################
# MDD
MDD_files = os.listdir(MDD_dir)
Notin_MDD_files=[file for file in MDD_files if "definitions" in file
             or ('xlsx' in file)
             or ('docx' in file)]
MDD_files = [file for file in MDD_files if "definitions" not in file
             and ('xlsx' not in file)
             and ('docx' not in file)
             and ('ymrs' not in file) and ('medication_history' not in file) and ('sacq01' not in file)] #dropping ymrs and medication_history, per Katherine June 11 2024. dropping self administered comorbidity questionnaire because not in extended Tozzi table
if '.DS_Store' in MDD_files:
    MDD_files.remove('.DS_Store')


AllVMDD = pd.DataFrame(columns=['element', 'variable', 'structure'])
merged_dfMDD = pd.DataFrame(columns=['src_subject_key', 'subjectkey'])

for file in MDD_files:
    prefix = os.path.splitext(file)[0]
    print("processing ", prefix)
    if 'hrsd01.csv' in file:
        # formatting issue, dlm is ; not ,
        df = pd.read_csv(os.path.join(MDD_dir, file), sep=',', header=1, dtype=str)
        df['interview_date'] = pd.to_datetime(df['interview_date']).dt.strftime('%m/%d/%Y')
    else:
        if prefix in ['cat_ss','idsc','nih_toolbox_emotion','ymrs','tobacco_use','medication_history_ketamine','substance_use','migraine_screener','nih_toolbox_pain','medication_history','alcohol_use','early_advisory','stressful_life_events','scid']:
            df = pd.read_csv(os.path.join(MDD_dir, file), header=0, dtype=str)
        else:
            df = pd.read_csv(os.path.join(MDD_dir, file), header=1, dtype=str)
        if prefix in ['scid']:
            df['interview_date'] = pd.to_datetime(df['scid_date']).dt.strftime('%m/%d/%Y')
            df=df.drop(columns=['scid_date'])
        else:
            df['interview_date'] = pd.to_datetime(df['interview_date']).dt.strftime('%m/%d/%Y')
    if 'gender' in df.columns:
        df['sex'] = df['gender']
        df = df.drop(columns='gender')
    df.loc[df.sex.str.upper() == 'FEMALE', 'sex'] = 'F'
    df.loc[df.sex.str.upper() == 'MALE', 'sex'] = 'M'
    df = MDDdupfix(df, mergelist)
    df = drop999cols(df, verbose=True)
    map, df, AllVMDD = partialcrosswalkmap(mergelist, df, prefix, AllVMDD)
    df, extra = droprows(df, mergelist)
    ##track ids that are different betweeen subjcts
    # set1 = set(df['subjectkey'])
    # set2 = set(merged_dfMDD['subjectkey'])
    # print('number of different subjectkey :', len(set1 ^ set2))
    # print("if still duplicated, check:")
    # duplicate_rows_df = df[df[mergelist + extra].duplicated(keep=False)]
    # print('duplicate', prefix, ":", duplicate_rows_df.shape)#,duplicate_rows_df)
    ## get the merged dataset
    merged_dfMDD = created_merged(prefix, df, merged_dfMDD, mergelist)
if not merged_dfMDD.empty:
    merged_dfMDD['study'] = 'MDD'
    AllVMDD['study'] = 'MDD'
AllVMDD = AllVMDD.drop_duplicates()
check = [a for a in AllVMDD.variable if a not in merged_dfMDD.columns]

cols = mergelist + ['study'] + [col for col in merged_dfMDD if col not in mergelist + [
    'study'] and 'version' not in col and 'wcst' not in col and 'language' not in col and 'pin' not in col and 'index' not in col]
merged_dfMDD = merged_dfMDD.reset_index()
merged_dfMDD = merged_dfMDD.drop(columns='index')
merged_dfMDD[cols].to_csv(os.path.join(root_dir, "CheckMDD_Sanity.csv"), index=False)

################################
# #####STACT ##
baseline_files = os.listdir(STACT_baseline_dir)
followup_files = os.listdir(STACT_followup_dir)
#try renaming ndar01.csv to proper ndar_subject01.csv if it hasn't already been renamed in a previous iteration
#shutil.copy(os.path.join(STACT_baseline_dir, "ndar01.csv"), os.path.join(STACT_baseline_dir, "ndar_subject01.csv"))

for d in ['.DS_Store']:#,'cqol.csv','sofas.csv','cope.csv']:
    try:
        baseline_files.remove(d)
    except:
        pass
    try:
        followup_files.remove(d)
    except:
        pass

merged_dfSTACT = pd.DataFrame(columns=mergelist).reset_index()
AllVSTACT = pd.DataFrame(columns=['element', 'variable', 'structure'])

#first the baseline files
for file in baseline_files:
    os.system("dos2unix " +os.path.join(STACT_baseline_dir, file))
    prefix = os.path.splitext(file)[0]
    if 'webneuro01' in prefix:
        prefix = 'webneuro01'
    print("processing ", prefix)
    df = pd.read_csv(os.path.join(STACT_baseline_dir, file), header=0, encoding='ISO-8859-1')
    try:
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
    except:
        df['interview_date'] = pd.to_datetime(df['baseline_interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        df=df.drop(columns=['baseline_interview_date'])
    df = drop999cols(df, verbose=True)
    if file in ['fagerstrom01.csv','qids01.csv','edinburgh_hand01.csv','dass01.csv','hrsd01','shaps01.csv']:
        map, df, AllVSTACT = partialcrosswalkmap([i for i in mergelist if i != 'sex'], df, prefix, AllVSTACT)
        df, extra = droprows(df, [i for i in mergelist if i != 'sex'])
    else:
        map, df, AllVSTACT = partialcrosswalkmap(mergelist, df, prefix, AllVSTACT)
        df, extra = droprows(df, mergelist)
    if merged_dfSTACT.empty:
        merged_dfSTACT = df
    else:
        if file in ['fagerstrom01.csv','qids01.csv','edinburgh_hand01.csv','dass01.csv','hrsd01.csv','shaps01.csv']:
            merged_dfSTACT = created_merged(prefix, df, merged_dfSTACT, [i for i in mergelist if i != 'sex'])
        else:
            merged_dfSTACT = created_merged(prefix, df, merged_dfSTACT, mergelist)
merged_dfSTACT = merged_dfSTACT.loc[merged_dfSTACT.interview_date.isnull() == False]

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
        df, AllVSTACT = rename_columns(df, AllVSTACT)
        df, extra = droprows(df, mergelist)
        # df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        print(file, ":", df.shape)
        if merged_dfDES.empty:
            merged_dfDES = df
        else:
            merged_dfDES = pd.concat([merged_dfDES, df], ignore_index=True, join='outer')
            print("Merged:", merged_dfDES.shape)
        merged_dfDES = merged_dfDES.loc[merged_dfDES.interview_date.isnull() == False]
AllVSTACT = AllVSTACT.drop_duplicates()

AllVSTACT = AllVSTACT.loc[AllVSTACT.element.isin(mergelist) == False]

merged_dfDESSTACT = pd.concat([merged_dfSTACT, merged_dfDES], axis=0)
merged_dfDESSTACT = merged_dfDESSTACT.reset_index()
merged_dfDESSTACT = merged_dfDESSTACT.drop(columns=['index'])

if not merged_dfDESSTACT.empty:
    merged_dfDESSTACT['study'] = 'DES'
    AllVSTACT['study'] = 'DES'

check = [a for a in AllVSTACT.variable if a not in merged_dfDESSTACT.columns]
check = [a for a in merged_dfDESSTACT.columns if a not in list(AllVSTACT.variable)]

# drop index and rearrange
cols = mergelist + ['study'] + [col for col in merged_dfDESSTACT if col not in mergelist + ['study','webneuro01_interview_age']]
merged_dfDESSTACT = merged_dfDESSTACT[cols].copy()
merged_dfDESSTACT.to_csv(os.path.join(root_dir, "CheckDES_Sanity.csv"), index=False)

# merged_dfADA_parents
# merged_dfADA
# AllADA_P
# AllADA_C

# check that variables in the 'All' precursors to a data dictionary are the same as the ones in the merged datasets, less the mergelist
for i in [merged_dfMDD, merged_dfDAM, merged_dfADA, merged_dfADA_parents, merged_dfDESSTACT]:
    print(i.shape)
for i in [AllVMDD, AllVDAM, AllADA_C, AllADA_P, AllVSTACT]:
    print(i.shape)

check =[i for i in list(merged_dfADA_parents) if i not in list(AllADA_P.variable)]

# stack 'five' studies
merged_all = pd.concat([merged_dfMDD, merged_dfDAM, merged_dfADA, merged_dfADA_parents, merged_dfDESSTACT], axis=0)
dropit=[i for i in list(merged_all.columns) if 'form_complete' in i]
merged_all=merged_all.drop(columns=dropit)

# merged_all.to_csv(os.path.join(root_dir, 'NDA_structures_table_combined.csv'), index=False)
#sanity:
print(merged_all[['subjectkey','study']].drop_duplicates().study.value_counts())
# stack  lists of variables that are non-empty in each study
col_all = pd.concat([AllADA_C, AllADA_P, AllVDAM, AllVMDD, AllVSTACT], axis=0)
col_all = col_all.drop_duplicates()
col_all=col_all.loc[~(col_all.variable.isin(dropit))]

col_all.to_csv(os.path.join(root_dir, "NDA_structures_variables_combined.csv"), index=False)
#merged_all.to_csv(os.path.join(root_dir, 'NDA_structures_table_combined.csv'), index=False)
# merge big stack csv and inventory
inventory = pd.read_csv(os.path.join(root_invent, 'Inventory_Datasets_2023-09-07.csv'))
inventory = inventory[['src_subject_id', 'CASE/CONTROL', 'interview_age_yrs', 'race']]
inventory['src_subject_id'] = inventory['src_subject_id'].apply(
    lambda x: 'sub-' + x if isinstance(x, str) and 'CONN' in x else x
)
#inventory.to_csv('inventory2.csv', index=False)
#inventory = pd.read_csv('inventory2.csv')
result = pd.merge(merged_all, inventory, on='src_subject_id', how='left')
result=result.drop_duplicates()
result['CASE/CONTROL'] = result.apply(
    lambda row: 'CASE' if row['subjectkey'] in ['NDARAB921RG', 'NDAREH040NB2'] else row['CASE/CONTROL'],
    axis=1
)
cols = mergelist + ['study'] + [col for col in result if col not in mergelist + ['study']]
result = result[cols].copy()

#apply subject filters
DESsublist=pd.read_csv(os.path.join(root_invent,DESsubs) #imaging is BL only

result.to_csv(os.path.join(root_dir, 'NDA_structures_table_combined.csv'), index=False)
result.to_csv(os.path.join(root_dir,'HarmonyData_Freeze'+str(freezenum)+'_'+ datestr +'_wSites.csv'), index=False)

print("************************")
print("ALL FINISHED")
