import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import date

rootdir="/Users/petralenzini/work/harmony/harmony_behavioral/"
MDDfiles=os.listdir(rootdir+"MDD_Narr")
BANDAfiles=os.listdir(rootdir+"BANDA_Whitfield_Gabrieli")
ANXPEfiles=os.listdir(rootdir+"ANXPE_Sheline")

#MDD
MDDinventory=pd.read_csv(rootdir+"phenotypes_original/MDD_Narr/ndar_subject01.csv", sep=',', header=1)
MDDinventory=MDDinventory[['subjectkey','src_subject_id','interview_age','sex','race','phenotype']].copy()
#MDD has multiple timepoints per person

for i in MDDfiles:
    temp=pd.read_csv(rootdir+"MDD_Narr/"+i, header=1)
    temp[i]='YES'
    if 'definitions' in i:
        pass
    elif 'hrsd01.csv' in i:
        #formatting issue, dlm is ; not ,
        temp=pd.read_csv(rootdir+"MDD_Narr/"+i, sep=';', header=1)
        temp = temp.drop_duplicates(subset=['subjectkey'])
    else:
        try:
            print(i)
            temp = temp.drop_duplicates(subset=['subjectkey'])
            MDDinventory=MDDinventory.merge(temp[['subjectkey',i]].drop_duplicates(),on='subjectkey',how='outer')
        except:
            print(i,"is (re)-intiating MDDinventory.  If not first structure in list, this is a problem")
            MDDinventory=temp[['subjectkey',i]].drop_duplicates().copy()

MDDinventory['study']='MDD'

#BANDA
BANDAinventory=pd.read_csv(rootdir+"/BANDA_Whitfield_Gabrieli/ndar_subject01.txt", sep='\t',header=0,encoding='ISO-8859-1')
BANDAinventory=BANDAinventory.iloc[1:]
BANDAinventory=BANDAinventory[['subjectkey','src_subject_id','interview_age','sex','race','phenotype']].copy()
for i in BANDAfiles:
    if ('pdf' not in i) and ('dataset_collection' not in i) and ('package_info' not in i) and ('md5_values' not in i) and ('.DS_Store' not in i):
        temp=pd.read_csv(rootdir+"/BANDA_Whitfield_Gabrieli/"+i, sep='\t',header=0,encoding='ISO-8859-1')
        # Drop the second header row
        temp = temp.iloc[1:]
        temp[i]='YES'
        if 'definitions' in i:
            pass
        else:
            try:
                print(i)
                temp = temp.drop_duplicates(subset=['subjectkey'])
                BANDAinventory=BANDAinventory.merge(temp[['subjectkey',i]],on='subjectkey',how='outer')
            except:
                print(i,"is (re)-intiating BANDA inventory.  If not first structure in list, this is a problem")
                BANDAinventory=temp[['subjectkey',i]].copy()

BANDAinventory['study']='BANDA'
BANDAinventory.columns=['subjectkey', 'src_subject_id', 'interview_age', 'sex', 'race',
       'phenotype', 'deldisk01.csv', 'flanker01.csv', 'dccs01.csv',
       'lswmt01.csv', 'chaphand01.csv', 'rcads01.csv', 'bisbas01.csv',
       'fmriresults01.csv', 'ndar_subject01.csv', 'wasi201.csv', 'mfq01.csv',
       'nffi01.csv', 'shaps01.csv', 'imagingcollection01.csv', 'pwmt01.csv',
       'orrt01.csv', 'er4001.csv', 'cbcl01.csv', 'rbqa01.csv', 'fhs01.csv',
       'ksads_diagnosesp201.csv', 'stai01.csv', 'strain01.csv', 'masq01.csv',
       'demographics02.csv', 'rmbi01.csv', 'cssrs01.csv', 'pmat01.csv',
       'ksads_diagnoses01.csv', 'pcps01.csv', 'tanner_sms01.csv', 'study']
#ANXPE
ANXPEinventory=pd.read_csv(rootdir+"ANXPE_Sheline/ndar_subject01.csv", sep=',',header=1)
ANXPEinventory=ANXPEinventory[['subjectkey','src_subject_id','interview_age','sex','race','phenotype']].copy()

for i in ANXPEfiles:
    if ("2019.02.27_FullDataSet_RedCap_824132.csv" not in i) and ('xlsx' not in i) and ('docx' not in i) and ('REDCap_ANXPEConnectomicsStudyDataCol_DataDictionary_2023-08-22' not in i):
        if "u01_neuropsych_summary_v1" in i:
            temp = pd.read_csv(rootdir + "ANXPE_Sheline/u01_neuropsych_summary_v1.csv")
        else:
            temp = pd.read_csv(rootdir + "phenotypes_original/ANXPE_Sheline/" + i, sep=',', header=1)
        # Drop the second header row
        #temp = temp.iloc[1:]
        temp[i] = 'YES'
        try:
            print(i)
            temp = temp.drop_duplicates(subset=['subjectkey'])
            ANXPEinventory = ANXPEinventory.merge(temp[['subjectkey', i]], on='subjectkey', how='outer')
        except:
            print(i, "is (re)-intiating ANXPE inventory.  If not first structure in list, this is a problem")
            ANXPEinventory = temp[['subjectkey', i]].copy()

ANXPExls=pd.DataFrame()
for i in ANXPEfiles:
    if ('xlsx'  in i) and ('Readme' not in i) and ('crosswalk' not in i):
        print(i)
        if ('MADRS_Baseline_V3_V4' in i) or ('Episodic Tx History_U01_V4_824132.xlsx' in i):
            temp=pd.read_excel(rootdir + "phenotypes_original/ANXPE_Sheline/" + i, sheet_name='Sheet1')
        else:
            tempA = pd.read_excel(rootdir + "phenotypes_original/ANXPE_Sheline/" + i, sheet_name='AM')
            try:
                tempB = pd.read_excel(rootdir + "phenotypes_original/ANXPE_Sheline/" + i, sheet_name='HC')
            except:
                tempB = pd.read_excel(rootdir + "phenotypes_original/ANXPE_Sheline/" + i, sheet_name='HCPatient ID')
            try:
                tempA.columns
                tempA = tempA.drop_duplicates(subset=['Patient ID'])
                tempB = tempB.drop_duplicates(subset=['Patient ID'])
                temp=pd.concat([tempA,tempB])
                temp[i] = 'YES'
                if ANXPExls.empty:
                    ANXPExls=temp[['Patient ID', i]].copy()
                else:
                    ANXPExls = ANXPExls.merge(temp[['Patient ID', i]], on='Patient ID', how='outer')
            except:
                print('Problemo')

ANXPEinventorycombo=pd.merge(ANXPEinventory,ANXPExls,left_on='subjectkey',right_on='Patient ID',how='outer')
ANXPEinventorycombo.loc[ANXPEinventorycombo.subjectkey.isnull()==True,'subjectkey']=ANXPEinventorycombo['Patient ID']
ANXPEinventorycombo=ANXPEinventorycombo.loc[ANXPEinventorycombo.subjectkey.isnull()==False]

#redcap files
anxpered=pd.read_csv(rootdir + "phenotypes_original/ANXPE_Sheline/2019.02.27_FullDataSet_RedCap_824132.csv",encoding = "cp1252",low_memory=False)
anxpered['ANXPE_REDCAP']='YES'
anxpered=anxpered.loc[anxpered.redcap_event_name.isin(['clinician_arm_2','participant_arm_2'])][['ï»¿record_id','ANXPE_REDCAP']]
ANXPEinventorycombored=pd.merge(ANXPEinventorycombo,anxpered,left_on='subjectkey',right_on='ï»¿record_id',how='left')
ANXPEinventorycombored=ANXPEinventorycombored.drop(columns=['ï»¿record_id'])
#ANXPEinventorycombored=ANXPEinventorycombored.rename(columns={'ï»¿record_id':'src_subject_id'}).copy()

ANXPEinventorycombored['study']='ANXPE'
ANXPEinventorycombored=ANXPEinventorycombored.drop_duplicates(subset='subjectkey').copy()
#ANXPEinventorycombored.to_csv('testANXPE.csv',index=False)

# STACT ####
stactred=pd.read_csv(rootdir + "phenotypes_original/STACT/WILLIAMS_data.csv",encoding = "cp1252",low_memory=False)
stactred=stactred.loc[stactred.redcap_event_name.str.contains('baseline')][['record_id','clinical','demo_age','demo_gender','demo_gender_birthsex','demo_race___1','demo_race___2','demo_race___3','demo_race___4','demo_race___5','demo_race___99','demo_other_race','demo_ethnicity']].copy()
#'White','Black or African American','Asian','Unknown or not reported','More than one race'}
#1, American Indian or Alaska Native | 2, Asian | 3, Black or African American | 4, Native Hawaiian or Other Pacific Islander | 5, White | 99, Other
#0, Hispanic or Latino | 1, Not Hispanic or Latino
stactred['sumrace']=stactred.demo_race___1+stactred.demo_race___2+stactred.demo_race___3+stactred.demo_race___4+stactred.demo_race___5
stactred.loc[(stactred.sumrace==1) & (stactred.demo_race___1==1),'race']='American Indian/Alaska Native'
stactred.loc[(stactred.sumrace==1) & (stactred.demo_race___2==1),'race']='Asian'
stactred.loc[(stactred.sumrace==1) & (stactred.demo_race___3==1),'race']='Black or African American'
stactred.loc[(stactred.sumrace==1) & (stactred.demo_race___4==1),'race']='Native Hawaiian or Other Pacific Islander'
stactred.loc[(stactred.sumrace==1) & (stactred.demo_race___5==1),'race']='White'
stactred.loc[(stactred.sumrace==1) & (stactred.demo_race___99==1),'race']='Unknown or not reported'
stactred.loc[(stactred.sumrace >1),'race']='More than one race'
stactred.loc[(stactred.sumrace==0),'race']='Unknown or not reported'
stactred=stactred.drop(columns=['sumrace','demo_race___1','demo_race___2','demo_race___3','demo_race___4','demo_race___5','demo_race___99','demo_other_race'])
stactred.loc[stactred.demo_gender==1,'sex']='F'
stactred.loc[stactred.demo_gender==2,'sex']='M'
stactred.loc[stactred.demo_gender==3,'sex']='O'
stactred.loc[stactred.demo_gender=='','sex']='N'
stactred['interview_age']=round(12*stactred.demo_age,0)
stactred['Stact_REDCap']='YES'
stactred['phenotype']=''
stactred.loc[stactred.clinical==1,'phenotype']='STACT_CLINICAL_1'
stactred.loc[stactred.clinical==0,'phenotype']='STACT_CLINICAL_0'

stactred=stactred.rename(columns={'record_id':'src_subject_id'})
stactredinventory=stactred[['src_subject_id', 'phenotype', 'race', 'sex', 'interview_age', 'Stact_REDCap']].copy()
stactredinventory['study']='STACT'

#put them together
INVENTORY=pd.concat([MDDinventory.reset_index(),BANDAinventory.reset_index(),ANXPEinventorycombored.reset_index(),stactredinventory.reset_index()],axis=0,ignore_index=True)
#INVENTORY.reset_index(inplace=True)
INVENTORY['CASE/CONTROL']='CASE'
INVENTORY.loc[INVENTORY.phenotype.isnull()==True,'phenotype']='UNKNOWN'
INVENTORY.loc[INVENTORY['CASE/CONTROL']=='UNKNONW','CASE/CONTROL']='UNKNOWN'
INVENTORY.loc[(INVENTORY.phenotype.str.upper().str.contains('CONTROL')) | (INVENTORY.phenotype=='STACT_CLINICAL_0'),'CASE/CONTROL']='CONTROL'
#INVENTORY.loc[INVENTORY.phenotype.str.upper().str.contains('STACT'),'CASE/CONTROL']=INVENTORY.phenotype
INVENTORY['CASE/CONTROL'].value_counts(dropna=False)
INVENTORY.loc[INVENTORY.sex=='Female','sex']='F'
INVENTORY.loc[INVENTORY.sex=='Male','sex']='M'
INVENTORY.loc[INVENTORY.race.isin(['Unkown or not reported','Uknown or not reported','Other Non-White']),'race']='Unknown or not reported'
INVENTORY.loc[INVENTORY.race.isnull()==True,'race']='Unknown or not reported'
INVENTORY.loc[INVENTORY.sex.isnull()==True,'sex']='Unknown or not reported'
INVENTORY['interview_age_yrs']=INVENTORY.interview_age.astype('float')/12
#race, age
pd.crosstab(INVENTORY.study,INVENTORY.race,margins=True).plot.bar(rot=45,title='')
plt.show()
#pd.crosstab(forplot.Race,forplot.Site).to_csv('Recruitment_Stats',mode='a')
r=pd.crosstab(INVENTORY.study,INVENTORY.race,margins=True)

#check
INVENTORY.sex.value_counts(dropna=False)
s=pd.crosstab(INVENTORY.study,INVENTORY.sex,margins=True)
s.plot.bar(rot=45)
plt.show()

INVENTORY['CASE/CONTROL'].value_counts(dropna=False)
c=pd.crosstab(INVENTORY.study,INVENTORY['CASE/CONTROL'],margins=True)
c.plot.bar(rot=45)
plt.show()


import seaborn as sns
#INVENTORY.boxplot(column=['interview_age'],by='study')
sns.boxplot(data=INVENTORY, x="study", y="interview_age_yrs")
plt.show()

harmtable=pd.concat([s,c,INVENTORY.groupby('study').mean()],axis=1)
harmtable=harmtable[['F', 'M','CASE', 'CONTROL', 'interview_age_yrs']]
harmtable=harmtable.rename(columns={'interview_age_yrs':'mean_age_yrs'})
harmtable['NEEDED CONTROLS']=harmtable.CASE-harmtable.CONTROL
harmtable.mean_age_yrs=harmtable.mean_age_yrs.round()

harmtable.to_csv('Inventory_CaseControl_Summary_'+date.today().strftime("%Y-%m-%d")+'.csv')
INVENTORY.to_csv('Inventory_'+date.today().strftime("%Y-%m-%d")+'.csv')
