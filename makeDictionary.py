import pandas as pd
import os
import requests
import json
import re
import datetime

datestr=datetime.date.today().strftime("%b%d_%Y")
freezenum=5

root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral_"+str(datestr)
stackvars="NDA_structures_variables_combined.csv"
inst="InstrumentDetails.csv"
os.system("dos2unix " +os.path.join(root_dir,stackvars))
os.system("dos2unix " +os.path.join(root_dir,inst))

vars=pd.read_csv(os.path.join(root_dir,stackvars))
instruments=pd.read_csv(os.path.join(root_dir,inst))[['Domain','Measure','nda_structure']]

varsBANDA=vars.loc[vars.study=='BANDA'].copy();varsBANDA['ADA_BANDA']='YES'
varsBANDA_PARENTS=vars.loc[vars.study=='BANDA PARENTS'].copy(); varsBANDA_PARENTS['ADA_BANDA_PARENTS']='YES'
varsDAM=vars.loc[vars.study=='DAM'].copy();varsDAM['DAM_ANXPE']='YES'
varsMDD=vars.loc[vars.study=='MDD'].copy();varsMDD['MDD']='YES'
varsDES=vars.loc[vars.study=='DES'].copy(); varsDES['DES_STACT']='YES'

varsStudy=pd.merge(vars.drop('study',axis=1),varsBANDA.drop('element',axis=1),on=['variable','structure'],how='left')
varsStudy=pd.merge(varsStudy.drop('study',axis=1),varsBANDA_PARENTS.drop('element',axis=1),on=['variable','structure'],how='left')
varsStudy=pd.merge(varsStudy.drop('study',axis=1),varsDAM.drop('element',axis=1),on=['variable','structure'],how='left')
varsStudy=pd.merge(varsStudy.drop('study',axis=1),varsMDD.drop('element',axis=1),on=['variable','structure'],how='left')
varsStudy=pd.merge(varsStudy.drop('study',axis=1),varsDES.drop('element',axis=1),on=['variable','structure'],how='left')
Encyclopedia=pd.merge(instruments,varsStudy.drop('study',axis=1),left_on='nda_structure',right_on='structure',how='right').drop('nda_structure',axis=1)
Encyclopedia.Measure=Encyclopedia.Measure.str.replace("¬†",'')
Encyclopedia.loc[Encyclopedia.Domain.isnull()==True,"Domain"]='Unassigned'

list(Encyclopedia.loc[Encyclopedia.Measure.isnull()==True]['variable'].str.split('_',expand=True)[0].unique())


Encyclopedia.to_csv(os.path.join(root_dir,'temp.csv'))
#API is not working right now so can't pull in labels"
def getNDAdetails(structure_name):
    #varlist = crosswalk.loc[crosswalk.nda_structure == structure_name, 'nda_element'].to_list()
    r = requests.get('https://ndar.nih.gov/api/datadictionary/datastructure/{}'
                     .format(structure_name),
                     headers={'Accept': 'application/json'})
    struct = json.loads(r.text)
    df = pd.DataFrame(struct['dataElements'])
    df.update(df[['description']].applymap('"{}"'.format))
    df.update(df[['valueRange']].applymap('"{}"'.format))
    df.update(df[['notes']].applymap('"{}"'.format))
    df2 = df[['name', 'description', 'valueRange', 'notes']].copy()
    df2['nda_structure'] = structure_name
    df2 = df2.rename(columns={'name': 'nda_element'})
    return df2

try:
    os.makedirs(os.path.join(root_dir,'nda_annotation'))
except:
    pass

structurelist=list(vars.structure.unique())

encyclopedia=pd.DataFrame()
for s in structurelist:
    print(s)
    try:
        if s in ['cqol','sofas']:
            news=pd.read_csv(os.path.join(root_dir,"DES_STACT_Williams","NDA data dictionaries",s+"_definitions.csv"))[['ELEMENT NAME', 'DESCRIPTION', 'VALUE RANGE', 'NOTES']]
            news.columns=['nda_element', 'description', 'valueRange', 'notes']
            news['nda_structure']=s
            news=news.loc[news.nda_element.isnull()==False]
            print(s)
        if s in ['hamd','soadjs01','h_r_lsi','handedness','pcl501','scid']:
            news = pd.read_csv(
                os.path.join(root_dir, "DAM_ANXPE_Sheline", "DCAM Data Dictionary_pl.csv"))[['Structure','Variable','Description','valueRange','notes']]
            news=news.loc[news.Structure==s]
            news.columns = ['nda_structure','nda_element', 'description', 'valueRange', 'notes']
            news = news.loc[news.nda_element.isnull() == False]
            print(s)
        if s in ['cat_ss','rrsmdd01','substance_use','stressful_life_events']:
            news=pd.read_csv(os.path.join(root_dir, "MDD_Narr", "mdd_extradefinitions.csv"))[['Structure','Variable','Description','valueRange','notes']]
            news=news.loc[news.Structure==s]
            news.columns = ['nda_structure', 'nda_element', 'description', 'valueRange', 'notes']
            print(s)
        if s not in ['cqol','sofas','hamd','soadjs01','h_r_lsi','handedness','pcl501','scid','shapstotals','cat_ss','rrsmdd01','stressful_life_events']:
            print(s)
            news=getNDAdetails(structure_name=s)
            news.to_csv(os.path.join(root_dir, 'nda_annotation',s+'def.csv'),index=False)
        encyclopedia=pd.concat([encyclopedia,news],axis=0)
        print("passed.......")
    except:
        print("No NDA data dictionary or other annotation for",s)

#filter down encyclopedia to mandatory variables and anything else that shows up in vars list
e0=encyclopedia.copy()
e0.loc[e0.nda_element.str.contains('src_subject_id')]

e0['variable']=e0.nda_structure+"_"+e0.nda_element
e2=pd.merge(Encyclopedia,encyclopedia,left_on=['element','structure'],right_on=['nda_element','nda_structure'],how='left')
e2=e2[['Domain','Measure','structure','variable','description','valueRange','notes','ADA_BANDA','ADA_BANDA_PARENTS','DAM_ANXPE','MDD','DES_STACT']]
e2=e2.drop_duplicates(subset='variable')

e2.to_csv(root_dir+'/HarmonyData_Freeze'+str(freezenum)+'_'+datestr+'_DataDictionary.csv',index=False)

#find diffs
#datavars=pd.read_csv(root_dir+'/HarmonyData_Freeze4_Mar6_2024_wSites.csv')

#need to fix these differences next time
#set(list(e2.variable))^set(list(datavars.columns))
