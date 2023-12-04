import pandas as pd
import os
import requests
import json
import re


root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
stackvars="NDA_structures_variables_combined.csv"
vars=pd.read_csv(os.path.join(root_dir,stackvars))

#patch ndar01
vars.loc[vars.structure=='ndar01','structure']='ndar_subject01'
vars.variable=vars.variable.str.replace('ndar01_','ndar_subject01_')
#variables=list(pd.read_csv(os.path.join(root_dir,"DataDictionaryDraft.csv"), header=None)[0])

def getNDAdetails(structure_name):
    #varlist = crosswalk.loc[crosswalk.nda_structure == structure_name, 'nda_element'].to_list()
    r = requests.get('https://ndar.nih.gov/api/datadictionary/datastructure/{}'
                     .format(structure_name),
                     headers={'Accept': 'application/json'})
    struct = json.loads(r.text)
    df = pd.DataFrame(struct['dataElements'])
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
        news=getNDAdetails(structure_name=s)
        encyclopedia=pd.concat([encyclopedia,news],axis=0)
        print("passed.......")
    except:
        print("Sorry...couldn't download data dictionary for",s)

#filter down encyclopedia to mandatory variables and anything else that shows up in vars list
e0=encyclopedia.head(5).copy()
e0['variable']=e0.nda_element
e2=pd.merge(vars,encyclopedia,left_on=['element','structure'],right_on=['nda_element','nda_structure'],how='left')

#prepare e3
e3=e2.drop(columns=['element','structure'])[['variable', 'description', 'valueRange','notes','nda_structure','nda_element']].drop_duplicates()
e3['BANDA']=''
e3['BANDA PARENTS']=''
e3['DAM']=''
e3['MDD']=''
e3['DES']=''

#now create an indicator variable for sites with a given variable based on e2, then populate e3
sitestructs=pd.DataFrame()
for i in list(e2.nda_structure.unique()):
    strucvars=vars.loc[vars.structure==i]
    studystrucvars = strucvars[['variable']].drop_duplicates()
    print("*****************")
    print("Structure:", i)
    for s in list(strucvars.study.unique()):
        print("Study:",s)
        studystrucvars=studystrucvars.merge(strucvars.loc[strucvars.study==s][['variable']],on='variable',how='inner')
        studystruclist=list(studystrucvars.variable)
        e3.loc[e3.variable.isin(studystruclist),s]='YES'
        #studystrucvars = studystrucvars.merge(strucvars.loc[strucvars.study == s][['variable']], on='variable', how='outer', indicator=s)
        #for c in studystrucvars.columns[1:]:
        #    studystrucvars[c] = studystrucvars[c].str.replace('left_only', '').str.replace('both', 'YES')
        #studystrucvars.to_csv(i+"_elements_by_site.csv",index=False)
        #sitestructs=pd.concat([sitestructs,studystrucvars],axis=0)
        #print(studystrucvars.variable)

common=e0.copy()
common['BANDA']='YES'
common['BANDA PARENTS']='YES'
common['DAM']='YES'
common['MDD']='YES'
common['DES']='YES'
common['nda_structure']='All'

Encyclopedia=pd.concat([common,e3]).copy()
Cognitive=['er4001','deldisk01','dccs01','pcps01','flanker01','lswmt01','orrt01','pwmt01','pmat01','psm01','tpvt01','webneuro01']
MoodAndAnxiety=['panas01','mini01','bai01','swis01','phq01','gad701','hrsd01','dass01','qids01','shaps01','strain01']
Emotion=['tlbx_sadness01','self_effic01','tlbx_socwit01']
Temperment=['bisbas01','nffi01','apath01']
Dems=['demographics02','ndar_subject01','fhs01','All','genomics_subject02']
Other=['edinburgh_hand01','chaphand01','tanner_sms01']
Diagnostic=['ksads_diagnosesp201','ksads_diagnosesp01','ksads_diagnoses01','ksads_diagnoses201']
Functioning=['cssrs01','psqi01']

Encyclopedia['Domain']=''
Encyclopedia.loc[Encyclopedia.nda_structure.isin(Cognitive),'Domain']='Cognitive'
Encyclopedia.loc[Encyclopedia.nda_structure.isin(MoodAndAnxiety),'Domain']='Mood and Anxiety'
Encyclopedia.loc[Encyclopedia.nda_structure.isin(Emotion),'Domain']='Emotion'
Encyclopedia.loc[Encyclopedia.nda_structure.isin(Temperment),'Domain']='Temperment'
Encyclopedia.loc[Encyclopedia.nda_structure.isin(Dems),'Domain']='Demographics and Sample Characteristics'
Encyclopedia.loc[Encyclopedia.nda_structure.isin(Other),'Domain']='Other'
Encyclopedia.loc[Encyclopedia.nda_structure.isin(Diagnostic),'Domain']='Diagnostic'
Encyclopedia.loc[Encyclopedia.nda_structure.isin(Functioning),'Domain']='Functioning'

reorder=['Domain','nda_structure','nda_element','variable','description','valueRange','notes','BANDA','BANDA PARENTS','DAM','MDD','DES']

#NDA formatting errors
Encyclopedia.loc[Encyclopedia.Domain.str.contains('Slowness of thought and speech; impair')]

Encyclopedia=Encyclopedia.loc[~(Encyclopedia.Domain.isin(['(Slowness of thought and speech; impair','ed ability','to concentrate; decr','eased motor activity).']))]
#,'ed ability','to concentrate; decr','eased motor activity).']))]

Encyclopedia[reorder].to_csv(os.path.join(root_dir,"Freeze1_NDA_DataDictionary_wSites.csv"),index=False)




######################################
#skeleton for manipulating inot REDCap format
# for a given structure (shortname), grab all the metadata for a list of elements as a dataframe
def temp():
    encyclopedia['Section Header']=''
    encyclopedia['Field Type']=''
    encyclopedia['Choices, Calculations, OR Slider Labels']=''
    encyclopedia=encyclopedia.rename(columns={'variable':'Variable / Field Name','nda_structure':'Form / Instrument','description':'Field Label'} )

    #The first five
    common=common.rename(columns={'variable':'Variable / Field Name','nda_structure':'Form / Instrument','description':'Field Label'} )
    common['Form / Instrument']='common'

    mandatory=list(common.variable)
    others=[v for v in encyclopedia['Variable / Field Name'] if v not in mandatory]

    Encyclopedia=encyclopedia.loc[encyclopedia['Variable / Field Name'].isin(others)].drop_duplicates(subset=['Variable / Field Name','Form / Instrument']).reset_index().drop(columns=['index'])

    Encyclopedia.valueRange=Encyclopedia.valueRange.str.replace(" :",":").str.replace(": ",":").str.replace(" ;",";").str.replace("; ",";")
    Encyclopedia.notes=Encyclopedia.notes.str.replace(" =","=").str.replace("= ","=").str.replace(" ;",";").str.replace("; ",";")
    #Encyclopedia.loc[Encyclopedia['Variable / Field Name']=='adhddxoverride']
    #Encyclopedia.loc[Encyclopedia['Variable / Field Name']=='apath_19'][['Field Label']]

    Encyclopedia.loc[((Encyclopedia.valueRange.str.strip().isnull()==False) & (Encyclopedia.notes.isnull()==False)) & (Encyclopedia.valueRange.str.replace(':','').str.replace(';','').str.strip().str.isnumeric()),'Choices, Calculations, OR Slider Labels']=Encyclopedia.notes.str.replace(";",'|').str.replace("=",",")
    Encyclopedia.loc[((Encyclopedia.valueRange.str.strip().isnull()==False) & (Encyclopedia.notes.isnull()==True)),'Choices, Calculations, OR Slider Labels']=''
    Encyclopedia.loc[((Encyclopedia.valueRange.str.strip().isnull()==True) & (Encyclopedia.notes.isnull()==False)),'Choices, Calculations, OR Slider Labels']=''
    #Encyclopedia.loc[Encyclopedia['Variable / Field Name']=='apath_19'][['Field Label']]
    #Encyclopedia.loc[Encyclopedia['Variable / Field Name']=='adhddxoverride']


    Test=Encyclopedia.copy()
    Fixed=pd.read_csv(os.path.join(root_dir,'FixedVariables.csv'))
    Test=Test.merge(Fixed,on=['Variable / Field Name','Form / Instrument','Field Label','valueRange','notes'],how='left',suffixes=('', '_y'),indicator=True)#,'valueRange','notes'])
    #Test.loc[Test['Variable / Field Name']=='adhddxoverride']
    #Test.loc[Test['Choices, Calculations, OR Slider Labels_y'].isnull()==False]
    #Test.loc[Test['Variable / Field Name']=='apath_19'][['Field Label']]

    Test.loc[Test._merge=='both','Choices, Calculations, OR Slider Labels']=Test['Choices, Calculations, OR Slider Labels_y']
    Test=Test.drop(columns=['Field Type_y','Section Header_y','Choices, Calculations, OR Slider Labels_y'])
    #Test.loc[Test['Variable / Field Name']=='apath_19'][['Field Label']]
    Test.loc[Test['Variable / Field Name']=='adhddxoverride']

    RedDict=pd.concat([common,Test.sort_values('Form / Instrument')]).reset_index().drop(columns=['index'])
    RedDict.loc[RedDict.notes.isnull()==False,'Field Label']=RedDict['Field Label']+'-----'+RedDict['notes']
    #RedDict.loc[RedDict['Variable / Field Name']=='apath_19'][['Field Label']]

    RedDict=RedDict.reset_index().drop(columns=['index','_merge'])
    RedDict.loc[RedDict['Variable / Field Name']=='adhddxoverride']

    sections=RedDict.loc[RedDict['Form / Instrument'].duplicated()==True][['Form / Instrument']].drop_duplicates(keep='first').index-1
    #others=[a for a in list(RedDict.index) if a not in sections]
    #others=list(set(RedDict.index)-set(list(sections)))
    RedDict.loc[sections,'Section Header']=RedDict['Form / Instrument']
    RedDict.loc[RedDict['Variable / Field Name']=='adhddxoverride']

    #for variables that are reused across structures,
    RedDict['mapto']=''
    RedDict.loc[RedDict['Variable / Field Name'].isnull()==False,'mapto']=RedDict['Form / Instrument']+'_'+RedDict['Variable / Field Name']
    #save the mapping
    RedDict[['Variable / Field Name','mapto']].to_csv(os.path.join(root_dir,"VariableMap2RedCap.csv"),index=False)
    RedDict.drop(columns=['mapto'])
    RedDict.loc[RedDict['Variable / Field Name'].isnull()==False,'Variable / Field Name']=RedDict['Form / Instrument']+'_'+RedDict['Variable / Field Name']
    RedDict.loc[RedDict['Variable / Field Name'].str.contains('adhddxoverride')]


    #now get list of instruments from online table and harmonization, and create 'Forms'
    # assign structures to forms, where available
    # create longitudinal database two arms (case control)
    # import inventory ids
    # import Lexi's dataset

    Cognitive=['dccs01.txt','pcps01.txt','flanker01.txt','lswmt01.txt','orrt01.txt','pwmt01.txt','pmat01.txt','psm01.csv','tpvt01.csv']
    cog2=[j.strip(".csv") for j in [i.strip(".txt") for i in Cognitive]]
    MoodAndAnxiety=['hrsd01.csv','dass01.csv','qids01.csv','shaps01.txt','strain01.txt']
    mood2=[j.strip(".csv") for j in [i.strip(".txt") for i in MoodAndAnxiety]]
    Temperment=['bisbas01.txt','bisbas01.txt','nffi01.txt']
    temp2=[j.strip(".csv") for j in [i.strip(".txt") for i in Temperment]]
    Dems=['demographics02.txt','edinburgh_hand01.csv','chaphand01.txt']
    dems2=[j.strip(".csv") for j in [i.strip(".txt") for i in Dems]]
    sortedlist=Cognitive+MoodAndAnxiety+Temperment+Dems
    sortedlist=sortedlist+['common']
    notlist=[i for i in list(RedDict['Form / Instrument'].unique()) if (i not in sortedlist)]

    RedDict['Form_old']=RedDict['Form / Instrument']
    RedDict.loc[RedDict['Form / Instrument'].str.replace('.txt','').str.replace(".csv","").isin(cog2),'Form / Instrument']='Cognitive'
    RedDict.loc[RedDict['Form / Instrument'].str.replace('.txt','').str.replace(".csv","").isin(mood2),'Form / Instrument']='Mood_and_Anxiety'
    RedDict.loc[RedDict['Form / Instrument'].str.replace('.txt','').str.replace(".csv","").isin(temp2),'Form / Instrument']='Temperment'
    RedDict.loc[RedDict['Form / Instrument'].str.replace('.txt','').str.replace(".csv","").isin(dems2),'Form / Instrument']='Sample_Characteristics'
    RedDict.loc[RedDict['Form / Instrument'].str.replace('.txt','').str.replace(".csv","").isin(notlist),'Form / Instrument']='Unassigned'


    reorder=['Variable / Field Name', 'Form / Instrument', 'Section Header','Field Type', 'Field Label', 'Choices, Calculations, OR Slider Labels']
    RedDict[reorder].to_csv(os.path.join(root_dir,"REDCap_DataDictionary.csv"),index=False)
