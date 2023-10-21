import pandas as pd
import os
import requests
import json
import re

root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
#replace this with list from Lexi
#list from Lexi varfile=[]
variables=list(pd.read_csv(os.path.join(root_dir,"DataDictionaryDraft.csv"), header=None)[0])

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

s=getNDAdetails(apath01)
try:
    os.makedirs(os.path.join(root_dir,'nda_annotation'))
except:
    pass

ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
DAM_dir = os.path.join(root_dir, "ANXPE_Sheline")
MDD_dir = os.path.join(root_dir, "MDD_Narr")

ADA_files = os.listdir(ADA_dir)  # txt
DAM_files = os.listdir(DAM_dir)
MDD_files = os.listdir(MDD_dir)

structurelist=[]
for i in list(os.listdir(ADA_dir))+list(os.listdir(DAM_dir)+ os.listdir(MDD_dir)):
    if ('csv' in i or 'txt' in i) and ('definitions' not in i):
        #print(i)
        try:
            j=os.path.splitext(i)[0]
            structurelist=structurelist+[j]
        except:
            pass


structurelist=list(set(structurelist))
encyclopedia=pd.DataFrame()
for s in structurelist:
    print(s)
    try:
        news=getNDAdetails(structure_name=s)
        encyclopedia=pd.concat([encyclopedia,news],axis=0)
        print("passed.......")
    except:
        print("Sorry...couldn't download data dictionary for",s)

# for a given structure (shortname), grab all the metadata for a list of elements as a dataframe
encyclopedia['Section Header']=''
encyclopedia['Field Type']=''
encyclopedia['Choices, Calculations, OR Slider Labels']=''
encyclopedia=encyclopedia.rename(columns={'nda_element':'Variable / Field Name','nda_structure':'Form / Instrument','description':'Field Label'} )

#The first five
common=pd.DataFrame(encyclopedia.head(5))
common['Form / Instrument']='common'

mandatory=list(common['Variable / Field Name'])
others=[v for v in variables if v not in mandatory]

Encyclopedia=encyclopedia.loc[encyclopedia['Variable / Field Name'].isin(others)].drop_duplicates(subset=['Variable / Field Name','Form / Instrument']).reset_index().drop(columns=['index'])
#Encyclopedia.loc[Encyclopedia['Variable / Field Name']=='adhddxoverride']
#Encyclopedia.loc[Encyclopedia['Variable / Field Name']=='apath_19'][['Field Label']]

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
