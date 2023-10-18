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

Encyclopedia.valueRange=Encyclopedia.valueRange.str.replace(" :",":").str.replace(": ",":").str.replace(" ;",";").str.replace("; ",";")
Encyclopedia.notes=Encyclopedia.notes.str.replace(" =","=").str.replace("= ","=").str.replace(" ;",";").str.replace("; ",";")

Encyclopedia.loc[((Encyclopedia.valueRange.str.strip().isnull()==False) & (Encyclopedia.notes.isnull()==False)) & (Encyclopedia.valueRange.str.replace(':','').str.replace(';','').str.strip().str.isnumeric()),'Choices, Calculations, OR Slider Labels']=Encyclopedia.notes.str.replace(";",'|').str.replace("=",",")
Encyclopedia.loc[((Encyclopedia.valueRange.str.strip().isnull()==False) & (Encyclopedia.notes.isnull()==True)),'Choices, Calculations, OR Slider Labels']=''
Encyclopedia.loc[((Encyclopedia.valueRange.str.strip().isnull()==True) & (Encyclopedia.notes.isnull()==False)),'Choices, Calculations, OR Slider Labels']=''

Fixed=pd.read_csv(os.path.join(root_dir,'FixedVariables.csv'))
Encyclopedia.update(Fixed,overwrite=True)

RedDict=pd.concat([common,Encyclopedia.sort_values('Form / Instrument')]).reset_index().drop(columns=['index'])

reorder=['Variable / Field Name', 'Form / Instrument', 'Section Header','Field Type', 'Field Label', 'Choices, Calculations, OR Slider Labels','valueRange', 'notes']
RedDict[reorder].to_csv(os.path.join(root_dir,"REDCap_DataDictionary.csv"),index=False)
