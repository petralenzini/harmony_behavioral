import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"

#ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
#DAM_dir = os.path.join(root_dir, "ANXPE_Sheline")
#MDD_dir = os.path.join(root_dir, "MDD_Narr")

#ADA_files = os.listdir(ADA_dir)  # txt
#DAM_files = os.listdir(DAM_dir)
#MDD_files = os.listdir(MDD_dir)

#form_name = "pcps01"

#ADA_full_form_name = form_name + '.txt'
#form_ADA = pd.read_csv(os.path.join(ADA_dir, ADA_full_form_name), delimiter='\t', header=1)
#form_ADA['study']='BANDA'
#print(form_ADA)

#DAM_full_form_name = form_name + '.csv'
#form_DAM = pd.read_csv(os.path.join(DAM_dir, DAM_full_form_name), header=1)
#form_DAM['study']='ANXPE'
#print(form_DAM)

#MDD_full_form_name = form_name + '.csv'
#form_MDD = pd.read_csv(os.path.join(MDD_dir, MDD_full_form_name), header=1)
#form_MDD['study']='MDD'
#print(form_MDD)

#replace with stack from other code
stack=pd.concat([form_MDD,form_DAM,form_ADA],axis=0)
stack=stack.reset_index().drop(columns=['index','level_0'])

#i='nih_tlbx_fctsc'
for i in list(stack.columns)[6:25]: #[6:]
    try:
        sns.histplot(stack[[i,'study']],x=i,hue='study')##,x='nih_tlbx_fctsc')#,hue='study')
        plt.savefig(os.join(root_dir,+ "/plots/" + i))  # , *, dpi='figure', format=None, metadata=None,
        #save this figure
        plt.show()
        #open a file for appending
        f = open(os.join(root_dir,+ "Variable_Descriptions" + i)) + ".txt", "a")
        print(" ",file=f)
        print(stack[i].describe(),file=f)
        f.close()
        print("*****************",file=f)

    except:
        print("Couldn't produce statistics for ",i)

