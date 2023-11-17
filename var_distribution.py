import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
stackfile="NDA_structures_table_combined.csv"
stackvars="NDA_structures_variables_combined.csv"
stack=pd.read_csv(os.path.join(root_dir,stackfile))
vars=pd.read_csv(os.path.join(root_dir,stackvars))
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

