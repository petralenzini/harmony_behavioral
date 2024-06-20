import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import scipy.stats as stats

root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral_Feb2024"
stackfile="NDA_structures_table_combined.csv"
stackvars="NDA_structures_variables_combined.csv"
stackall=pd.read_csv(os.path.join(root_dir,stackfile),low_memory=False).replace(999, np.NaN).replace(-999, np.NaN)
#stack.tpvt01_tbx_vocab_se.value_counts()
stackj=stackall.drop_duplicates(subset='src_subject_id')

vars=pd.read_csv(os.path.join(root_dir,stackvars))
structsunique=vars.drop_duplicates(subset=['structure','study'])
structstudies=pd.DataFrame(structsunique.structure.value_counts())
structs2check=structstudies.loc[structstudies.structure>1].index

for struct in structs2check:
    print(struct)
    varstudies = pd.DataFrame(vars.loc[vars.structure==struct].variable.value_counts())
    cols2check = varstudies.loc[varstudies.variable > 1].index

    f = open(os.path.join(root_dir, "plots", struct + "_Variable_Descriptions.txt"), "a")
    for i in [i for i in cols2check if struct in i and 'version' not in i and 'wcst' not in i and 'language' not in i and 'pin' not in i]:
        print(i)
        try:
            stack = stackj#.loc[stackj[i].isin(+) == False]
            l=len(stack[i].value_counts())
            if l>1:
                try:
                    print("**************************************",file=f)
                    print(struct,"element: ",i)
                    print(stack.groupby('study')[i].describe(), file=f)
                    if stack[i].value_counts().shape[0] < 8:
                        p = 0
                        nl = "\n"
                        crosstab = pd.crosstab(stack['study'], stack[i])
                        chi2, cp, dof, expected = stats.chi2_contingency(crosstab)
                        print(f"Chi2-p-value= {cp}{nl}", file=f)
                    if stack[i].value_counts().shape[0] >= 8:
                        cp = 0
                        nl = "\n"
                        s = stack[[i, 'study']].loc[stack[i].isnull() == False]
                        s2 = s.assign(idx=s.groupby('study').cumcount()).pivot(index='idx', columns='study', values=i)
                        try:
                            w = list(s2['BANDA'].loc[s2.BANDA.isnull() == False])
                            W = True
                        except:
                            W = False
                        try:
                            x = list(s2['DAM'].loc[s2.DAM.isnull() == False])
                            X = True
                        except:
                            X = False
                        try:
                            y = list(s2['MDD'].loc[s2.MDD.isnull() == False])
                            Y = True
                        except:
                            Y = False
                        try:
                            z = list(s2['DES'].loc[s2.STACT.isnull() == False])
                            Z = True
                        except:
                            Z = False
                        if W and X and Y and Z:
                            k, p = stats.kruskal(w, x, y, z)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if not W and X and Y and Z:
                            k, p = stats.kruskal(x, y, z)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if W and not X and Y and Z:
                            k, p = stats.kruskal(w, y, z)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if W and X and not Y and Z:
                            k, p = stats.kruskal(w, x, z)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if W and X and Y and not Z:
                            k, p = stats.kruskal(w, x, y)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if W and X and not Y and not Z:
                            k, p = stats.kruskal(w, x)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if W and not X and Y and not Z:
                            k, p = stats.kruskal(w, y)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if W and not X and not Y and Z:
                            k, p = stats.kruskal(w, z)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if not W and X and Y and not Z:
                            k, p = stats.kruskal(x, y)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if not W and X and not Y and Z:
                            k, p = stats.kruskal(x, z)
                            a, pa = stats.f_oneway(w, x, y, z)
                        if not W and not X and Y and Z:
                            k, p = stats.kruskal(y, z)
                            a, pa = stats.f_oneway(w, x, y, z)
                        print(f"kruskal p-value= {p}{nl}", file=f)
                        print(f"anova p-value= {pa}{nl}", file=f)
                except:
                    print("Couldn't produce statistics for ", i, file=f)
                fig, axs = plt.subplots(nrows=5,ncols=1,figsize=(7,15),sharex=True,sharey=True)
                for s in ['BANDA','MDD','DAM','DES']:
                    if s=='BANDA':
                        si=0
                    sip=si+511
                    #print(sip)
                    plt.subplot(sip)
                    try:
                        sns.histplot(stack.loc[stack.study==s][[i]], x=i, stat='percent')
                        plt.title(s)
                    except:
                        pass #print(s,"does not have",i)
                    si=si+1
                sip=si+511
                plt.subplot(sip)
                sns.histplot(stack[[i,'study']],x=i,stat='percent',hue='study')
                if p>0:
                    plt.title('Overlap kruskal-pvalue '+str(p))
                if cp>0:
                    plt.title('Overlap Chisquared-pvalue '+str(cp))
                fig.tight_layout()
                plt.savefig(os.path.join(root_dir, 'plots', i))  # , *, dpi='figure', format=None, metadata=None,
                plt.show()
        except:
            print("Coundn't process:",i)
#now compare variable lists of variables for structures with more than 1 representative study:
for i in structs2check:
    strucvars=vars.loc[vars.structure==i]
    studystrucvars = strucvars[['variable']].drop_duplicates()
    print("*****************")
    print("Structure:", i)
    for s in list(strucvars.study.unique()):
        print("Study:",s)
        studystrucvars=studystrucvars.merge(strucvars.loc[strucvars.study==s][['variable']],on='variable',how='outer',indicator=s)
        for c in studystrucvars.columns[1:]:
            studystrucvars[c] = studystrucvars[c].str.replace('left_only', 'NO').str.replace('both', 'YES')
        studystrucvars.to_csv(i+"_elements_by_site.csv",index=False)
        #print(studystrucvars.variable)






