import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

root_dir = "/Users/yuanyuanxiaowang/PycharmProjects/pythonProject/Harmony/harmony_behavioral" \
           "/Harmony_non_imaging_data_sharing"


ADA_dir = os.path.join(root_dir, "BANDA_Whitfield_Gabrieli")
DAM_dir = os.path.join(root_dir, "ANXPE_Sheline")
MDD_dir = os.path.join(root_dir, "MDD_Narr")

ADA_files = os.listdir(ADA_dir)  # txt
DAM_files = os.listdir(DAM_dir)
MDD_files = os.listdir(MDD_dir)

form_name = "pcps01"

ADA_full_form_name = form_name + '.txt'
form_ADA = pd.read_csv(os.path.join(ADA_dir, ADA_full_form_name), delimiter='\t', header=0)
print(form_ADA)

DAM_full_form_name = form_name + '.csv'
form_DAM = pd.read_csv(os.path.join(DAM_dir, DAM_full_form_name), header=1)
print(form_DAM)

MDD_full_form_name = form_name + '.csv'
form_MDD = pd.read_csv(os.path.join(MDD_dir, MDD_full_form_name), header=1)
print(form_MDD)

list(form_ADA)
list(form_DAM)
list(form_MDD)
set(list(form_MDD))&set(list(form_DAM))

form_MDD['nih_tlbx_fctsc'].plot.bar()
plt.show()

form_DAM['nih_tlbx_fctsc'].plot.bar()
plt.show()

mdd = form_MDD[['nih_tlbx_fctsc']].copy()
mdd['label'] = 'MDD'

dam = form_DAM[['nih_tlbx_fctsc']].copy()
dam['label'] = 'DAM'

result_df = pd.concat([mdd, dam], ignore_index=True)
plt.figure(figsize=(10,6))
ax = sns.boxplot(x='label', y='nih_tlbx_fctsc', data=result_df)
ax.set_title('Boxplot of nih_tlbx_fctsc')
plt.show()
