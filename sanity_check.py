import os
import pandas as pd


root_dir = '/Harmony_non_imaging_data_sharing'
banda_parent = pd.read_csv(os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckBANDA_Parents_Sanity.csv'))
banda_children = pd.read_csv(os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckBANDA_Sanity_Child.csv'))
dam = pd.read_csv(os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckDAM_Sanity.csv'))
des = pd.read_csv(os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckDES_Sanity.csv'))
mdd = pd.read_csv(os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckMDD_Sanity.csv'))


# check number of subjects, gender
def summarize_data(df, index):
    num_subject_id = df['src_subject_id'].nunique()
    num_subject_key = df['subjectkey'].nunique()
    df.sort_values(by='src_subject_id')
    df_unique_subjectkey = df.drop_duplicates(subset='subjectkey', keep="first")
    num_female = df_unique_subjectkey[df_unique_subjectkey['sex'] == 'F'].shape[0]
    num_male = df_unique_subjectkey[df_unique_subjectkey['sex'] == 'M'].shape[0]

    summary_df = pd.DataFrame({
        'Unique src_subject_id': num_subject_id,
        'Unique subjectkey': num_subject_key,
        'Number of Females': num_female,
        'Number of Males': num_male
    }, index=[index])

    return summary_df


df1 = summarize_data(banda_children, 'banda_children')
df2 = summarize_data(banda_parent, 'banda_parent')
df3 = summarize_data(des, 'DES')
df4 = summarize_data(dam, 'DAM')
df5 = summarize_data(mdd, 'MDD')
summary_df = pd.concat([df1, df2, df3, df4, df5], axis=0)
print(summary_df)

df = des[des['sex'] == 'O']

# check timepoint
df = pd.read_csv(os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/NDA_structures_table_combined.csv'))
num_subject_parent = df['subjectkey'].nunique()
num_subject_parent = df['src_subject_id'].nunique()
print(num_subject_parent)

des_timepoint = des.groupby('subjectkey')['interview_date'].nunique().reset_index()
dam_timepoint = dam.groupby('subjectkey')['interview_date'].nunique().reset_index()
mdd_timepoint = mdd.groupby('subjectkey')['interview_date'].nunique().reset_index()
banda_parent_timepoint = banda_parent.groupby('subjectkey')['interview_date'].nunique().reset_index()
banda_children_timepoint = banda_children.groupby('subjectkey')['interview_date'].nunique().reset_index()

des_timepoint['label'] = 'DES'
dam_timepoint['label'] = 'DAM'
mdd_timepoint['label'] = 'MDD'
banda_parent_timepoint['label'] = 'Banda Parent'
banda_children_timepoint['label'] = 'Banda Children'

combined_df = pd.concat([des_timepoint, dam_timepoint, mdd_timepoint, banda_parent_timepoint, banda_children_timepoint])
import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(12, 6))
sns.boxplot(x='label', y='interview_date', data=combined_df)
plt.title('Distribution of Number of Interview Dates across Different Datasets')
plt.xlabel('Dataset')
plt.ylabel('Number of Interview Dates')
plt.show()

# check cols
# Checking for columns that are completely missing or have a mixture of missing codes
file_paths = {
    "CheckBANDA_Parents_Sanity": os.path.join(root_dir,
                                              'Harmony_non_imaging_data_sharing/CheckBANDA_Parents_Sanity.csv'),
    "CheckBANDA_Sanity_Child": os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckBANDA_Sanity_Child.csv'),
    "CheckDAM_Sanity": os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckDAM_Sanity.csv'),
    "CheckDES_Sanity": os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckDES_Sanity.csv'),
    "CheckMDD_Sanity": os.path.join(root_dir, 'Harmony_non_imaging_data_sharing/CheckMDD_Sanity.csv'),
    "NDA_structures_table_combined": os.path.join(root_dir,
                                                  'Harmony_non_imaging_data_sharing/NDA_structures_table_combined.csv'),
    "NDA_structures_variables_combined": os.path.join(root_dir,
                                                      'Harmony_non_imaging_data_sharing/NDA_structures_variables_combined.csv')
}

# Load each file into a DataFrame
dataframes = {name: pd.read_csv(path) for name, path in file_paths.items()}


def missing_data_report(df):
    missing_data = df.isnull().mean() * 100
    missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
    return missing_data


# Generating missing data reports for each DataFrame
missing_reports = {name: missing_data_report(df) for name, df in dataframes.items()}

# Displaying the top 5 columns with the most missing data for each DataFrame as a sample
for name, report in missing_reports.items():
    report_df = pd.DataFrame(report).reset_index()
    report_df.columns = ['Column', 'MissingPercentage']
    file_name= name+'_colcheck.csv'
    report_df.to_csv(os.path.join(root_dir, file_name))

for name, df in dataframes.items():
    print(f"Before dropping columns in '{name}':", df.shape)
    cleaned_df = df.dropna(axis=1, how='all')
    dataframes[name] = cleaned_df
    print(f"After dropping columns in '{name}':", cleaned_df.shape, "\n")

for name, report in missing_reports.items():
    report_df = pd.DataFrame(report).reset_index()
    report_df.columns = ['Column', 'MissingPercentage']
    print(f"{name} - Top 5 columns with missing data:")
    print(report_df.head(5), "\n")


# merge issue
# Checking for signs of merge issues, such as having an _x and _y version of the same variables

def check_merge_issues(df):
    merge_issue_vars = []
    for col in df.columns:
        if col.endswith('_x') or col.endswith('_y'):
            base_var_name = col.rsplit('_', 1)[0]
            if f"{base_var_name}_x" in df.columns and f"{base_var_name}_y" in df.columns:
                merge_issue_vars.append(base_var_name)

    return list(set(merge_issue_vars))


# Checking each DataFrame for merge issues
merge_issues_reports = {name: check_merge_issues(df) for name, df in dataframes.items()}

# Displaying the merge issue reports for each DataFrame
print(merge_issues_reports)
