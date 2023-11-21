import os
import pandas as pd

# root_dir = "/Users/petralenzini/work/harmony/harmony_behavioral"
root_dir = "/Users/yuanyuanxiaowang/PycharmProjects/pythonProject/Harmony/harmony_behavioral/Harmony_non_imaging_data_sharing/STACT_Williams"

baseline_dir = os.path.join(root_dir, "baseline")
followup_dir = os.path.join(root_dir, "followups (3,6,9,12 months)")

baseline_files = os.listdir(baseline_dir)
followup_files = os.listdir(followup_dir)

mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']
merged_df = pd.DataFrame(columns=mergelist)
if '.DS_Store' in baseline_files:
    baseline_files.remove('.DS_Store')
for file in baseline_files:
    prefix = os.path.splitext(file)[0]
    if 'ndar01.csv' in file:
        df = pd.read_csv(os.path.join(baseline_dir, file), header=0, encoding='ISO-8859-1')
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        print(prefix, ":", df.shape)
        if merged_df.empty:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on=mergelist, how='outer')
            print("Merged:", merged_df.shape)
    else:
        df = pd.read_csv(os.path.join(baseline_dir, file), header=0)
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        print(prefix, ":", df.shape)
        if merged_df.empty:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on=mergelist, how='outer')
            print("Merged:", merged_df.shape)
if not merged_df.empty:
    merged_df['study'] = 'STACT'

cols = ['src_subject_id', 'interview_date', 'interview_age', 'study', 'sex'] + [col for col in merged_df if
                                                                                col not in ['src_subject_id',
                                                                                            'interview_date', 'sex',
                                                                                            'interview_age', 'study']]
merged_df = merged_df[cols]


def rename_columns(df):
    new_columns = []
    for col in df.columns:
        if 'gad' in col:
            new_columns.append(f'gad701_{col}')
        elif 'phq' in col:
            new_columns.append(f'phq01_{col}')
        elif 'swls' in col:
            new_columns.append(f'swls01_{col}')
        else:
            new_columns.append(col)
    df.columns = new_columns
    return df


mergelist = ['src_subject_id', 'subjectkey', 'interview_date', 'sex']
for file in followup_files:
    prefix = os.path.splitext(file)[0]
    if 'webneuro01_F' in file:
        df = pd.read_csv(os.path.join(followup_dir, file), header=0)
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        before = [i for i in list(df.columns) if i not in mergelist]
        after = [prefix + '_' + i for i in before]
        d = dict(zip(before, after))
        df = df.rename(columns=d)
        df = df.dropna(axis=1, how='all').copy()
        print(prefix, ":", df.shape)
        if merged_df.empty:
            merged_df = df
        else:
            merged_df = pd.merge(merged_df, df, on=mergelist, how='outer')
            print("Merged:", merged_df.shape)
    else:
        df = pd.read_csv(os.path.join(followup_dir, file), header=0)
        df = rename_columns(df)
        df['interview_date'] = pd.to_datetime(df['interview_date'], format='%m/%d/%y').dt.strftime('%m/%d/%Y')
        df = df.dropna(axis=1, how='all').copy()
        print(file, ":", df.shape)
        if merged_df.empty:
            merged_df = df
        else:
            # merged_df = pd.merge(merged_df, df, on=mergelist, how='outer')
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            print("Merged:", merged_df.shape)




