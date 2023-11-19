
#pulling out common functions
def drop999cols(df,verbose=False):
    col_condition = df[(df == 999) | (df == '999') | (df == np.nan) | (df == np.NaN) ].count() / df.shape[0] < 1
    col_select = col_condition.reset_index()
    col_select.columns = ['element', 'select']
    keepcols = col_select.loc[col_select.select == True].element.tolist()
    dropcols = col_select.loc[col_select.select == False].element.tolist()
    print("before 999drop", df.shape)
    if verbose:
        print("dropped columns 999",dropcols)
    df = df[keepcols].copy()
    print("after 999drop", df.shape)
    col_condition2 = df.isnull().sum() / df.shape[0] < 1
    col_select2 = col_condition2.reset_index()
    col_select2.columns = ['element', 'select']
    keepcols2 = col_select2.loc[col_select2.select == True].element.tolist()
    dropcols2 = col_select2.loc[col_select2.select == False].element.tolist()
    df = df[keepcols2].copy()
    if verbose:
        print("dropped columns Null", dropcols2)
    print("after nulldrop", df.shape)
    return df
def partialcrosswalkmap(mergelist,df,prefix,AllVADA):
    before = [i for i in list(df.columns) if i not in mergelist]
    after = [prefix + '_' + i for i in before]
    map = dict(zip(before, after))
    df = df.rename(columns=map)
    V = pd.DataFrame.from_dict(map, orient='index').reset_index()
    V.columns = ['element', 'variable']
    #print('V:', prefix)
    V['structure'] = prefix
    AllVADA = pd.concat([AllVADA, V], axis=0)
    return map,df,AllVADA

def droprows(df,mergelist):
    extra = []
    if 'study' in df.columns:
        extra = extra+['study']
    if 'version' in df.columns:
        extra=extra+['version']
    na = [c for c in df.columns if c not in mergelist + extra]
    df = df.dropna(axis=0, how='all', subset=na).copy()
    print("after drop rows with completely empty columns in non-mergelist fields:",df.shape)
    df = df.drop_duplicates(keep='first', inplace=False, ignore_index=True).copy()
    print("after dropping identical duplicates of rows:",df.shape)
    return df,extra

def MDDdupfix(df):
    extra=[]
    if 'version' in df.columns:
        extra=['version']
    dupfix = df[['src_subject_id', 'subjectkey', 'interview_date', 'interview_age', 'sex']].sort_values(
        ['subjectkey', 'interview_date']).drop_duplicates(subset=['subjectkey', 'interview_date'], keep='first')
    df = pd.merge(df.drop(columns='interview_age'), dupfix,
                  on=['src_subject_id', 'subjectkey', 'interview_date', 'sex'], how='outer')
    df = df.drop_duplicates(subset=mergelist+extra, keep='last', inplace=False, ignore_index=True).copy()
    print("after MDD dupfix:",df.shape)
    return df
