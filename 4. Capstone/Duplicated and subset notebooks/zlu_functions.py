import numpy as np
import re
import pandas as pd

def df_column_uniquify(df):
    df_columns = df.columns
    new_columns = []
    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = item+','*counter
        new_columns.append(newitem)
    df.columns = new_columns
    return df

def del_subset_col(df):
    if df.shape[0]==0 or df.shape[1]<=1:
        return pd.DataFrame()
    counter=0
    for col_idx in range(df.shape[1]):
        col_idx=col_idx-counter
        col_lst=list(range(df.shape[1]))
        col_lst.remove(col_idx)
        #delete column col_idx if it is a subset of another column
        for i in col_lst:
            name1=re.sub('[\s,\.\d]+','',df.iloc[:,col_idx].name).lower()
            name2=re.sub('[\s,\.\d]+','',df.iloc[:,i].name).lower()
            if all([df.iloc[k,col_idx] in v for k,v in enumerate(df.iloc[:,i].values)]) \
            and name1 in name2: #both value and name are a subset of another column
                counter+=1
                df=df.iloc[:,col_lst]
                break
    return df



def get_index_with_values(df, axis=0):
    indx = df.apply(lambda ax: any(ax.str.contains('\w')), axis=axis)
    indx=indx.reset_index(drop=True)
    if axis==0: indx[0]=True
    indx = indx[indx == True].index
    return indx

def remove_empty_rows_and_cols(df):
    cols_index = get_index_with_values(df,0)
#     print(cols_index)
    rows_index = get_index_with_values(df, 1)
    return df.iloc[rows_index, cols_index]

def allstr_rows_to_col_headers(df):
    for i in range(df.shape[0]):
        df.iloc[i]=df.iloc[i].str.lower()
        cond0 = sum(df.iloc[i].str.contains('\w'))==1
        cond=any(df.iloc[i].str.contains('swap') )or any(df.iloc[i].str.contains('collar')) or any(df.iloc[i].str.contains('put')) \
        or any(df.iloc[i].str.contains('oil')) or any(df.iloc[i].str.contains('gas')) or any(df.iloc[i].str.contains('ngl')) #new
        if not all([True if re.search('2\d{3}',col)!=None and re.search('\d2\d{3}',col)==None else re.search('\d',col)==None for col in df.iloc[i]]) \
        or (cond and cond0):
            break
    if i>0: #collapse rows to get column names
        list1=["" if 'Unnamed' in col else col for col in df.columns]
        list2=df.apply(lambda x: ",".join(x[:i]))
        df.columns=[str(a) + str(b) for a,b in zip(list1,list2)]
        df=df.iloc[i:]
    return df


def set_col_headers(df):

    df=remove_empty_rows_and_cols(df)
    df.columns=[re.sub('\.\d{1}',',',col) for col in df.columns]
    df=df.applymap(lambda x: re.sub(r'\(\d{1}\)',",",x))
    if df.shape[0]==0:
        return pd.DataFrame()

    if any(['Unnamed' in col for col in df.columns]):
        df.columns=["" if 'Unnamed' in col else col for col in df.columns]
        if np.all([col.replace(',','')=='' for col in df.columns]): #column headers are missing
            df.columns=df.iloc[0]
            df=df.iloc[1:]
            if df.shape[0]==0:
                return pd.DataFrame()
        df=allstr_rows_to_col_headers(df)

    df=remove_rows_for_total_value(df) #if any row for total value; save it
    df=remove_cols_for_fair_value(df)
    if df.shape[0]==0 or df.shape[1]<=1:
        return pd.DataFrame()
    cond0 = re.search( '\w',df.iloc[0,0])==None and all([re.search( '\w',col)==None for col in df.iloc[0,1:]]) #only the first column is not empty; ignore last column because it can be a total column
    if df.shape[1]>2:
        cond1 = df.iloc[0,0]==df.iloc[0,1] and not any(df.iloc[0,2:])
    else:
        cond1 = False #special case: first two cells are identical
    cond2= (not any(df.iloc[0,:].str.contains('\d'))) and any(df.iloc[0,:].str.contains('thousands')) #first row contains "thousands"
    cond3= (not any(df.iloc[0,:].str.replace('000','').str.contains('\d'))) or \
    all([True if re.search('2\d{3}',col)!=None and re.search('\d2\d{3}',col)==None else re.search('\d',col)==None for col in df.iloc[0]]) #first row contains "in 000s" or dates

    # print([True if re.search('2\d{3}',col)!=None else re.search('\d',col)==None for col in df.iloc[0]])
    if (cond2 or cond3) and not (cond0 or cond1):
        df=allstr_rows_to_col_headers(df)
    return df

def row_header_to_col(df):
    if df.shape[0]==0 or df.shape[1]<=1:
        return pd.DataFrame()
    row_lst=[]
    count_prod=0
    count_inst=0
    count_add=0
    count=0
    kw_prod=''
    kw_add=''
    kw_inst=''

    for i in range(df.shape[0]):
        cond1 = df.iloc[i,0] and not any(df.iloc[i,1:-1]) #only the first column is not empty; ignore last column because it can be a total column
        cond2 = sum(df.iloc[i].unique()!="")==1#special case: first two cells are identical
        if cond1 or cond2:
            kw=df.iloc[i,0].lower()
            product_cond=('oil' in kw or 'gas' in kw or 'ngl' in kw)
            instrument_cond=('swap' in kw or 'collar' in kw \
            or 'option' in kw or 'put' in kw or 'call' in kw)
            if product_cond:
                kw_prod=df.iloc[i,0].lower()
                if count_prod==0: df['product_type']='' #initilization
                count_prod+=1
            elif instrument_cond:
                kw_inst=df.iloc[i,0].lower()
                if count_inst==0: df['instrument_type']='' #initilization
                count_inst+=1
            else:
                kw_add=df.iloc[i,0].lower()
                if count_add==0: df['additional']='' #initilization
                count_add+=1
            row_lst.append(df.index[i])
            count+=1
        elif any(df.iloc[i,1:].apply(lambda x: re.sub(r'\(\d\)?$',"",x)).str.contains('\d')) and count!=0: #regex to exclude (number)
            if count_inst>0: df.loc[df.index[i],df.columns=='instrument_type']=kw_inst
            if count_prod>0: df.loc[df.index[i],df.columns=='product_type']=kw_prod
            if count_add>0: df.loc[df.index[i],df.columns=='additional']=kw_add
    df=df.drop(row_lst)
    return df

def remove_rows_for_total_value(df):
    row_lst=[i for i in range(df.shape[0])]
    del_lst=[]
    for i in range(df.shape[0]):
        empty_cell=any(df.iloc[i,:].str.contains('\d'))
        row_str=re.sub('\s',"",''.join(df.iloc[i,:]).lower())
        if ('total' in row_str or 'fair' in row_str) and empty_cell:
            row_lst.remove(i)
            del_lst.append(i)
    df_del=df.iloc[del_lst]
    df=df.iloc[row_lst]
    #bug: how to save df_total?
    return df

def remove_cols_for_fair_value(df):
    col_lst=[x for x in range(df.shape[1])]
    del_lst=[]
    for i in range(1,df.shape[1]): #skip frist col
        empty_cell=any(df.iloc[:,i]=="")
        col_str=df.columns[i].lower()
        if ('fair' in col_str) and empty_cell:
            col_lst.remove(i)
            del_lst.append(i)
    df_del=df.iloc[:,del_lst]
    df=df.iloc[:,col_lst]
    return df

def dfs_need_transpose(df):
    if df.shape[0]==0 or df.shape[1]<=1 :  #empty df
        return pd.DataFrame()
    vol_col_keywords = ['mmbbls', 'mbbls', 'bbl',
                  'mmcfe', 'mmcf', 'mcfe',
                  'mcf', 'bcfe', 'bcf',
                  'mmbtu', 'mbtu', 'btu',
                  'gj', 'dekatherm', 'volume',
                  'barrel', 'barrle']

    price_vol_keywords = ['weighted', 'price',
                         'ceiling', 'floor']
    row_str=re.sub('\s',"",''.join(df.iloc[:,0]).lower())+df.columns[0]
    col_str=[x.lower() for x in df.columns]
    # print(col_str)

    cond0=any([kw in row_str for kw in vol_col_keywords]) and any([kw in row_str for kw in price_vol_keywords])
    cond1=((all([kw not in col_str for kw in vol_col_keywords]) and all([kw not in col_str for kw in price_vol_keywords])) \
    or sum([re.search('2\d{3}',col)!=None for col in col_str])>1)
    # print(cond0,cond1,sum([re.search('2\d{3}',col)!=None for col in col_str]))
    if  cond0\
    and cond1:
        row_lst=[]
        count=0
        for i in range(df.shape[0]):
            if df.iloc[i,0] and not any(df.iloc[i,1:-1]): #only the first column is not empty
                kw=df.iloc[i,0].lower()
                row_lst.append(df.index[i])
                count+=1
            elif any(df.iloc[i,1:].str.contains('\d')) and count!=0:
                df.iloc[i,0]=kw+df.iloc[i,0]
        df=df.drop(row_lst)

        df=df.T
        df.columns=df.iloc[0]
        df=df.iloc[1:]
    if df.shape[0]==0 or df.shape[1]<=1 :  #empty df
        return pd.DataFrame()
    for i in range(df.shape[0]-1,-1,-1):
        if any([re.search('\d',re.sub('2\d{3}','',col))!=None for col in df.iloc[i]]): break

    if i!=df.shape[0]-1:
        list1=df.columns
        list2=df.apply(lambda x: ",".join(x[i+1:]))
        df.columns=[str(a) +','+ str(b) for a,b in zip(list1,list2)]
        df=df.iloc[:i+1]
    return df

def find_vol_col(df):
    key_words = ['mmbbls', 'mbbls', 'bbl',
              'mmcfe', 'mmcf', 'mcfe',
              'mcf', 'bcfe', 'bcf',
              'mmbtu', 'mbtu', 'btu',
              'gj', 'dekatherm', 'volume',
              'barrel', 'barrle']
    stop_words = ['price', 'sales']
    cols=[col.lower() for col in df.columns]
    for i in range(df.shape[1]):
        if any([kw in cols[i] for kw in key_words]) and all([kw not in cols[i] for kw in stop_words]):
            return i
    return ""

def collar_fill_empty(df):
    col_index= find_vol_col(df)
    if col_index=="": return df
    start=0
    row_lst=[]
    for rowindex in range(df.shape[0]-1):
        cond1=(any(['collar' in x.lower() for x in df.iloc[rowindex+1,:]]) or any(['collar' in x.lower() for x in df.iloc[rowindex,:]]))
        cond2=(any(['way' in x.lower() for x in df.iloc[rowindex+1,:]]) or any(['way' in x.lower() for x in df.iloc[rowindex,:]]))
        if (cond1 or cond2) and df.iloc[rowindex+1,col_index]=="" and  \
        re.search(r'\d', df.iloc[rowindex,col_index])!=None: #current or the row above contains "collar"; volume cell for current row contains a number but next row is empty
            row_lst.append(rowindex)
    if row_lst==[]: return df
    row_lst.append(df.shape[0])
    row_remain=[i for i in range(df.shape[0])]
    for i in range(len(row_lst)-1):
        row_remain.remove(row_lst[i])
        for j in range(row_lst[i]+1,row_lst[i+1]):
            if any(['swap' in x.lower() for x in df.iloc[j]]) or any(['basis' in x.lower() for x in df.iloc[j]]) or \
            re.search('\d',df.iloc[j,col_index])!=None: break
    #         print(df.iloc[row_lst[i]].values,'smae',df.iloc[j].values)
            list1=df.iloc[row_lst[i]].values
            list2=df.iloc[j].values
            df.iloc[j]=[str(a) +','+ str(b) if (a!=b and b!="" and re.search(r'\d', a)!=None and re.search(r'\d', b)==None) else b if (b!="") else a for a,b in zip(list1,list2)]

    df=df.iloc[row_remain]
    return df
