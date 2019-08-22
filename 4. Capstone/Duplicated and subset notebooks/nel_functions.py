from itertools import combinations, permutations, chain, groupby
from collections import defaultdict, Counter
import re
import pandas as pd
import numpy as np
from zlu_functions import set_col_headers


def delete_duplicated(dictionary):
    '''
    This function removes any duplicated dataframe within the same key,
    outputs a dictionary indicating deleted tickers and respective indices.

    dictionary: dictionary with file_header as keys and list of dataframes as values
    '''

    # Initialize a dictionary with list as values to house the indices
    deleted_df = defaultdict(list)

    for k in dictionary.keys():
        target_list = dictionary[k]
        # Extract distinct combinations of indices
        combo_list = list(combinations(range(len(target_list)),2))
        duplicated_index = []

        for df1_index, df2_index in combo_list:
            # Assert that we are comparing two dataframes
            if isinstance(target_list[df1_index], pd.DataFrame) & isinstance(target_list[df2_index], pd.DataFrame):
                # Checks if two dataframes are equal
                df1 = target_list[df1_index].copy()
                df2 = target_list[df2_index].copy()
                df1.columns = np.arange(len(df1.columns))
                df2.columns = np.arange(len(df2.columns))
                if df1.equals(df2):
                    duplicated_index.append(df2_index)

        # Remove duplicated indices in case there are >2 identical dataframes
        to_be_del = list(set(duplicated_index))
        # We want to remove the dataframes backward to not mess up with the indices
        to_be_del.sort(reverse=True)

        for index in to_be_del:
            del target_list[index]
            # Save the deleted dataframe as dictionary with file_header as keys and list of respective indices as values
            deleted_df[k].append(index)

    return deleted_df


def delete_subset(dictionary):
    '''
    This function removes any subset dataframe within the same key,
    outputs a dictionary indicating deleted tickers and respective indices.

    dictionary: dictionary with file_header as keys and list of dataframes as values
    '''

    # Initialize a dictionary with list as values to house the indices
    deleted_df = defaultdict(list)

    for k in dictionary.keys():
        target_list = dictionary[k]
        # Use permutations here because sequence is important to compare subsets
        combo_list = list(permutations(range(len(target_list)),2))
        subset_index = []

        for df1_index, df2_index in combo_list:
            # Assert that we are comparing two dataframes
            if isinstance(target_list[df1_index], pd.DataFrame) & isinstance(target_list[df2_index], pd.DataFrame):
                # Flatten a dataframe to get unique cell values
                df1_set = {x for x in (target_list[df1_index].to_numpy().flatten()) if pd.notna(x) & (x != '')}
                df2_set = {x for x in (target_list[df2_index].to_numpy().flatten()) if pd.notna(x) & (x != '')}
                # Flatten a dataframe to get cell values
                df1_lst = [x for x in (target_list[df1_index].to_numpy().flatten()) if pd.notna(x) & (x != '')]
                df2_lst = [x for x in (target_list[df2_index].to_numpy().flatten()) if pd.notna(x) & (x != '')]
                if df1_set.issubset(df2_set):
                    df1_count = Counter(df1_lst)
                    df2_count = Counter(df2_lst)
                    boolean = []
                    # Further safety measures
                    for keys in df1_count.keys():
                        if df1_count[keys] <= df2_count[keys]:
                            boolean.append(True)
                        else:
                            boolean.append(False)
                        if np.array(boolean).all():
                            subset_index.append(df1_index)

        to_be_del = list(set(subset_index))
        to_be_del.sort(reverse=True)

        for index in to_be_del:
            del target_list[index]
            deleted_df[k].append(index)

    return deleted_df


def delete_empty(dictionary):
    '''
    This function removes any empty dataframe within a list.

    dictionary: dictionary with file_header as keys and list of dataframes as values
    '''

    for k in dictionary.keys():
        target_list = dictionary[k]
        empty_index = []

        for index in range(len(target_list)):
            if isinstance(target_list[index], pd.DataFrame):
                if target_list[index].empty:
                    empty_index.append(index)
        to_be_del = empty_index
        to_be_del.sort(reverse=True)

        for index in to_be_del:
            del target_list[index]


def delete_uninformative(dictionary):
    '''
    This function removes any dataframes where all of the following conditions are satisfied:
    1) no column name starts with 'vol\d+',
    2) no column name starts with 'Price\d+',
    where '\d+' is regex and represents any sequence of numbers.

    Outputs a dictionary indicating deleted tickers and respective indices.

    dictionary: dictionary with file_header as keys and list of dataframes as values
    '''

    # Initialize a dictionary with list as values to house the indices
    deleted_df = defaultdict(list)

    for k in dictionary.keys():
        target_list = dictionary[k]
        uninformative_index = []

        for index in range(len(target_list)):
            df = target_list[index]
            if isinstance(df, pd.DataFrame):
                df_col = df.columns.astype(str)
                # Condition 1 and 2 respectively
                if (np.array([re.findall('^vol\d+', df_col[x]) == [] for x in range(len(df_col))]).all() & np.array([re.findall('^Price\d+', df_col[x]) == [] for x in range(len(df_col))]).all()):
                    uninformative_index.append(index)

        to_be_del = uninformative_index
        to_be_del.sort(reverse=True)

        for index in to_be_del:
            del target_list[index]
            deleted_df[k].append(index)

    return deleted_df


def get_vertically_stacked_subtables(dictionary):
    '''
    We want to find dataframes that satisfy one of the following conditions, to be flagged as subtables:
        Condition A: has one or more rows that contain only strings
        Condition B: volume columns contain any empty strings
    This function outputs a dictionary indicating tickers and respective indices for subtables.

    dictionary: dictionary with file_header as keys and list of dataframes as values
    '''

    # Initialize an empty defaultdict to store subtables
    flagged_dict = defaultdict(set)

    for k in dictionary.keys():
        target_list = dictionary[k]
        for index in range(len(target_list)):
            # Make sure it's a dataframe and it's not empty
            if isinstance(target_list[index], pd.DataFrame) & (not target_list[index].empty):
                df = target_list[index]
                Cond_A = False

            ### Condition A
                # Check if the dataframe has rows that have no numbers to be found (i.e. all strings)
                for x in range(df.shape[0]):
                    boolean_row = []
                    for y in range(df.shape[1]):
                        # Skip cells that are empty
                        if df.iloc[x, y] != '':
                            # Removes parentheses and everything within it, for each cell
                            df.iloc[x, y] = re.sub(r'\([^()]*\)', '', df.iloc[x, y])
                            # Iterate every cells within a row to see if number exists
                            if re.findall('[\d]', df.iloc[x, y]) == []:
                                boolean_row.append(True)
                            else: boolean_row.append(False)
                    # If there exists a row where no numbers are to be found, flag that dataframe as subtable
                    if np.array(boolean_row).all():
                        # Output the potentially subtable to flagged_dict
                        flagged_dict[k].add(index)
                        Cond_A = True

            ### Condition B
                # Check to see if Condition A is satisfied
                if Cond_A:
                    # Check to see if there are any volume containing columns
                    if (df.columns.astype(str).str.contains('Volume', case=False)).any():
                        # If so, see if there are any missing strings within those columns
                        if (df[df.columns[df.columns.astype(str).str.contains('Volume')]] == '').any().any():
                            flagged_dict[k].add(index)

    # return the subtable dictionary for bookkeeping and input for vertically_stacked_subtable_cleaner()
    return flagged_dict


def vertically_stacked_subtable_cleaner(dictionary, flagged_dict, exclusion=False, excluded_ticker=None):
    '''
    This function does two things:
    1) Appends the splitted subtables to the list where the subtables are contained in
    2) Deletes the original subtables

    dictionary: dictionary with file_header as keys and list of dataframes as values
    flagged_dict: output from get_vertically_stacked_subtables()
    exclusion: whether to exclude certain company within the flagged_dict or not
    excluded_ticker: the tickers to be excluded, as a typle of strings
    '''

    if exclusion == True:
        flagged_dict = {k: v for k, v in flagged_dict.items() if not k.startswith(excluded_ticker)}

### Append subtables to original list under respective tickers
    for ticker, indices in flagged_dict.items():
        for index in indices:
            df_original = dictionary[ticker][index]
            # Assuming we don't have to split more than fifteen times per dataframe
            for _ in range(15):
                for x_idx in range(df_original.shape[0]):
                    # Create a temp version of df that removes parentheses and everything within it, for each cell
                    df_temp = df_original.applymap(lambda x: re.sub(r'\([^()]*\)', "", x))
                    # Find the first row index to split on, based of df_temp
                    # The row must satisfy two conditions (1. no numbers within any cells, 2. the entine row not consist of only empty strings)
                    if (not any(df_temp.iloc[x_idx].str.contains('\d'))) & (not all(df_temp.iloc[x_idx] == '')):
                        break
                # If it's not a subtable anymore, break
                if (x_idx + 1) == df_original.shape[0]:
                    break
                # We don't wanna split on the last row
                if x_idx == range(df_original.shape[0])[-1]:
                    break
                df_split1 = df_original.iloc[:x_idx]
                df_split2 = df_original.iloc[x_idx:]
                # Keep the original columns as series for further usage
                columns_temp = df_split2.columns
                # Clear the columns to use set_col_header function
                df_split2.columns = ["" for x in columns_temp]
                if not any(df_split2.iloc[0, 1:].str.contains('\d')):
                   df_split2.columns=df_split2.iloc[0]
                   df_split2=df_split2.iloc[1:]
                df_split2 = set_col_headers(df_split2)
                # Use original columns if empty OR if the new would_have_been column names are contained in columns_temp, or else, stick with the new ones
                df_split2.columns = [columns_temp[k] if (re.search('[\w]',v) == None or v in columns_temp[k]) else v for k,v in enumerate(df_split2.columns)]
                dictionary[ticker].append(df_split1)
                df_original = df_split2
            # Grab the final piece of the dataframe. If splitting not occured, we will append the original and have the duplicated be handled by drop_duplicates()
            dictionary[ticker].append(df_original)

### Delete the original subtables
    for k in flagged_dict.keys():
        target_list = dictionary[k]
        to_be_del = list(flagged_dict[k])
        to_be_del.sort(reverse=True)
        for index in to_be_del:
            del target_list[index]























# Do not use this for now
def extract_subset_tables(dictionary):
    """
    This function extracts subset dataframes (both column-wise and row_wise),
    and append those into a list.

    dictionary: dictionary with file_header as keys and list of dataframes as values
    """
    company_list = []
    subset_df_list = [] # To store the subset dataframes
    for keys in dictionary.keys():
        company_list.append(keys)
    trim_company_set = {re.sub('[0-9]', '', company) for company in company_list} # Extract unique company tickers
    for company in trim_company_set:
        subdict = {k: v for k, v in dictionary.items() if k.startswith(company)} # Dictionary comprehension for each ticker
        tables_within_a_company = list(chain.from_iterable(subdict.values())) # Flatten list of list
        combo_list = list(permutations(range(len(tables_within_a_company)),2)) # Use permutations here because sequence is important to compare subsets
        subset_index = []
        for df1_index, df2_index in combo_list:
            if isinstance(tables_within_a_company[df1_index], pd.DataFrame) & isinstance(tables_within_a_company[df1_index], pd.DataFrame):
                df1_set = {x for x in (tables_within_a_company[df1_index].to_numpy().flatten()) if pd.notna(x) & (x != '')}
                df2_set = {x for x in (tables_within_a_company[df2_index].to_numpy().flatten()) if pd.notna(x) & (x != '')} # Flatten a dataframe to get unique cell values
                df1_lst = [x for x in (tables_within_a_company[df1_index].to_numpy().flatten()) if pd.notna(x) & (x != '')]
                df2_lst = [x for x in (tables_within_a_company[df2_index].to_numpy().flatten()) if pd.notna(x) & (x != '')] # Flatten a dataframe to get cell values
                if df1_set.issubset(df2_set):
                    df1_count = Counter(df1_lst)
                    df2_count = Counter(df2_lst)
                    boolean = []
                    for keys in df1_count.keys(): # Further safety measures
                        if df1_count[keys] <= df2_count[keys]:
                            boolean.append(True)
                        else:
                            boolean.append(False)
                        if np.array(boolean).all():
                            subset_index.append(df1_index)
        to_be_del = list(set(subset_index))
        for index in to_be_del:
            subset_df_list.append(tables_within_a_company[index])
    return subset_df_list


# Do not use this for now
def remove_subset_tables(dictionary, list_subset_tables):
    '''
    This function removes dataframes that are subset of other dataframes within the same ticker,
    outputs a dictionary indicating deleted tickers and respective indices.

    dictionary: dictionary with file_header as keys and list of dataframes as values
    list_subset_tables: list of subset dataframes that are obtained from extract_subset_tables()
    '''
    deleted_df = defaultdict(list) # Initialize a dictionary with list as values to house the indices
    for k in dictionary.keys():
        target_list = dictionary[k]
        duplicated_index = []
        for index in range(len(target_list)):
            for subset_df in list_subset_tables:
                if isinstance(target_list[index], pd.DataFrame):
                    if target_list[index].equals(subset_df): # Compare if the dataframes matches the subset dataframes
                        duplicated_index.append(index)
        to_be_del = list(set(duplicated_index)) # Remove duplicated indices in case there are >2 identical dataframes
        to_be_del.sort(reverse=True) # We want to remove the dataframes backward to not mess up with the indices
        for index in to_be_del:
            del target_list[index]
            deleted_df[k].append(index) # Save the deleted dataframes as dictionary with file_header as keys and list of respective indices as values
    return deleted_df
