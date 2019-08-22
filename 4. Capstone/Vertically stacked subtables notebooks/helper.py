from IPython.display import display, HTML
import pandas as pd
import numpy as np
import re


def verticalSplit_specialSauce(table_list, verbose = False):
    '''
    Takes a list of tables and attempts to split them.
    Returns a new table_list that has the original tables split, if needed.
    '''
    split_dfs = []
    transformation_dict = {}
    verbose=True
    i = 0
    #print(len(table_list))
    for table in table_list:
        table=df
        table = table.rename(str.lower, axis = "columns")
        table_columns = table.columns
        # we want the columns that have either the word 'oil' or 'bbl' in the column, but we want to omit columns that
        # have information purely about gas, ie, either 'gas' or 'btu'. We then add these two queries together, and remove
        # the columns that are duplicates between oil_cols and oil_cols2.
        # We then create a pure_oil_cols, which we make sure does not have ANY gas information in it.
        pure_oil_cols = [i for i,col in enumerate(table_columns) if (("oil" in col or 'bbl' in col or 'barrel' in col) and (("gas" not in col) and ('btu' not in col)))]
        # this is the same process as above, but flipped.
        pure_gas_cols = [i for i,col in enumerate(table_columns) if (("gas" in col or 'btu' in col) and (("oil" not in col) and ('bbl' not in col)))]
        print(pure_oil_cols,pure_gas_cols)
        # if we have BOTH columns that ONLY HAVE oil information and gas information, we continue.
        if ((pure_oil_cols != []) & (pure_gas_cols != [])):
            if max(pure_oil_cols)<min(pure_gas_cols): #first oil then gas
                oil_df=df.drop(df.columns[min(pure_gas_cols):],axis=1)
                gas_df=df.drop(df.columns[min(pure_oil_cols):min(pure_gas_cols)],axis=1)
            if max(pure_gas_cols)<min(pure_oil_cols): #first gas then oil
                gas_df=df.drop(df.columns[min(pure_oil_cols):],axis=1)
                oil_df=df.drop(df.columns[min(pure_gas_cols):min(pure_oil_cols)],axis=1)
            # add the newly split dfs to a new list, that we will eventually return.
            split_dfs.append(oil_df)
            split_dfs.append(gas_df)
            # keep track of updates
            transformation_dict.update({"table" : "attempted to split horizontally"})
            i += 1
        else:
            split_dfs.append(table)
    return split_dfs



def verticalSplit(table_list, verbose = False):
    '''
    Takes a list of tables and attempts to split them.
    Returns a new table_list that has the original tables split, if needed.
    '''
    split_dfs = []
    transformation_dict = {}

    i = 0
    #print(len(table_list))
    for table in table_list:
        table = table.rename(str.lower, axis = "columns")
        table_columns = table.columns
        # we want the columns that have either the word 'oil' or 'bbl' in the column, but we want to omit columns that
        # have information purely about gas, ie, either 'gas' or 'btu'. We then add these two queries together, and remove
        # the columns that are duplicates between oil_cols and oil_cols2.
        # We then create a pure_oil_cols, which we make sure does not have ANY gas information in it.
        oil_cols = [col for col in table_columns if (("oil" in col) or (("gas" not in col) and ('btu' not in col)))]
        oil_cols2 = [col for col in table_columns if (('bbl' in col) or ('btu' not in col))]
        oil_cols += [col for col in oil_cols2 if (col not in oil_cols)]
        pure_oil_cols = [col for col in oil_cols if ((("oil" in col) or ('bbl' in col)) and ('btu' not in col))]

        # this is the same process as above, but flipped.
        gas_cols = [col for col in table_columns if (("gas" in col) or (("oil" not in col) and ('bbl' not in col)))]
        gas_cols2 = [col for col in table_columns if (('btu' in col) or (('bbl' not in col) and ('oil' not in col)))]
        gas_cols += [col for col in gas_cols2 if (col not in gas_cols)]
        pure_gas_cols = [col for col in gas_cols if ((('gas' in col) or ('btu' in col)) and ('bbl' not in col))]

        # output to console
        if verbose:
            print("oil_cols: ", oil_cols)
            print("pure_oil_cols: ", pure_oil_cols)
            print("gas_cols: ", gas_cols)
            print("pure_gas_cols: ", pure_gas_cols)

        # if we have BOTH columns that ONLY HAVE oil information and gas information, we continue.
        if ((pure_oil_cols != []) & (pure_gas_cols != [])):
            # this is the noisy version of the below function
            if verbose:
                print("Oil Split")
                print("*"*50)
                oil_df = table.loc[:, oil_cols]
                oil_df.replace('', np.nan, inplace = True)
                display(HTML(oil_df.to_html()))
                oil_df = oil_df.dropna(how = 'all', subset = pure_oil_cols)
                display(HTML(oil_df.to_html()))

                print("Gas Split")
                print("*"*50)
                gas_df = table.loc[:, gas_cols]
                gas_df.replace('', np.nan, inplace = True)
                display(HTML(gas_df.to_html()))
                gas_df = gas_df.dropna(how = 'all', subset = pure_gas_cols)
                display(HTML(gas_df.to_html()))
            else:
                # grab columns that we've sorted using oil_cols
                oil_df = table.loc[:, oil_cols]
                # replace all empty strings with np.nan so we can drop NA
                oil_df.replace('', np.nan, inplace = True)
                # if the PURELY OIL columns have NAs for all values, that means that they can be dropped without loss of information.
                oil_df = oil_df.dropna(how = 'all', subset = pure_oil_cols)

                # this is the same process as the oil.
                gas_df = table.loc[:, gas_cols]
                gas_df.replace('', np.nan, inplace = True)
                gas_df = gas_df.dropna(how = 'all', subset = pure_gas_cols)

            # return the NANs to empty strings so you don't break anyone elses' function.
            oil_df.replace(np.nan, '', inplace = True)
            gas_df.replace(np.nan, '', inplace = True)
            # add the newly split dfs to a new list, that we will eventually return.
            split_dfs.append(oil_df)
            split_dfs.append(gas_df)

            # keep track of updates
            transformation_dict.update({"table" : "attempted to split horizontally"})
            i += 1
        else:
            # if we did not have both pure_gas_cols and pure_oil_cols, we don't need to split,
            # so we just add the table and move on.
            if verbose:
                print("Nothing to split here! Moving on...")
                print("^"*50)
            split_dfs.append(table)

    # return new table_list with the newly split dataFrames.
    return split_dfs

def trimDictonary(dictonary, value):
    new_dict = {}
    listOfItems = dictonary.items()
    for item in listOfItems:
        if item[1] != value:
            new_dict.update({item[0] : item[1]})
    return new_dict

def filterBusiness(dictonary, businessTicker):
    new_dict = {}
    listOfItems = dictonary.items()
    for item in listOfItems:
        if businessTicker in item[0]:
            new_dict.update({item[0] : item[1]})
    return new_dict


def keyWords(string, keyword_list):

    for keyword in keyword_list:
        if (keyword in string):
            return True

    return True


def stopWords(string, stopword_list):

    for stopword in stopword_list:
        if stopword in string:
            return False

    return True

def columnSorter(dataFrame):

    colNames = list(dataFrame.columns)

    unprocessed_columns = []
    for colName in colNames:
        if re.search("vol\d+", colName):
            continue
        elif re.search("price\d+", colName):
            continue
        elif re.search("dattime\d+", colName):
            continue
        else:
            unprocessed_columns.append(colName)

    return unprocessed_columns

def volumeCleaner(dataFrame, key_words = "", stop_words = ""):
    dataFrame = dataFrame.rename(str.lower, axis = 'columns')
    #dataFrame = dataFrame.loc[:,~dataFrame.columns.duplicated()]
    if key_words == "":
        key_words = ['mmbbls', 'mbbls', 'bbl',
                  'mmcfe', 'mmcf', 'mcfe',
                  'mcf', 'bcfe', 'bcf',
                  'mmbtu', 'mbtu', 'btu',
                  'gj', 'dekatherm', 'volume',
                  'barrel', 'barrle', 'production',
                  'total outstanding notional']
    if stop_words == "":
        stop_words = ['sold price', 'purchased price', 'sales']

    units_column_helper = ['bbl', 'btu', 'gj']

    i = 0

    transformation_dict = {}

    for colName in dataFrame.columns:
        volume_bool = False
        if colName not in dataFrame.columns:
            continue

        for key in key_words:
            if (key in colName) & (stopWords(colName, stop_words)):
                volume_bool = True
                break

        if volume_bool:

            i += 1
            new_name = 'Vol' + str(i)
            idx = dataFrame.columns.get_loc(colName)
            transformation_dict.update({colName : new_name})
            dataFrame = dataFrame.rename(columns = {colName : new_name})

            length = dataFrame.shape[0]
            new_vals = np.array([colName]*length)
            dataFrame.insert(idx+1, new_name+"_type", new_vals)

            if (idx+2) < len(dataFrame.columns):
                for unit in units_column_helper:
                    if np.any(dataFrame.iloc[:,idx+2].astype(str).str.contains(unit, regex = False, case = False)):
                        dataFrame[new_name+"_type"] += ", " + dataFrame.iloc[:,idx+2]
                        dataFrame = dataFrame.drop(columns = dataFrame.columns[idx+2])
                        break
    # print("volumeCleaner finished, processed " + str(i) + " columns.")
    # print("-"*50)
    #
    # for item in transformation_dict.items():
    #     print("Old column: ", item[0], "  ----->  ", "New column: ", item[1])
    #
    # print("-"*50)

    return dataFrame


def dateCleaner(dataFrame, key_words = "", stop_words = ""):
    dataFrame = dataFrame.rename(str.lower, axis = 'columns')
    #dataFrame = dataFrame.loc[:,~dataFrame.columns.duplicated()]
    if key_words == "":
        key_words = ['date', 'period', 'term',
                     'commodity/operating area/index', 'month',
                     'maturity', 'expiration', 'duration']
    if stop_words == "":
        stop_words = ['price', 'location', 'volume',
                      'type of', ',,period,']


    i = 0

    transformation_dict = {}

    for colName in dataFrame.columns:
        key_bool = False

        if colName == "":
            continue

        for unit in key_words:
            if (unit in colName) & (stopWords(colName, stop_words)):
                key_bool = True
                break

        if key_bool:

            i += 1
            new_name = 'Dattime' + str(i)
            idx = dataFrame.columns.get_loc(colName)
            # print('The index of the changed date column is : ', idx, '\n',
            #       'The column name is: ', colName)
            transformation_dict.update({colName : new_name})
            dataFrame = dataFrame.rename(columns = {colName : new_name})

            length = dataFrame.shape[0]
            new_vals = np.array([colName]*length)
            dataFrame.insert(idx+1, new_name+"_type", new_vals)

    # print("dateCleaner finished, processed " + str(i) + " columns.")
    # print("-"*50)
    #
    # for item in transformation_dict.items():
    #     print("Old column: ", item[0], "  ----->  ", "New column: ", item[1])
    #
    # print("-"*50)

    return dataFrame



def priceCleaner(dataFrame, key_words = "", stop_words = ""):

    dataFrame = dataFrame.rename(str.lower, axis = 'columns')

    if key_words == "":
        key_words = ['fair value', 'weighted', 'price',
                     'ceiling', 'floor', 'sale', 'sold',
                     '$']

    if stop_words == "":
        stop_words = ['(in thousands,', 'united states', 'priceindex',
                      'price index', 'volume', 'mmbtu/d', 'mbbls/d',
                      'except price']

    i = 0

    transformation_dict = {}

    for colName in dataFrame.columns:
        key_bool = False

        for unit in key_words:
            if (unit in colName) & (stopWords(colName, stop_words)):
                key_bool = True

        if key_bool:

            i += 1
            new_name = 'Price' + str(i)

            idx = dataFrame.columns.get_loc(colName)

            transformation_dict.update({colName : new_name})

            dataFrame = dataFrame.rename(columns = {colName : new_name})


            length = dataFrame.shape[0]
            new_vals = np.array([colName]*length)
            dataFrame.insert(idx+1, new_name+"_type", new_vals)

    # print("priceCleaner finished, processed " + str(i) + " columns.")
    # print("-"*50)
    # for item in transformation_dict.items():
    #     print("Old column: ", item[0], "  ----->  ", "New column: ", item[1])
    #
    # print("-"*50)
    return dataFrame


def tableProcessor(table_list):
    new_list = []
    i = 0
    for table in table_list:
        i += 1
        print("@"*50, "\n", "Processing table: " + str(i))
        display(HTML(table.to_html()))
        table = dateCleaner(table)
        table = volumeCleaner(table)
        table = priceCleaner(table)
        table.columns = table.columns.str.lower()
        unprocessed = columnSorter(table)
        print("Unprocessed columns: ", "\n", unprocessed)
        display(HTML(table.to_html()))
        new_list.append(table)

    return new_list

def productCleaner(dataFrame, key_words = "", stop_words = "", verbose = False):
    dataFrame = dataFrame.rename(str.lower, axis = "columns")
    #dataFrame = dataFrame.loc[:, ~dataFrame.columns.duplicated()]

    if key_words == "":
        key_words = ['oil', 'natural gas liquid', 'liquid natural gas', 'natural gas', 'gas',
                     'barrel', 'btu', 'bbl']
        units_helper = {'barrel' : 'oil', 'btu' : 'gas', 'bbl' : 'oil'}
    if stop_words == "":
        stop_words = []

    i = 0

    transformation_dict = {}
    product_types = []

    # checks to see if product_type already exists as a column name
    if "product_type" not in dataFrame.columns:
        # if not, it creates a column with '@' as a temporary placeholder for easy searching.
        idx = len(list(dataFrame.columns))

        # inserts column at the end of the dataFrame.
        length = dataFrame.shape[0]
        new_vals = np.array(["@"]*length)
        dataFrame.insert(idx, "product_type", new_vals)

        # look through the keywords
        for key in key_words:
            if key in units_helper.keys():
                prod_type = units_helper[key]
            else:
                prod_type = key
            # filter data rows that contain any product information ('oil', 'gas', etc)
            data_rows = dataFrame[dataFrame.apply(lambda row: row.astype(str).str.contains(key, case = False).any(), axis = 1)]

            # if we have any results, continue
            if data_rows.shape[0] != 0:
                # same logic as instrumentCleaner, looks to see if it needs updating or replacing of the value.
                updated_values = dataFrame.product_type.loc[data_rows.index, ].apply(lambda existing_val: prod_type if ((existing_val in prod_type) or (existing_val == "@")) else (existing_val + " " + prod_type))
                dataFrame.loc[data_rows.index, "product_type"] = updated_values
                transformation_dict.update({", ".join(data_rows.index.values.astype(str)) : key + " " + prod_type})

        # if there is still a "@" (aka placeholder) in the product_type, continue
        if "@" in dataFrame.product_type.values:

            # filter rows that still need information
            rows_to_update = dataFrame[dataFrame.product_type == "@"]
            # placeholder value so we can do string comparison later
            # without it throwing an error
            product_types = [""]

            for colName in dataFrame.columns:

                for key in key_words:
                    if (key in colName):
                        if key in units_helper.keys():
                            key = units_helper[key]
                        # so we want to make sure we don't add the same thing
                        # (ie, 'gas', 'natural gas') so we do a little boolean logic
                        # to check if the key we are about to add is already a substring.
                        for product in product_types:
                            # this is our base case and will evaluate the first time a keyword shows up.
                            if product == "":
                                # we want to remove it, since it's just a placeholder, and replace it with
                                # the key that is in the colName.
                                product_types.remove(product)
                                product_types.append(key)
                            # if the key is not in product_types, it's ok to add it
                            if key not in product:
                                product_types.append(key)
                            # otherwise we want to remove the key that's a substring, and replace it with
                            # the new key.
                            else:
                                product_types.remove(product)
                                product_types.append(key)

            # if any keys are added in the last step, continue
            if len(product_types) != 0:

                # remove duplicates
                product_types = list(dict.fromkeys(product_types))
                transformation_dict.update({'added these products' : product_types})

                # get index for rows that need to still be updated
                update_idx = dataFrame.product_type[dataFrame.product_type == "@"].index

                # update them with the list collapsed to a single string
                dataFrame.loc[update_idx, "product_type"] = ", ".join(product_types)


        if verbose:
            print("productCleaner finished, processed " + str(i) + " columns.")
            print("-"*50)

            for item in transformation_dict.items():
                print("Old column: ", item[0], "  ----->  ", "The product_type entry is: ", item[1])

            print("-"*50)
    else:
        if verbose:
            print("Column product_type already exists")

    return dataFrame


def instrumentCleaner(dataFrame, key_words = "", stop_words = "", verbose = False):
    '''
    instrumentCleaner attempts to isolate option, whether it be swaps, collars, put,
    etc. It first looks through the column names, and then through the table itself
    in order to impute what kind of option it is.
    '''
    dataFrame = dataFrame.rename(str.lower, axis = "columns")
    dataFrame = dataFrame.loc[:, ~dataFrame.columns.duplicated()]
    if key_words == "":
        key_words = ['swap', 'collar', 'put', 'call', 'swap option', 'fixed-price swap',
                     'put option', 'fixed price swap']
        units_helper = {}
    if stop_words == "":
        stop_words = []

    i = 0
    transformation_dict = {}
    # need to store instrument_types in case there are multiple in the table.
    instrument_types = []

    # make sure you don't already have this column in your DF.
    if "instrument_type" not in list(dataFrame.columns):
        idx = len(list(dataFrame.columns))
        length = dataFrame.shape[0]
        new_vals = np.array(["@"]*length)
        dataFrame.insert(idx, "instrument_type", new_vals)

        for key in key_words:
            # isolate the rows in the dataFrame that contain the key.
            data_rows = dataFrame[dataFrame.apply(lambda row: row.astype(str).str.contains(key, case = False).any(), axis = 1)]

            # if any rows are found, continue
            if data_rows.shape[0] != 0:
                # FIRST: get a subset of the dataFrame column we made above 'instrument_type' using the data_rows that are flagged (also above)
                # SECOND: use .apply to decide whether to UPDATE, or REPLACE the existing_val in the dataFrame.
                # THIRD: update the dataFrame with the new values.
                # FOURTH: update the transformation dictionary to store changes.
                updated_values = dataFrame.instrument_type.loc[data_rows.index, ].apply(lambda existing_val: key if (existing_val in key) else (existing_val + ", " + key))
                dataFrame.instrument_type.loc[data_rows.index, ] = updated_values
                transformation_dict.update({", ".join(data_rows.index.values.astype(str)) : key})

    if "@" in dataFrame.instrument_type.values:
        rows_to_update = dataFrame[dataFrame.instrument_type == "@"]
        instrument_types = []
        for colName in dataFrame.columns:

            for key in key_words:
                if ((key in colName) & stopWords(colName, stop_words)):
                    if key in units_helper.keys():
                        key = units_helper[key]
                    instrument_types.append(key)

        # if any of the keywords showed up. continue through to here
        if len(instrument_types) != 0:

            # this step removes duplicate instrument types
            # and updates the transformation dictionary
            instrument_types = list(dict.fromkeys(instrument_types))
            transformation_dict.update({'added these products' : instrument_types})

            # get index for rows that need to still be updated
            update_idx = dataFrame.instrument_type[dataFrame.instrument_type == "@"].index

            # update them with the list collapsed to a single string
            dataFrame.loc[update_idx, "instrument_type"] = ", ".join(instrument_types)
    else:
        print("instrument_type column already exists")

    if verbose:
        print("instrumentCleaner finished, processed " + str(i) + " columns.")
        print("-"*50)

        for item in transformation_dict.items():
            print("Old column: ", item[0], "  ----->  ", "The instrument_type entry is: ", item[1])

        print("-"*50)

    return dataFrame


def theConcatenator(dictonary, output="df"):

    big_df = pd.DataFrame()
    listOfItems = dictonary.items()
    processed_dict = {}

    for item in listOfItems:
        print("*"*50)
        print(item[0])
        print("Processing ", len(item[1]), " tables")
        doc_id = item[0]
        processed_tables = tableProcessor(item[1])

        if output == "df":
            for table in processed_tables:
                length = table.shape[0]
                doc_id_col = np.array([doc_id]*length)
                table.insert(0, "doc_id", doc_id_col)
                table = table.drop_duplicates(  )
                big_df = pd.concat([big_df, table], axis = 0, join = 'outer', sort = False)

        elif output == 'pickle':
            for table in processed_tables:
                length = table.shape[0]
                doc_id_col = np.array([doc_id]*length)
                table.insert(0, "doc_id", doc_id_col)
                table = table.drop_duplicates()
            processed_dict.update({item[0] : processed_tables})

        else:
            print('not recognized output')
            return ""

    if output == 'df':
        big_df = big_df.drop_duplicates()
        return big_df
    elif output == 'pickle':
        return processed_dict
    else:
        return ""
