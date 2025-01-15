import pandas as pd
import os
import re 
# def get_transform()
    

# def get_aggregate()

# -------------------

# def check_date_column(df, column_name):
#     try:
#         # Attempt to convert the column to datetime
#         df[column_name] = pd.to_datetime(df[column_name])
#         return True
#     except Exception as e:
#         return False
    
# def get_validate_after():

#     df = get_data()
    
#     # Column checks

#     # Constraints: Validate primary keys, foreign keys, and uniqueness.

#     return done_validate
    
# -------------------

# def get_load()

def get_data(file_path):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist.")
        
        if file_path.endswith('.csv'):
            data = pd.read_csv(file_path, encoding='ISO-8859-1')
        elif file_path.endswith('.json'):
            data = pd.read_json(file_path)
        else:
            raise ValueError("Unsupported file format. Please provide a CSV or JSON file.")
        
        return data

    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")

# -------------------
    
def get_validate_before(df, columns_neededs):
    
    # Column checks
    matching_columns = [col for col in df.columns if any(columns_needed in col.lower() for columns_needed in columns_neededs)]
    non_empty_matching_columns = [col for col in matching_columns if not df[col].isnull().all()]
    print(matching_columns)
    if non_empty_matching_columns:
        return True, df[matching_columns]

    return False, None 

# --------------------

def get_year(datas):
    for index, row in datas.iterrows():
        first_string = row["Description"].split(' ')[0]
        datas.at[index, 'Description'] = row["Description"].replace(first_string + ' ', '', 1)
        datas.at[index, 'year_produce'] = first_string

    return datas

def get_brand(datas):
    for index, row in datas.iterrows():
        if re.search("N/A", row["Model"], flags=re.IGNORECASE):
            continue
        first_string = row["Model"].split(' ')[0]
        datas.at[index, 'Model'] = row["Model"].replace(first_string + ' ', '', 1)
        datas.at[index, 'brand'] = first_string
    
    return datas

def get_model(datas):

    temp = datas["description"].split(' ')[0]
    datas["description"] = datas["description"].replace(temp + ' ', '', 1)

    return temp

def get_cap(datas):

    cap_match = re.search(r'\b\d+\.\d\b', datas["description"])
    if cap_match:
        temp = cap_match.group(0)
        datas["description"] = datas["description"].replace(temp, '', 1).strip()
    else:
        temp = "0.0"

    return temp
        
def get_mileage(datas):

    mileage_match = re.search(r'\d+K', datas["mileage"])
    if mileage_match:
        datas["mileage"] = mileage_match.group(0).replace("K", "000")
    else:
        datas["mileage"] = "0"

    # datas["description"] = re.sub(r'\d+K KM', '', datas["description"], 1).strip()

    return datas

def get_price(datas):

    datas["list_price"] = datas["list_price"].replace("N/A", "0")
    
    for index, row in datas[datas['list_price'] == "0"].iterrows():
        datas.at[index, 'list_price'] = row['monthly_installment']

    datas = datas.str.replace(r'[^0-9]', '', regex=True)
    
    return datas["list_price"]

def get_clean(df):

    #Correcting invalid, inconsistent, or irrelevant data
    df_cleaned = df.copy()

    for col in df_cleaned.columns:
        if 'description' in col.lower():
            df_cleaned = get_year(df_cleaned)
    
    print(df_cleaned.dtypes)

    for col in df_cleaned.columns:
        if 'model' in col.lower():
            df_cleaned = get_brand(df_cleaned)

    print("done2")
    df_cleaned["car_type"] = "used"

    if re.search(r'desc', df_cleaned.columns, flags=re.IGNORECASE):
        df_cleaned["engine_cap"] = df_cleaned.apply(get_cap)

    if re.search(r'mileage', df_cleaned.columns, flags=re.IGNORECASE):
        df_cleaned["mileage"] = df_cleaned.apply(get_mileage)
    
    if re.search(r'list_price', df_cleaned.columns, flags=re.IGNORECASE):
        df_cleaned["old_price"] = get_price(df_cleaned)

    #Resolving formatting issues

    #handle missing values (mean, median, mode, drop, estimate)
    if re.search(r'desc', df_cleaned.columns, flags=re.IGNORECASE):
        df_cleaned["model"] = df_cleaned.apply(get_model, axis=1)

    #remove duplicate
    df_cleaned = df.drop_duplicates()

    print(df_cleaned.dtypes)
    return df_cleaned


if __name__ == "__main__":
    print("ETL process started")
    file_path2 = "c:/Users/User/Desktop/Project/data project/car_scrape_pipeline/dags/etl/data/resale_car.csv"
    columns_keyword = ["year_produce", "brand", "model","car_type", "gear_type", "engine_cap","mileage", "old_price"]

    df = get_data(file_path2)
    
    df = get_clean(df)
    # df = df[["year_produce", "brand", "model","car_type", "gear_type", "engine_cap","mileage", "old_price"]]
    # df.to_csv('old_car_detail1.csv')