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

def get_clean(df):

    #remove duplicate
    df_cleaned = df.drop_duplicates()

    #Correcting invalid, inconsistent, or irrelevant data
    for col in df_cleaned.columns:
        if 'make' in col.lower():
            df_cleaned = df_cleaned.rename(columns={"Make": "brand"})

    for col in df_cleaned.columns:
        if 'car.type' in col.lower():
            df_cleaned["Car.Type"] = df_cleaned["Car.Type"].str.replace('Car', '')
            df_cleaned = df_cleaned.rename(columns={"Car.Type": "car_type"})

    for col in df_cleaned.columns:
        if 'transm' in col.lower():   
            df_cleaned = df_cleaned.rename(columns={"Transm": "gear_type"})

    for col in df_cleaned.columns:
        if 'engine.cap' in col.lower():
            df_cleaned["Engine.Cap"] = df_cleaned["Engine.Cap"].str.replace('-', '0')
            df_cleaned["Engine.Cap"] = df_cleaned["Engine.Cap"].str.replace('cc', '')
            df_cleaned["Engine.Cap"] = df_cleaned["Engine.Cap"].astype(int)
            df_cleaned = df_cleaned.rename(columns={"Engine.Cap": "engine_cap"})
    
    for col in df_cleaned.columns:
        if 'mileage' in col.lower():
            df_cleaned["Mileage"] = df_cleaned["Mileage"].fillna(0).astype(int)
            df_cleaned = df_cleaned.rename(columns={"Mileage": "mileage"})
    
    for col in df_cleaned.columns:
        if 'price' in col.lower():   
            df_cleaned["Price"] = df_cleaned["Price"].astype(float).round(2)
            df_cleaned = df_cleaned.rename(columns={"Price": "old_price"})

    df_cleaned.columns = df_cleaned.columns.str.lower()

    #handle missing values (mean, median, mode, drop, estimate)
    for index, row in df_cleaned[df_cleaned['engine_cap'] == 0].iterrows():
        model = row['model']
        year_produce = row['year']
        matching_row = df_cleaned[(df_cleaned['model'] == model) & (df_cleaned['year'] == year_produce) & (df_cleaned['engine_cap'] != 0)]
        if not matching_row.empty:
            df_cleaned.at[index, 'engine_cap'] = matching_row['engine_cap'].values[0]

    for col in df_cleaned.columns:
        if 'year' in col.lower():
            df_cleaned = df_cleaned.rename(columns={"year": "year_produce"})

    print(df_cleaned.dtypes)

    return df_cleaned

if __name__ == "__main__":
    print("ETL process started")
    file_path1 = "c:/Users/User/Desktop/Project/data project/car_scrape_pipeline/dags/etl/data/list_car.csv"
    columns_keyword = ["year_produce", "brand", "model","car_type", "gear_type", "engine_cap","mileage", "price"]

    data = get_data(file_path1)
    df = get_clean(data)
    # df = df[["year_produce", "brand", "model","car_type", "gear_type", "engine_cap","mileage", "old_price"]]
    # df.to_csv('old_car_detail1.csv')

