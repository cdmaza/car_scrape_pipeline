from extract.extract_carlist import get_data
from extract.extract_carsome import get_data

def get_all_data():
    carlist_data = get_data()
    carsome_data = get_data()
    return carlist_data, carsome_data




