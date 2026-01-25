WEB_SOURCES = [
    {
        "name": "carlist",
        "module": "src.extract.carlist_client",
        "class": "carlistWebClient",
        "url": "https://www.carlist.my/used-cars-for-sale/malaysia",
        "table": "raw_carlist"
    },
    {
        "name": "carsome",
        "module": "src.extract.carsome_client",
        "class": "CarsomeWebClient",
        "url": "https://www.carsome.com.my/cars-for-sale",
        "table": "raw_carsome"
    }
]