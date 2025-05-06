
/*create bronze table for raw data*/
CREATE TABLE bronze.car_details (
    title VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(50)
    model VARCHAR(50),
    model_group VARCHAR(50),
    variant VARCHAR(50),
    body_type VARCHAR(50),
    transmission CHAR(50),
    mileage INT(7),
    type CHAR(50),
    capacity string
    price INT,
    manufactured string
    data_posted DATE,
    date_extracted DATE,
    detail_link NVARCHAR(max)
);

/*create silver table for transformed data*/
CREATE TABLE silver.car_details (
    title VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(50)
    model VARCHAR(50),
    model_group VARCHAR(50),
    variant VARCHAR(50),
    body_type VARCHAR(50),
    transmission CHAR(50),
    mileage INT(7),
    type CHAR(50),
    capacity string
    price INT,
    manufactured string
    data_posted DATE,
    date_extracted DATE,
    detail_link NVARCHAR(max)
);

/*create gold table for insight data*/
CREATE TABLE gold.aftermarket_price (
    title VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(50)
    model VARCHAR(50),
    model_group VARCHAR(50),
    variant VARCHAR(50),
    body_type VARCHAR(50),
    transmission CHAR(50),
    mileage INT(7),
    type CHAR(50),
    capacity string
    price INT,
    manufactured string
    data_posted DATE,
    date_extracted DATE,
    detail_link NVARCHAR(max)
);


CREATE TABLE gold.price_trends (
    title VARCHAR(50) PRIMARY KEY,
    brand VARCHAR(50)
    model VARCHAR(50),
    model_group VARCHAR(50),`
    variant VARCHAR(50),
    body_type VARCHAR(50),
    transmission CHAR(50),
    mileage INT(7),
    type CHAR(50),
    capacity string
    price INT,
    manufactured string
    data_posted DATE,
    date_extracted DATE,
    detail_link NVARCHAR(max)
);