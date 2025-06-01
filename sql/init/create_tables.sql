-- create parent table
/*silver table after clean and transform data*/
CREATE TABLE IF NOT EXISTS car_informations (
    car_id SERIAL PRIMARY KEY,
    car_name VARCHAR(50) NOT NULL,
    brand_id INT NOT NULL,
    spec_id INT NOT NULL,
    type_id INT NOT NULL,
    condition VARCHAR(25),
    date_id INT NOT NULL,
    link VARCHAR(500),
    price FLOAT(6),
    created_at TIMESTAMP NOT NULL,
    last_login TIMESTAMP
);

CREATE TABLE brand (
  brand_id SERIAL PRIMARY KEY,
  brand VARCHAR(50) NOT NULL,
  model VARCHAR(50) NOT NULL,
  variant VARCHAR(50) NOT NULL,
  body_type VARCHAR(50) NOT NULL,
);

CREATE TABLE IF NOT EXISTS body_type (
  body_id SERIAL PRIMARY KEY,
  body VARCHAR(25) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS spec (
  spec_id SERIAL PRIMARY KEY,
  gear_id INT NOT NULL,
  cc_id INT NOT NULL,
);

CREATE TABLE IF NOT EXISTS gear (
  gear_id SERIAL PRIMARY KEY,
  gear_type VARCHAR(25) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS cc (
  cc_id SERIAL PRIMARY KEY,
  cc_value INT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS date_info (
  date_id SERIAL PRIMARY KEY,
  date_manufactured VARCHAR(25) NOT NULL,
  date_posted VARCHAR(25) NOT NULL,
  date_extracted VARCHAR(25) NOT NULL
);

/*create gold table for insight data*/
CREATE TABLE IF NOT EXISTS aftermarket_price (
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


CREATE TABLE IF NOT EXISTS price_trends (
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

CREATE TABLE IF NOT EXISTS meta (
  table_name text not null,
  column_name text not null,
  attribute_name text not null,
  attribute_value text not null,
  primary key (table_name, column_name, attribute_name)
);