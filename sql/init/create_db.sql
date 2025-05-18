CREATE DATABASE car_collection_data
 OWNER =  cdmaza
 ENCODING = 'UTF8'
 CONNECTION LIMIT = 5;

CREATE TABLE car_collection_data.cars (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price INTEGER NOT NULL
);

CREATE TABLE car_collection_data.cars (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price INTEGER NOT NULL
);