-- ADD STATEMENT TO CREATE THE DATABASE IN POSTGRESQL
CREATE DATABASE WasteCollectionDW;

-- ADD STATEMENT TO USE THE DATABASE IN POSTGRESQL
\c WasteCollectionDW;

-- ADD STATEMENT TO CREATE THE TABLES IN POSTGRESQL
CREATE TABLE "DimDate"(
    dateid integer NOT NULL PRIMARY KEY,
    date DATE NOT NULL,
    Day integer NOT NULL,
    Weekday integer NOT NULL,
    WeekdayName varchar(10) NOT NULL,    
    Month integer NOT NULL,
    Monthname varchar(10) NOT NULL,
    Quarter integer NOT NULL,
    QuarterName varchar(2) NOT NULL,
    Year integer NOT NULL
);


CREATE TABLE "DimStation"(
    Stationid integer NOT NULL PRIMARY KEY,
    City varchar(50) NOT NULL
);


CREATE TABLE "DimTruck"(
    Truckid integer NOT NULL PRIMARY KEY,
    TruckType varchar(20) NOT NULL
);


CREATE TABLE "FactTrip"(
    Tripid integer NOT NULL PRIMARY KEY,
    Wastecollected decimal(6,2) NOT NULL,
    Dateid integer NOT NULL,
    Stationid integer NOT NULL,
    Truckid integer NOT null,
    FOREIGN KEY (Dateid) REFERENCES "DimDate" (dateid),
    FOREIGN KEY (Stationid) REFERENCES "DimStation" (Stationid),
    FOREIGN KEY (Truckid) REFERENCES "DimTruck" (Truckid)
);