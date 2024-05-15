-- SQL Script to create a Data Warehouse in Google BigQuery within the 'cis4400' dataset

-- Create a Date Dimension Table in the 'cis4400' dataset
CREATE TABLE cis4400.DateDimensionRefined (
    DateKey STRING NOT NULL,
    Date DATE,
    Year INT64,
    Quarter INT64,
    Month INT64,
    Day INT64,
    WeekdayName STRING
) OPTIONS(
    description="A dimension table for dates"
);

-- Create an Incidents Fact Table in the 'cis4400' dataset
CREATE TABLE cis4400.IncidentsFactRefined (
    IncidentKey STRING NOT NULL,
    DateKey STRING,
    SomeMeasure INT64,
    OtherMeasure INT64
) OPTIONS(
    description="A fact table for incidents"
);


-- Insert a sample date into DateDimension
INSERT INTO cis4400.DateDimensionRefined (DateKey, Date, Year, Quarter, Month, Day, WeekdayName)
VALUES (GENERATE_UUID(), '2023-10-01', 2023, 4, 10, 1, 'Sunday');

-- Insert a sample incident into IncidentsFact linking to the DateDimension
INSERT INTO cis4400.IncidentsFactRefined (IncidentKey, DateKey, SomeMeasure, OtherMeasure)
VALUES (GENERATE_UUID(), (SELECT DateKey FROM cis4400.DateDimensionRefined WHERE Date = '2023-10-01'), 100, 200);
