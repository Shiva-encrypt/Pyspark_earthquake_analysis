CREATE TABLE neic_earthquakes (
    Date DATE,
    Time TIME,
    Latitude FLOAT,
    Longitude FLOAT,
    Type VARCHAR(50),
    Depth FLOAT,
    Depth_Error FLOAT,
    Depth_Seismic_Stations INT,
    Magnitude FLOAT,
    Magnitude_Type VARCHAR(10),
    Magnitude_Error FLOAT,
    Magnitude_Seismic_Stations INT,
    Azimuthal_Gap FLOAT,
    Horizontal_Distance FLOAT,
    Horizontal_Error FLOAT,
    Root_Mean_Square FLOAT,
    ID VARCHAR(20),
    Source VARCHAR(20),
    Location_Source VARCHAR(20),
    Magnitude_Source VARCHAR(20),
    Status VARCHAR(20)
);
