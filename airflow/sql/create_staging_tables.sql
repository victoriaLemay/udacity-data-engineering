CREATE TABLE IF NOT EXISTS public.i94_immigration_staging (
    cicid float,
    i94yr float,
    i94mon float,
    i94cit float,
    i94res float,
    i94port varchar,
    arrdate float,
    i94mode float,
    i94addr varchar,
    depdate float,
    i94bir float,
    i94visa float,
    "count" float,
    dtadfile varchar,
    visapost varchar,
    occup varchar,
    entdepa varchar,
    entdepd varchar,
    entdepu varchar,
    matflag varchar,
    biryear float,
    dtaddto varchar,
    gender varchar,
    insnum varchar,
    airline varchar,
    admnum float,
    fltno varchar,
    visa_type varchar
);

CREATE TABLE IF NOT EXISTS public.land_temp_staging (
    dt varchar,
    AverageTemperature numeric(20,16),
    AverageTemperatureUncertainty numeric(20,16),
    City varchar,
    Country varchar,
    Latitude varchar,
    Longitude varchar
);

CREATE TABLE IF NOT EXISTS public.demographic_staging (
    City varchar,
    State varchar,
    MedianAge numeric(5,2),
    MalePopulation int4,
    FemalePopulation int4,
    TotalPopulation int4,
    NumberOfVeterans int4,
    ForeignBorn int4,
    AverageHouseholdSize numeric(3,1),
    StateCode varchar,
    Race varchar,
    Count int4
);

CREATE TABLE IF NOT EXISTS public.port_code_staging (
    name varchar,
    state_code varchar,
    country_name varchar,
    i94_port_code varchar
);