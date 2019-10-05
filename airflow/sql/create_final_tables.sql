CREATE TABLE IF NOT EXISTS public.i94_immigration (
    id int4 identity(1,1),
    year int4,
    month int4,
    res_country_code_id int4,
    port_id int4,
    travel_mode_id int4,
    arrival_date timestamp not null,
    departure_date timestamp,
    respondent_age int4,
    visa_type_code_id int4,
    visa_post_code_id int4,
    occupation varchar(256),
    birth_year int4,
    gender char(1),
    visa_type varchar(12),
    CONSTRAINT i94_immigration_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.country_codes (
    id int4 identity(1,1),
    name varchar(256) not null,
    i94_code int4 not null,
    CONSTRAINT country_codes_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.travel_modes (
    id int4 identity(1,1),
    name varchar(256) not null,
    travel_mode_code int4 not null,
    CONSTRAINT travel_modes_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.port_codes (
    id int4 identity(1,1),
    city varchar(512),
    state_id int4,
    i94_port_code varchar(12) not null,
    CONSTRAINT port_codes_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.us_states (
    id int4 identity(1,1),
    name varchar(256) not null,
    state_code varchar(3) not null,
    CONSTRAINT us_states_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.visa_types (
    id int4 identity(1,1),
    name varchar(256) not null,
    visa_type_code int4 not null,
    CONSTRAINT visa_types_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.land_temperatures (
    id int4 identity(1,1),
    date timestamp not null,
    avg_temp numeric(20,16),
    avg_temp_uncertainty numeric(20,16),
    city varchar(256),
    country_code_id int4,
    latitude varchar(12),
    longitude varchar(12),
    CONSTRAINT land_temperatures_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.us_city_demographics (
    id int4 identity(1,1),
    port_code_id int4,
    city varchar(128),
    median_age numeric(5,2),
    male_pop int4,
    female_pop int4,
    total_pop int4,
    num_vets int4,
    foreign_pop int4,
    avg_household_size numeric(3,1),
    race varchar(256),
    race_pop int4,
    CONSTRAINT us_city_demographics_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.i94_immigration_by_year_month_port (
    id int4 identity(1,1),
    year int4,
    month int4,
    city varchar(128),
    state_code varchar(3),
    i94_port_code varchar(4),
    total int4,
    total_men int4,
	total_women int4,
	avg_age int4,
	CONSTRAINT i94_rollup_pkey PRIMARY KEY (id)
);
