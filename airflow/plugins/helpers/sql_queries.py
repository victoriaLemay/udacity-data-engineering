class SqlQueries:
    port_codes_table_insert = ("""
            SELECT
                port_code_staging.name,
                us_states.id,
                port_code_staging.i94_port_code
            FROM
                port_code_staging
            LEFT JOIN
                us_states ON port_code_staging.state_code = us_states.state_code
        """)

    us_city_demographics_table_insert = ("""
            SELECT DISTINCT
                port_codes.id,
                demographic_staging.City,
                demographic_staging.MedianAge,
                demographic_staging.MalePopulation,
                demographic_staging.FemalePopulation,
                demographic_staging.TotalPopulation,
                demographic_staging.NumberOfVeterans,
                demographic_staging.ForeignBorn,
                demographic_staging.AverageHouseholdSize,
                demographic_staging.Race,
                demographic_staging.Count
            FROM
                demographic_staging
            LEFT JOIN
                us_states ON demographic_staging.State = us_states.name
            LEFT JOIN
            	port_codes ON port_codes.state_id = us_states.id
            WHERE 
                LOWER(demographic_staging.City) = LOWER(port_codes.city)
        """)

    land_temperatures_table_insert = ("""
        SELECT
            to_timestamp(land_temp_staging.dt, 'YYYY-MM-DD'),
            land_temp_staging.AverageTemperature,
            land_temp_staging.AverageTemperatureUncertainty,
            land_temp_staging.City,
            country_codes.id,
            land_temp_staging.Latitude,
            land_temp_staging.Longitude
        FROM
            land_temp_staging
        LEFT JOIN
            country_codes ON LOWER(land_temp_staging.Country) = LOWER(country_codes.name)
    """)

    i94_immigration_table_insert = ("""
        SELECT
            convert(integer, i94_immigration_staging.i94yr),
            convert(integer, i94_immigration_staging.i94mon),
            country_codes.id as country_code_id,
            port_codes.id as port_code_id,
            travel_modes.id as travel_mode_id,
            dateadd(day, convert(integer, i94_immigration_staging.arrdate), '1960-01-01'),
            dateadd(day, convert(integer, i94_immigration_staging.depdate), '1960-01-01'),
            convert(integer, i94bir),
            visa_types.id,
            visa_port_codes.id,
            i94_immigration_staging.occup,
            convert(integer, i94_immigration_staging.biryear),
            i94_immigration_staging.gender,
            i94_immigration_staging.visa_type
        FROM
            i94_immigration_staging
        LEFT JOIN
            country_codes ON i94_immigration_staging.i94res = country_codes.i94_code
        LEFT JOIN
            port_codes ON TRIM(i94_immigration_staging.i94port) = TRIM(port_codes.i94_port_code)
        LEFT JOIN
            port_codes AS visa_port_codes ON TRIM(i94_immigration_staging.visapost) = TRIM(port_codes.i94_port_code)
        LEFT JOIN
            visa_types ON i94_immigration_staging.i94visa = visa_types.visa_type_code
        LEFT JOIN
            travel_modes ON convert(integer, i94_immigration_staging.i94mode) = travel_modes.travel_mode_code
    """)

    i94_immigration_by_year_month_port_table_insert = ("""
        SELECT 
	        i94_immigration.year, 
            i94_immigration.month, 
            port_codes.city,
            us_states.state_code,
            port_codes.i94_port_code,
            COUNT(i94_immigration.id) as total, 
            COUNT(CASE WHEN i94_immigration.gender = 'M' THEN 1 END) as total_men,
	        COUNT(CASE WHEN i94_immigration.gender = 'F' THEN 1 END) as total_women,
	        AVG(i94_immigration.respondent_age) as avg_age
        FROM 
	        i94_immigration
        LEFT JOIN 
	        port_codes ON i94_immigration.port_id = port_codes.id
        LEFT JOIN
	        us_states ON port_codes.state_id = us_states.id
        GROUP BY 
	        i94_immigration.year, 
            i94_immigration.month, 
            port_codes.city,
            us_states.state_code,
            port_codes.i94_port_code;
    """)
