import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Database connection
DATABASE_URL = "postgresql://postgres.mqtghilvvxpvjnpzodkd:sianfkp63@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"
engine = create_engine(DATABASE_URL)

class FishOceanographyQuery:
    def __init__(self):
        self.engine = engine
    
    def query_1_fish_with_oceanography_by_location(self, distance_km=50):
        """
        Find fish species and corresponding oceanographic data within a specified distance.
        Uses PostGIS ST_DWithin for spatial proximity matching.
        """
        query = text("""
            SELECT 
                f."scientificName",
                f.species,
                f.habitat,
                f.diet_type,
                f."decimalLatitude" as fish_lat,
                f."decimalLongitude" as fish_lng,
                o.data_set,
                o.version,
                o."waterTemperature",
                o.salinity,
                o."dissolvedOxygen",
                o."water_pH",
                o."chlorophyll_mg_m3",
                o."decimalLatitude" as ocean_lat,
                o."decimalLongitude" as ocean_lng,
                o.measurement_date,
                ST_Distance(f.location::geography, o.location::geography) / 1000 as distance_km,
                ds_f.name as fish_data_source,
                ds_o.name as ocean_data_source
            FROM fish f
            JOIN oceanography o ON ST_DWithin(f.location::geography, o.location::geography, :distance_meters)
            LEFT JOIN data_sources ds_f ON f.data_source_id = ds_f.id
            LEFT JOIN data_sources ds_o ON o.data_source_id = ds_o.id
            WHERE f.location IS NOT NULL 
                AND o.location IS NOT NULL
            ORDER BY distance_km ASC, f."scientificName"
            LIMIT 100;
        """)
        
        distance_meters = distance_km * 1000
        return pd.read_sql(query, self.engine, params={"distance_meters": distance_meters})
    
    def query_2_fish_by_water_temperature_range(self, min_temp=None, max_temp=None):
        """
        Find fish species in areas with specific water temperature ranges.
        """
        conditions = []
        params = {}
        
        if min_temp is not None:
            conditions.append('o."waterTemperature" >= :min_temp')
            params['min_temp'] = min_temp
            
        if max_temp is not None:
            conditions.append('o."waterTemperature" <= :max_temp')
            params['max_temp'] = max_temp
            
        where_clause = ""
        if conditions:
            where_clause = "AND " + " AND ".join(conditions)
        
        query = text(f"""
            SELECT 
                f."scientificName",
                f.species,
                f.family,
                f.habitat,
                f.diet_type,
                f.lifespan_years,
                o."waterTemperature",
                o.salinity,
                o."dissolvedOxygen",
                o."water_pH",
                o.data_set,
                COUNT(*) OVER (PARTITION BY f."scientificName") as location_count,
                AVG(o."waterTemperature") OVER (PARTITION BY f."scientificName") as avg_temp_for_species,
                ST_Distance(f.location::geography, o.location::geography) / 1000 as distance_km
            FROM fish f
            JOIN oceanography o ON ST_DWithin(f.location::geography, o.location::geography, 100000)
            WHERE f.location IS NOT NULL 
                AND o.location IS NOT NULL
                AND o."waterTemperature" IS NOT NULL
                {where_clause}
            ORDER BY f."scientificName", o."waterTemperature"
        """)
        
        return pd.read_sql(query, self.engine, params=params)
    
    def query_3_fish_habitat_ocean_conditions(self):
        """
        Analyze fish habitat preferences based on oceanographic conditions.
        """
        query = text("""
            SELECT 
                f.habitat,
                f.diet_type,
                COUNT(DISTINCT f."scientificName") as species_count,
                COUNT(*) as total_observations,
                AVG(o."waterTemperature") as avg_temperature,
                AVG(o.salinity) as avg_salinity,
                AVG(o."dissolvedOxygen") as avg_dissolved_oxygen,
                AVG(o."water_pH") as avg_ph,
                AVG(o."chlorophyll_mg_m3") as avg_chlorophyll,
                MIN(o."waterTemperature") as min_temperature,
                MAX(o."waterTemperature") as max_temperature,
                STDDEV(o."waterTemperature") as temp_stddev
            FROM fish f
            JOIN oceanography o ON ST_DWithin(f.location::geography, o.location::geography, 50000)
            WHERE f.location IS NOT NULL 
                AND o.location IS NOT NULL
                AND f.habitat IS NOT NULL
                AND f.diet_type IS NOT NULL
                AND o."waterTemperature" IS NOT NULL
            GROUP BY f.habitat, f.diet_type
            HAVING COUNT(*) >= 3
            ORDER BY species_count DESC, avg_temperature;
        """)
        
        return pd.read_sql(query, self.engine)
    
    def query_4_species_environmental_profile(self, scientific_name):
        """
        Get detailed environmental profile for a specific fish species.
        """
        query = text("""
            SELECT 
                f."scientificName",
                f.species,
                f.family,
                f.habitat,
                f.diet_type,
                f.reproductive_type,
                f.lifespan_years,
                f.migration_patterns,
                o.data_set,
                o."waterTemperature",
                o.salinity,
                o."dissolvedOxygen",
                o."water_pH",
                o."chlorophyll_mg_m3",
                o."maximumDepthInMeters" as ocean_depth,
                f."maximumDepthInMeters" as fish_depth_range,
                o.measurement_date,
                f.locality,
                f.fishing_region,
                ST_X(f.location::geometry) as longitude,
                ST_Y(f.location::geometry) as latitude,
                ds_f.name as fish_data_source,
                ds_o.name as ocean_data_source,
                ds_f.description as fish_source_desc,
                ds_o.description as ocean_source_desc
            FROM fish f
            JOIN oceanography o ON ST_DWithin(f.location::geography, o.location::geography, 100000)
            LEFT JOIN data_sources ds_f ON f.data_source_id = ds_f.id
            LEFT JOIN data_sources ds_o ON o.data_source_id = ds_o.id
            WHERE f."scientificName" = :scientific_name
                AND f.location IS NOT NULL 
                AND o.location IS NOT NULL
            ORDER BY o.measurement_date DESC NULLS LAST, o."waterTemperature"
        """)
        
        return pd.read_sql(query, self.engine, params={"scientific_name": scientific_name})
    
    def query_5_seasonal_patterns(self):
        """
        Analyze seasonal patterns in fish observations and oceanographic conditions.
        """
        query = text("""
            SELECT 
                EXTRACT(MONTH FROM o.measurement_date) as month,
                CASE 
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (12, 1, 2) THEN 'Winter'
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (3, 4, 5) THEN 'Spring'
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (6, 7, 8) THEN 'Summer'
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (9, 10, 11) THEN 'Fall'
                    ELSE 'Unknown'
                END as season,
                f.habitat,
                f.migration_patterns,
                COUNT(DISTINCT f."scientificName") as species_count,
                COUNT(*) as observation_count,
                AVG(o."waterTemperature") as avg_temperature,
                AVG(o.salinity) as avg_salinity,
                AVG(o."chlorophyll_mg_m3") as avg_chlorophyll
            FROM fish f
            JOIN oceanography o ON ST_DWithin(f.location::geography, o.location::geography, 75000)
            WHERE f.location IS NOT NULL 
                AND o.location IS NOT NULL
                AND o.measurement_date IS NOT NULL
                AND f.habitat IS NOT NULL
            GROUP BY 
                EXTRACT(MONTH FROM o.measurement_date),
                CASE 
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (12, 1, 2) THEN 'Winter'
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (3, 4, 5) THEN 'Spring'
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (6, 7, 8) THEN 'Summer'
                    WHEN EXTRACT(MONTH FROM o.measurement_date) IN (9, 10, 11) THEN 'Fall'
                    ELSE 'Unknown'
                END,
                f.habitat,
                f.migration_patterns
            HAVING COUNT(*) >= 2
            ORDER BY month, habitat, species_count DESC
        """)
        
        return pd.read_sql(query, self.engine)
    
    def query_6_water_quality_fish_diversity(self):
        """
        Analyze relationship between water quality parameters and fish diversity.
        """
        query = text("""
            WITH water_quality_zones AS (
                SELECT 
                    o.data_set,
                    o.version,
                    o.location,
                    o."waterTemperature",
                    o.salinity,
                    o."dissolvedOxygen",
                    o."water_pH",
                    o."chlorophyll_mg_m3",
                    CASE 
                        WHEN o."dissolvedOxygen" >= 8 THEN 'High Oxygen'
                        WHEN o."dissolvedOxygen" >= 5 THEN 'Medium Oxygen'
                        ELSE 'Low Oxygen'
                    END as oxygen_level,
                    CASE 
                        WHEN o."water_pH" >= 8.2 THEN 'High pH'
                        WHEN o."water_pH" >= 7.8 THEN 'Normal pH'
                        ELSE 'Low pH'
                    END as ph_level,
                    CASE 
                        WHEN o."chlorophyll_mg_m3" >= 10 THEN 'High Productivity'
                        WHEN o."chlorophyll_mg_m3" >= 1 THEN 'Medium Productivity'
                        ELSE 'Low Productivity'
                    END as productivity_level
                FROM oceanography o
                WHERE o."dissolvedOxygen" IS NOT NULL
                    AND o."water_pH" IS NOT NULL
                    AND o."chlorophyll_mg_m3" IS NOT NULL
            )
            SELECT 
                wq.oxygen_level,
                wq.ph_level,
                wq.productivity_level,
                COUNT(DISTINCT f."scientificName") as species_diversity,
                COUNT(*) as total_fish_observations,
                AVG(f.lifespan_years) as avg_lifespan,
                AVG(wq."waterTemperature") as avg_temperature,
                AVG(wq.salinity) as avg_salinity,
                AVG(wq."dissolvedOxygen") as avg_dissolved_oxygen,
                AVG(wq."water_pH") as avg_ph,
                AVG(wq."chlorophyll_mg_m3") as avg_chlorophyll,
                STRING_AGG(DISTINCT f.habitat::text, ', ') as habitats_present,
                STRING_AGG(DISTINCT f.diet_type::text, ', ') as diet_types_present
            FROM water_quality_zones wq
            JOIN fish f ON ST_DWithin(f.location::geography, wq.location::geography, 50000)
            WHERE f.location IS NOT NULL
            GROUP BY wq.oxygen_level, wq.ph_level, wq.productivity_level
            HAVING COUNT(DISTINCT f."scientificName") >= 2
            ORDER BY species_diversity DESC, total_fish_observations DESC
        """)
        
        return pd.read_sql(query, self.engine)
    
    def query_7_geographic_species_distribution(self):
        """
        Analyze geographic distribution of species with oceanographic context.
        """
        query = text("""
            SELECT 
                f."scientificName",
                f.species,
                f.family,
                COUNT(DISTINCT o.data_set) as different_ocean_datasets,
                COUNT(*) as total_locations,
                AVG(ST_Y(f.location::geometry)) as avg_latitude,
                AVG(ST_X(f.location::geometry)) as avg_longitude,
                MIN(ST_Y(f.location::geometry)) as min_latitude,
                MAX(ST_Y(f.location::geometry)) as max_latitude,
                MAX(ST_Y(f.location::geometry)) - MIN(ST_Y(f.location::geometry)) as latitude_range,
                AVG(o."waterTemperature") as avg_water_temp,
                STDDEV(o."waterTemperature") as temp_variability,
                AVG(o.salinity) as avg_salinity,
                AVG(o."maximumDepthInMeters") as avg_depth,
                STRING_AGG(DISTINCT f.fishing_region, ', ') as fishing_regions,
                STRING_AGG(DISTINCT f.locality, ', ') as localities
            FROM fish f
            JOIN oceanography o ON ST_DWithin(f.location::geography, o.location::geography, 100000)
            WHERE f.location IS NOT NULL 
                AND o.location IS NOT NULL
                AND o."waterTemperature" IS NOT NULL
            GROUP BY f."scientificName", f.species, f.family
            HAVING COUNT(*) >= 3
            ORDER BY latitude_range DESC, total_locations DESC
            LIMIT 50
        """)
        
        return pd.read_sql(query, self.engine)
    
    def custom_query(self, query_string, params=None):
        """
        Execute a custom query with optional parameters.
        """
        try:
            if params:
                query = text(query_string)
                return pd.read_sql(query, self.engine, params=params)
            else:
                return pd.read_sql(query_string, self.engine)
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

# Example usage and testing
if __name__ == "__main__":
    # Initialize the query class
    querier = FishOceanographyQuery()
    
    print("🐟 Fish & Oceanography Query Examples")
    print("=" * 50)
    
    try:
        # Example 1: Fish and oceanography within 25km
        print("\n1. Fish species with nearby oceanographic data (within 25km):")
        result1 = querier.query_1_fish_with_oceanography_by_location(distance_km=25)
        print(f"Found {len(result1)} matches")
        if not result1.empty:
            print(result1.head())
        
        # Example 2: Fish in warm waters (20-30°C)
        print("\n2. Fish species in warm waters (20-30°C):")
        result2 = querier.query_2_fish_by_water_temperature_range(min_temp=20, max_temp=30)
        print(f"Found {len(result2)} records")
        if not result2.empty:
            print(result2[['scientificName', 'species', 'waterTemperature', 'habitat']].head())
        
        # Example 3: Habitat analysis
        print("\n3. Fish habitat vs ocean conditions analysis:")
        result3 = querier.query_3_fish_habitat_ocean_conditions()
        print(f"Found {len(result3)} habitat-diet combinations")
        if not result3.empty:
            print(result3.head())
        
        # Example 4: Species environmental profile (if we have species)
        if not result1.empty and 'scientificName' in result1.columns:
            species_name = result1['scientificName'].iloc[0]
            print(f"\n4. Environmental profile for {species_name}:")
            result4 = querier.query_4_species_environmental_profile(species_name)
            print(f"Found {len(result4)} environmental records")
            if not result4.empty:
                print(result4[['waterTemperature', 'salinity', 'dissolvedOxygen', 'measurement_date']].head())
        
        # Example 5: Seasonal patterns
        print("\n5. Seasonal patterns analysis:")
        result5 = querier.query_5_seasonal_patterns()
        print(f"Found {len(result5)} seasonal pattern records")
        if not result5.empty:
            print(result5.head())
        
        # Example 6: Water quality and fish diversity
        print("\n6. Water quality vs fish diversity:")
        result6 = querier.query_6_water_quality_fish_diversity()
        print(f"Found {len(result6)} water quality zones")
        if not result6.empty:
            print(result6.head())
        
        # Example 7: Geographic distribution
        print("\n7. Geographic species distribution:")
        result7 = querier.query_7_geographic_species_distribution()
        print(f"Found {len(result7)} species with geographic analysis")
        if not result7.empty:
            print(result7.head())
            
    except Exception as e:
        print(f"Error running queries: {e}")
    
    print("\n" + "=" * 50)
    print("Query examples completed!")
    print("\nYou can use any of these query methods individually:")
    print("- querier.query_1_fish_with_oceanography_by_location(25)")
    print("- querier.query_2_fish_by_water_temperature_range(15, 25)")
    print("- querier.query_3_fish_habitat_ocean_conditions()")
    print("- querier.query_4_species_environmental_profile('Salmo salar')")
    print("- querier.query_5_seasonal_patterns()")
    print("- querier.query_6_water_quality_fish_diversity()")
    print("- querier.query_7_geographic_species_distribution()")
    print("- querier.custom_query('SELECT * FROM fish LIMIT 5')")