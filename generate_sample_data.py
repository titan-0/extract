import random
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import numpy as np

# Database connection
DATABASE_URL = "postgresql://postgres.mqtghilvvxpvjnpzodkd:sianfkp63@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"
engine = create_engine(DATABASE_URL)

def generate_sample_data():
    """Generate sample data for both fish and oceanography tables"""
    
    print("🐟 Generating sample data for fish and oceanography tables...")
    
    # Sample fish species data (expanded with more realistic species)
    fish_species = [
        # Commercial fish species
        ("Thunnus thynnus", "Atlantic Bluefin Tuna", "Actinopterygii", "Scombridae", "marine", "carnivore", "oviparous", 15.0),
        ("Salmo salar", "Atlantic Salmon", "Actinopterygii", "Salmonidae", "marine", "carnivore", "oviparous", 8.0),
        ("Gadus morhua", "Atlantic Cod", "Actinopterygii", "Gadidae", "marine", "carnivore", "oviparous", 12.0),
        ("Clupea harengus", "Atlantic Herring", "Actinopterygii", "Clupeidae", "marine", "planktivore", "oviparous", 6.0),
        ("Scomber scombrus", "Atlantic Mackerel", "Actinopterygii", "Scombridae", "marine", "carnivore", "oviparous", 7.0),
        ("Pleuronectes platessa", "European Plaice", "Actinopterygii", "Pleuronectidae", "marine", "carnivore", "oviparous", 10.0),
        ("Solea solea", "Common Sole", "Actinopterygii", "Soleidae", "marine", "carnivore", "oviparous", 9.0),
        ("Merlangius merlangus", "Whiting", "Actinopterygii", "Gadidae", "marine", "carnivore", "oviparous", 5.0),
        ("Pollachius virens", "Pollock", "Actinopterygii", "Gadidae", "marine", "carnivore", "oviparous", 8.0),
        ("Melanogrammus aeglefinus", "Haddock", "Actinopterygii", "Gadidae", "marine", "carnivore", "oviparous", 7.0),
        
        # Deep water species
        ("Microstomus kitt", "Lemon Sole", "Actinopterygii", "Pleuronectidae", "marine", "carnivore", "oviparous", 8.0),
        ("Limanda limanda", "Common Dab", "Actinopterygii", "Pleuronectidae", "marine", "carnivore", "oviparous", 4.0),
        ("Hippoglossus hippoglossus", "Atlantic Halibut", "Actinopterygii", "Pleuronectidae", "marine", "carnivore", "oviparous", 25.0),
        ("Molva molva", "Ling", "Actinopterygii", "Lotidae", "marine", "carnivore", "oviparous", 15.0),
        ("Sebastes norvegicus", "Golden Redfish", "Actinopterygii", "Sebastidae", "marine", "carnivore", "viviparous", 20.0),
        
        # Pelagic species
        ("Sardina pilchardus", "European Sardine", "Actinopterygii", "Clupeidae", "marine", "planktivore", "oviparous", 4.0),
        ("Trachurus trachurus", "Atlantic Horse Mackerel", "Actinopterygii", "Carangidae", "marine", "carnivore", "oviparous", 6.0),
        ("Sprattus sprattus", "European Sprat", "Actinopterygii", "Clupeidae", "marine", "planktivore", "oviparous", 3.0),
        ("Engraulis encrasicolus", "European Anchovy", "Actinopterygii", "Engraulidae", "marine", "planktivore", "oviparous", 4.0),
        ("Scomberesox saurus", "Atlantic Saury", "Actinopterygii", "Scomberesocidae", "marine", "carnivore", "oviparous", 5.0),
        
        # Demersal species
        ("Glyptocephalus cynoglossus", "Witch Flounder", "Actinopterygii", "Pleuronectidae", "marine", "carnivore", "oviparous", 12.0),
        ("Reinhardtius hippoglossoides", "Greenland Halibut", "Actinopterygii", "Pleuronectidae", "marine", "carnivore", "oviparous", 18.0),
        ("Anarhichas lupus", "Atlantic Wolffish", "Actinopterygii", "Anarhichadidae", "marine", "carnivore", "oviparous", 12.0),
        ("Cyclopterus lumpus", "Lumpfish", "Actinopterygii", "Cyclopteridae", "marine", "carnivore", "oviparous", 7.0),
        ("Brosme brosme", "Tusk", "Actinopterygii", "Lotidae", "marine", "carnivore", "oviparous", 14.0),
        
        # Cartilaginous fish
        ("Raja clavata", "Thornback Ray", "Chondrichthyes", "Rajidae", "marine", "carnivore", "oviparous", 15.0),
        ("Scyliorhinus canicula", "Small-spotted Catshark", "Chondrichthyes", "Scyliorhinidae", "marine", "carnivore", "oviparous", 8.0),
        ("Squalus acanthias", "Spiny Dogfish", "Chondrichthyes", "Squalidae", "marine", "carnivore", "ovoviviparous", 20.0),
        ("Mustelus mustelus", "Smooth-hound", "Chondrichthyes", "Triakidae", "marine", "carnivore", "ovoviviparous", 12.0),
        ("Torpedo nobiliana", "Electric Ray", "Chondrichthyes", "Torpedinidae", "marine", "carnivore", "ovoviviparous", 10.0),
        
        # Arctic/subarctic species
        ("Boreogadus saida", "Arctic Cod", "Actinopterygii", "Gadidae", "marine", "carnivore", "oviparous", 6.0),
        ("Mallotus villosus", "Capelin", "Actinopterygii", "Osmeridae", "marine", "planktivore", "oviparous", 5.0),
        ("Arctogadus glacialis", "Arctic Cod", "Actinopterygii", "Gadidae", "marine", "carnivore", "oviparous", 7.0),
        ("Eleginus gracilis", "Saffron Cod", "Actinopterygii", "Gadidae", "marine", "carnivore", "oviparous", 5.0),
        ("Triglops murrayi", "Moustache Sculpin", "Actinopterygii", "Cottidae", "marine", "carnivore", "oviparous", 4.0),
    ]
    
    # Geographic regions (expanded with more diverse marine areas)
    regions = [
        # North Atlantic
        {"name": "North Sea", "lat_range": (51, 61), "lng_range": (-4, 9), "temp_base": 10},
        {"name": "Baltic Sea", "lat_range": (53, 66), "lng_range": (10, 30), "temp_base": 8},
        {"name": "Norwegian Sea", "lat_range": (62, 75), "lng_range": (-5, 15), "temp_base": 6},
        {"name": "Celtic Sea", "lat_range": (48, 52), "lng_range": (-11, -4), "temp_base": 12},
        {"name": "Bay of Biscay", "lat_range": (43, 48), "lng_range": (-10, -1), "temp_base": 14},
        {"name": "Barents Sea", "lat_range": (70, 81), "lng_range": (16, 60), "temp_base": 2},
        
        # Extended Atlantic areas
        {"name": "Faroe Islands", "lat_range": (61, 63), "lng_range": (-8, -6), "temp_base": 8},
        {"name": "Iceland Waters", "lat_range": (63, 67), "lng_range": (-25, -13), "temp_base": 5},
        {"name": "Greenland Sea", "lat_range": (70, 80), "lng_range": (-20, 0), "temp_base": 1},
        {"name": "Labrador Sea", "lat_range": (55, 65), "lng_range": (-65, -45), "temp_base": 4},
        {"name": "Grand Banks", "lat_range": (42, 48), "lng_range": (-55, -40), "temp_base": 8},
        {"name": "Scotian Shelf", "lat_range": (42, 46), "lng_range": (-67, -57), "temp_base": 10},
        
        # Mediterranean influence areas
        {"name": "Western English Channel", "lat_range": (49, 51), "lng_range": (-6, -2), "temp_base": 13},
        {"name": "Portuguese Coast", "lat_range": (37, 42), "lng_range": (-10, -8), "temp_base": 16},
        {"name": "Cantabrian Sea", "lat_range": (43, 44), "lng_range": (-9, -2), "temp_base": 15},
    ]
    
    # Create data source first
    data_source_query = text("""
        INSERT INTO data_sources (name, description, source_type) 
        VALUES ('sample_marine_data', 'Generated sample data for testing queries', 'Database')
        ON CONFLICT (name) DO UPDATE SET description = EXCLUDED.description
        RETURNING id
    """)
    
    with engine.begin() as conn:
        result = conn.execute(data_source_query)
        data_source_id = result.fetchone()[0]
        print(f"Created/updated data source with ID: {data_source_id}")
    
    # Generate fish data (500 records)
    fish_data = []
    fishing_regions = ['IVa', 'IVb', 'IVc', 'VIa', 'VIb', 'VIc', 'VIIa', 'VIIb', 'VIIc', 'VIId', 'VIIe', 'VIIf', 'VIIg', 'VIIh', 'VIIj', 'VIIk', 'VIIIa', 'VIIIb', 'VIIIc', 'VIIId', 'VIIIe', 'IXa', 'IXb', 'Xa', 'XIIa', 'XIIb', 'XIVa', 'XIVb']
    
    for i in range(500):
        species = random.choice(fish_species)
        region = random.choice(regions)
        
        # Generate coordinates within region
        lat = random.uniform(region["lat_range"][0], region["lat_range"][1])
        lng = random.uniform(region["lng_range"][0], region["lng_range"][1])
        
        # More realistic depth ranges based on species
        if "Halibut" in species[1] or "Redfish" in species[1]:
            depth_range = f"{random.randint(50, 200)}-{random.randint(400, 1500)}m"
        elif "Cod" in species[1] or "Haddock" in species[1]:
            depth_range = f"{random.randint(20, 100)}-{random.randint(200, 600)}m"
        elif "Herring" in species[1] or "Mackerel" in species[1] or "Sardine" in species[1]:
            depth_range = f"{random.randint(5, 50)}-{random.randint(100, 300)}m"
        elif "Ray" in species[1] or "Shark" in species[1]:
            depth_range = f"{random.randint(10, 150)}-{random.randint(300, 800)}m"
        else:
            depth_range = f"{random.randint(10, 100)}-{random.randint(200, 500)}m"
        
        # Realistic migration patterns
        migration_patterns = {
            "Tuna": "Long-distance",
            "Salmon": "Anadromous",
            "Herring": "Seasonal",
            "Cod": "Short-distance",
            "Mackerel": "Seasonal",
            "Plaice": "Spawning migration",
            "Sole": "Seasonal",
            "Ray": "Non-migratory",
            "Shark": "Long-distance"
        }
        
        migration = "Seasonal"  # default
        for key, value in migration_patterns.items():
            if key in species[1]:
                migration = value
                break
        
        fish_record = {
            'scientificName': f"{species[0]}_{i:03d}",  # Make unique
            'species': species[1],
            'class': species[2],
            'family': species[3],
            'location': f"POINT({lng} {lat})",
            'decimalLatitude': lat,
            'decimalLongitude': lng,
            'locality': region["name"],
            'kingdom': 'Animalia',
            'fishing_region': f"ICES_{random.choice(fishing_regions)}",
            'maximumDepthInMeters': depth_range,
            'lifespan_years': max(1, species[7] + random.uniform(-3, 8)),  # Add more variation
            'migration_patterns': migration,
            'synonyms': f"{{Syn_{i}_0,Syn_{i}_1,Syn_{i}_2}}",
            'reproductive_type': species[6],
            'habitat': species[4],
            'phylum': 'Chordata',
            'diet_type': species[5],
            'data_source_id': data_source_id
        }
        fish_data.append(fish_record)
    
    # Generate oceanography data (500 records)
    ocean_datasets = [
        "NOAA_BUOY", "ICES_CTD", "COPERNICUS_MARINE", "UK_METOFFICE", 
        "NORWEGIAN_INSTITUTE", "SMHI_SWEDEN", "DMI_DENMARK", "FINNISH_METEOROLOGICAL",
        "EURO_ARGO", "SEADATANET", "EMODnet_PHYSICS", "PANGAEA", "BODC",
        "GEOMAR", "AWI_GERMANY", "IFREMER_FRANCE", "CNR_ITALY", "CSIC_SPAIN"
    ]
    
    ocean_data = []
    base_date = datetime(2022, 1, 1)  # Extended time range
    
    for i in range(500):
        region = random.choice(regions)
        
        # Generate coordinates within region (but slightly different from fish)
        lat = random.uniform(region["lat_range"][0], region["lat_range"][1])
        lng = random.uniform(region["lng_range"][0], region["lng_range"][1])
        
        # More realistic depth distribution
        depth_weights = [0.3, 0.25, 0.2, 0.15, 0.1]  # Surface to deep
        depth_ranges = [(5, 50), (50, 200), (200, 500), (500, 1000), (1000, 3000)]
        chosen_range = random.choices(depth_ranges, weights=depth_weights)[0]
        depth = random.uniform(chosen_range[0], chosen_range[1])
        
        # Season-based variations
        measurement_date = base_date + timedelta(days=random.randint(0, 1095))  # 3 years of data
        season = (measurement_date.month - 1) // 3  # 0=winter, 1=spring, 2=summer, 3=autumn
        
        # Temperature varies by latitude, season, and depth
        base_temp = region["temp_base"]
        seasonal_adjustments = [-3, 0, 4, 1]  # winter, spring, summer, autumn
        seasonal_var = seasonal_adjustments[season] + random.uniform(-2, 2)
        depth_effect = -depth * 0.01  # Temperature decreases with depth
        water_temp = max(0.1, base_temp + seasonal_var + depth_effect + random.uniform(-1, 1))  # Ensure > 0
        
        # Realistic salinity based on region
        if "Baltic" in region["name"]:
            base_salinity = random.uniform(6, 15)  # Baltic Sea is brackish
        elif "Arctic" in region["name"] or "Greenland" in region["name"]:
            base_salinity = random.uniform(32, 35)  # Lower due to ice melt
        else:
            base_salinity = random.uniform(33, 37)  # Normal marine salinity
        
        # Dissolved oxygen varies with temperature and depth
        oxygen_base = 14 - (water_temp * 0.3) - (depth * 0.005)  # Colder/shallower = more oxygen
        dissolved_oxygen = max(2, oxygen_base + random.uniform(-2, 2))
        
        # pH varies slightly with depth and region
        ph_base = 8.1 - (depth * 0.0002)  # Slightly lower pH at depth
        ph_value = max(7.5, min(8.4, ph_base + random.uniform(-0.2, 0.2)))
        
        # Chlorophyll varies seasonally and with depth
        if depth < 100 and season in [1, 2]:  # Spring/summer bloom in surface waters
            chlorophyll = np.random.gamma(3, 2)  # Higher values more likely
        elif depth < 50:
            chlorophyll = np.random.gamma(2, 1)
        else:
            chlorophyll = np.random.exponential(0.5)  # Low deep water values
        
        # Realistic nutrient patterns
        nitrate = max(0, 15 - (chlorophyll * 0.8) + random.uniform(-3, 3))  # Inverse with chlorophyll
        phosphate = max(0, 1.5 - (chlorophyll * 0.1) + random.uniform(-0.3, 0.3))
        
        ocean_record = {
            'data_set': random.choice(ocean_datasets),
            'version': f"v{random.randint(1, 8)}.{random.randint(0, 9)}_{i:03d}",  # Make unique
            'location': f"POINT({lng} {lat})",
            'decimalLatitude': lat,
            'decimalLongitude': lng,
            'maximumDepthInMeters': depth,
            'waterTemperature': round(water_temp, 2),
            'salinity': round(base_salinity, 2),
            'dissolvedOxygen': round(dissolved_oxygen, 2),
            'water_pH': round(ph_value, 2),
            'chlorophyll_mg_m3': round(chlorophyll, 3),
            'nutrients': '{"nitrate": ' + str(round(nitrate, 2)) + ', "phosphate": ' + str(round(phosphate, 3)) + '}',
            'pressure_bar': round(depth * 0.1 + random.uniform(-0.5, 0.5), 2),
            'density_kg_m3': round(1025 + (base_salinity - 35) * 0.8 - (water_temp - 4) * 0.2, 2),
            'turbidity': round(np.random.exponential(1.5), 2),
            'alkalinity': round(2200 + base_salinity * 10 + random.uniform(-100, 100), 2),
            'surface_currents': round(np.random.exponential(0.8), 2),
            'measurement_date': measurement_date,
            'data_source_id': data_source_id
        }
        ocean_data.append(ocean_record)
    
    # Insert fish data
    print("Inserting fish data...")
    fish_columns = list(fish_data[0].keys())
    quoted_columns = [f'"{col}"' for col in fish_columns]
    
    fish_insert_sql = f"""
        INSERT INTO fish ({", ".join(quoted_columns)}) 
        VALUES ({", ".join([f":{col}" for col in fish_columns])})
        ON CONFLICT ("scientificName") DO UPDATE SET
        species = EXCLUDED.species,
        "decimalLatitude" = EXCLUDED."decimalLatitude",
        "decimalLongitude" = EXCLUDED."decimalLongitude"
    """
    
    with engine.begin() as conn:
        conn.execute(text(fish_insert_sql), fish_data)
        print(f"✅ Inserted {len(fish_data)} fish records")
    
    # Insert oceanography data
    print("Inserting oceanography data...")
    ocean_columns = list(ocean_data[0].keys())
    quoted_ocean_columns = [f'"{col}"' for col in ocean_columns]
    
    ocean_insert_sql = f"""
        INSERT INTO oceanography ({", ".join(quoted_ocean_columns)}) 
        VALUES ({", ".join([f":{col}" for col in ocean_columns])})
        ON CONFLICT (data_set, version) DO UPDATE SET
        "waterTemperature" = EXCLUDED."waterTemperature",
        salinity = EXCLUDED.salinity
    """
    
    with engine.begin() as conn:
        conn.execute(text(ocean_insert_sql), ocean_data)
        print(f"✅ Inserted {len(ocean_data)} oceanography records")
    
    print("🎉 Sample data generation completed!")
    
    # Show summary
    with engine.connect() as conn:
        fish_count = conn.execute(text("SELECT COUNT(*) FROM fish")).fetchone()[0]
        ocean_count = conn.execute(text("SELECT COUNT(*) FROM oceanography")).fetchone()[0]
        print(f"📊 Total records - Fish: {fish_count}, Oceanography: {ocean_count}")

if __name__ == "__main__":
    generate_sample_data()