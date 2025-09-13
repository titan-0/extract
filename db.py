from sqlalchemy import (
    create_engine, Column, String, Float, JSON,
    Enum, MetaData, Table, PrimaryKeyConstraint,ForeignKeyConstraint,Text,Date
)
from sqlalchemy.dialects.postgresql import ARRAY,JSONB
from geoalchemy2 import Geography

DATABASE_URL = "postgresql://postgres.mqtghilvvxpvjnpzodkd:sianfkp63@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Fish table with location as geography(Point)
fish = Table(
    "fish", metadata,
    Column("scientific_name", String(150), primary_key=True),
    Column("species", String(100)),
    Column("class", String(100)),
    Column("family", String(100)),
    Column("location", Geography(geometry_type='POINT', srid=4326)),  # 🌍 single column
    Column("location_lat", Float),  # Latitude coordinate
    Column("location_lng", Float),  # Longitude coordinate
    Column("locality", String(200)),
    Column("kingdom", String(100)),
    Column("fishing_region", String(100)),
    Column("depth_range", String(50)),
    Column("lifespan_years", Float),
    Column("migration_patterns", String(200)),
    Column("synonyms", ARRAY(String)),  # multiple synonyms
    Column("reproductive_type", Enum("oviparous", "viviparous", "ovoviviparous", name="reproductive_type_enum"),nullable=True,default=None),
    Column("habitat_type", Enum("freshwater", "marine", "brackish", "estuarine", "benthic", "planktonic", "nektonic", "demersal", "mesopelagic", name="habitat_type_enum"),nullable=True,default=None),
    Column("phylum", String(100)),
    Column("diet_type", Enum("carnivore", "herbivore", "omnivore", "planktivore", "detritivore", name="diet_type_enum"),nullable=True,default=None),
)

# Oceanography table with location as geography(Point)
oceanography = Table(
    "oceanography", metadata,
    Column("data_set", String(100)),
    Column("version", String(50)),
    Column("location", Geography(geometry_type='POINT', srid=4326)),  # 🌍 single column
    Column("location_lat", Float),  # Latitude coordinate
    Column("location_lng", Float),  # Longitude coordinate
    Column("max_depth", Float),
    Column("temperature_kelvin", Float),
    Column("salinity_psu", Float),
    Column("dissolved_oxygen", Float),
    Column("ph", Float),
    Column("chlorophyll_mg_m3", Float),
    Column("nutrients", JSON),
    Column("pressure_bar", Float),
    Column("density_kg_m3", Float),
    Column("turbidity", Float),
    Column("alkalinity", Float),
    Column("surface_currents", Float),
    PrimaryKeyConstraint("data_set", "version")
)






# metadata = MetaData()

EDNA = Table(
    "edna", metadata,
    Column("sequence_id", String, primary_key=True),
    Column("dna_sequence", Text, nullable=False),  
    Column("description", Text),                   
    Column("blast_matching", JSONB),              
    Column("sample_date", Date),                  
    Column("location", Geography(geometry_type='POINT', srid=4326)),  
    Column("location_lat", Float),  # Latitude coordinate
    Column("location_lng", Float),  # Longitude coordinate
    Column("collector", String),                  
    Column("sample_type", String),                # Type of sample: water, sediment, tissue
    Column("species_detected", ARRAY(String)),    # Array of detected species names
    Column("quality_score", Float),              # Quality score
    Column("status", String),                     # Status of the sample
    Column("qr_code_link", String, unique=True),  # QR code identifier
    Column("reference_link", String),            # Reference link
    Column("project", String),                    # Project name
    Column("notes", Text),                        # Additional notes
    
    
)


# Create both tables
metadata.create_all(engine)
# print("✅ 'fish' and 'oceanography' tables created successfully with PostGIS location column!")
