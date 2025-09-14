from sqlalchemy import (
    create_engine, Column, String, Float, JSON,
    Enum, MetaData, Table, PrimaryKeyConstraint,ForeignKeyConstraint,Text,Date,UniqueConstraint,Identity,Integer,TIMESTAMP,text,Boolean,ForeignKey,Index
    , CheckConstraint
)
from sqlalchemy.dialects.postgresql import ARRAY,JSONB
from geoalchemy2 import Geography

DATABASE_URL = "postgresql://postgres.mqtghilvvxpvjnpzodkd:sianfkp63@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Fish table with location as geography(Point)
fish = Table(
    "fish", metadata,
    Column("id", Integer, Identity(start=1, cycle=True), primary_key=True),
    Column("scientificName", String(150), unique=True,nullable=False),
    Column("species", String(100)),
    Column("class", String(100)),
    Column("family", String(100)),
    Column("location", Geography(geometry_type='POINT', srid=4326)),  # 🌍 single column
    Column("decimalLatitude", Float),  # Latitude coordinate
    Column("decimalLongitude", Float),  # Longitude coordinate
    Column("locality", String(200)),
    Column("kingdom", String(100)),
    Column("fishing_region", String(100)),
    Column("maximumDepthInMeters", String(50)),
    Column("lifespan_years", Float,CheckConstraint("lifespan_years > 0")),
    Column("migration_patterns", String(200)),
    Column("synonyms", ARRAY(String)),  # multiple synonyms
    Column("reproductive_type", Enum("oviparous", "viviparous", "ovoviviparous", name="reproductive_type_enum"),nullable=True,default=None),
    Column("habitat", Enum("freshwater", "marine", "brackish", "estuarine", "benthic", "planktonic", "nektonic", "demersal", "mesopelagic", name="habitat_type_enum"),nullable=True,default=None),
    Column("phylum", String(100)),
    Column("diet_type", Enum("carnivore", "herbivore", "omnivore", "planktivore", "detritivore", name="diet_type_enum"),nullable=True,default=None),
    Column("eventDate", TIMESTAMP(timezone=True), nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text('now()'), nullable=False),
    Column("data_source_id", Integer, ForeignKey("data_sources.id"), nullable=False)
)

data_sources = Table(
    "data_sources", metadata,
    Column("id", Integer, Identity(start=1, cycle=True), primary_key=True),
    Column("name", String(200), unique=True, nullable=False), # e.g., "NOAA Buoy 44013" or "Smith et al. 2022"
    Column("description", Text),
    Column("source_type", Enum("Journal Article", "Sensor Feed", "Field Observation", "Database", "Government Report", name="source_type_enum"),default="None"),
    Column("url", String), # A link to the dataset or article
    Column("citation", Text), # e.g., a DOI or full citation
    Column("created_at", TIMESTAMP(timezone=True), server_default=text('now()'), nullable=False)
)

# Oceanography table with location as geography(Point)
oceanography = Table(
    "oceanography", metadata,
    Column("data_set", String(100)),
    Column("version", String(50)),
    Column("location", Geography(geometry_type='POINT', srid=4326)),  # 🌍 single column
    Column("decimalLatitude", Float),  # Latitude coordinate
    Column("decimalLongitude", Float),  # Longitude coordinate
    Column("maximumDepthInMeters", Float),
    Column("waterTemperature", Float,CheckConstraint('"waterTemperature" > 0')),
    Column("salinity", Float,CheckConstraint("salinity > 0 AND salinity <= 40")),
    Column("dissolvedOxygen", Float, CheckConstraint('"dissolvedOxygen" > 0')),
    Column("water_pH", Float,CheckConstraint('"water_pH" BETWEEN 0 AND 14')),
    Column("chlorophyll_mg_m3", Float,CheckConstraint("chlorophyll_mg_m3 > 0")),
    Column("nutrients", JSONB),
    Column("pressure_bar", Float),
    Column("density_kg_m3", Float),
    Column("turbidity", Float),
    Column("alkalinity", Float),
    Column("surface_currents", Float),
    Column("measurement_date", TIMESTAMP(timezone=True),nullable=True),
    Column("created_at", TIMESTAMP(timezone=True), server_default=text('now()'), nullable=False),
    Column("data_source_id", Integer, ForeignKey("data_sources.id"), nullable=False),
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

# Index('idx_fish_location', fish.c.location, postgresql_using='gist')  # GIST index for the 'location' column in the fish table
# Index('idx_oceanography_location', oceanography.c.location, postgresql_using='gist')  # GIST index for the 'location' column in the oceanography table
# Index('idx_edna_location', EDNA.c.location, postgresql_using='gist')  # GIST index for the 'location' column in the eDNA table

# Add additional indexes for commonly queried fields
# Index('idx_fish_scientificName', fish.c.scientificName)  # Index for scientificName in the fish table
# Index('idx_oceanography_measurement_date', oceanography.c.measurement_date)  # Index for measurement_date in the oceanography table
# Index('idx_edna_sample_date', EDNA.c.sample_date)  # Index for sample_date in the eDNA table
# Create both tables
# metadata.drop_all(engine)
metadata.create_all(engine, checkfirst=True)
print("✅ 'fish' and 'oceanography' tables created successfully with PostGIS location column!")
