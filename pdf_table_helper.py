import re
import pandas as pd
from file_to_postgres import best_table_match

def determine_pdf_table_type(table_df, schemas, pk_synonyms):
   
    if table_df is None or table_df.empty:
        return None
        
    # Get the column names from the DataFrame
    original_columns = list(table_df.columns)
    
    # Try direct match first using the existing best_table_match function
    table_type = best_table_match(original_columns, schemas, pk_synonyms)
    if table_type:
        return table_type
        
    # If no direct match, try normalizing column names
    clean_columns = []
    for col in original_columns:
        # Convert to lowercase and remove special characters
        clean_col = re.sub(r'[^a-zA-Z0-9]', '_', str(col).lower())
        clean_col = re.sub(r'_+', '_', clean_col).strip('_')  # Replace multiple underscores with one
        clean_columns.append(clean_col)
    
    # Try match with normalized columns
    table_type = best_table_match(clean_columns, schemas, pk_synonyms)
    if table_type:
        return table_type
    
    # Enhanced column mapping with more variations for all file types
    column_mapping = {
        # Fish table mappings
        'scientific_name': ['scientific name', 'sci name', 'species name', 'fish name', 'scientific_name', 
                           'scientificname', 'sci_name', 'organism', 'fish species', 'name (scientific)', 
                           'latin name', 'taxon', 'taxonomy'],
        'species': ['species', 'common name', 'commonname', 'fish species', 'common_name', 'organism', 
                   'vernacular name', 'species name'],
        'class': ['class', 'classification', 'taxon class', 'taxonomic class', 'class_name'],
        'family': ['family', 'fish family', 'taxonomic family', 'family_name', 'fam'],
        'location': ['location', 'coordinates', 'position', 'geo location', 'geographical location', 
                     'loc', 'site', 'place', 'collection site'],
        'location_lat': ['location_lat', 'lat', 'latitude', 'y', 'coord_y', 'geo_lat', 'lat_coord'],
        'location_lng': ['location_lng', 'lng', 'lon', 'longitude', 'x', 'coord_x', 'geo_lng', 'lng_coord', 'long'],
        'locality': ['locality', 'area', 'region', 'zone', 'collection area'],
        'kingdom': ['kingdom', 'taxonomic kingdom', 'kingdom_name'],
        'fishing_region': ['fishing region', 'fishing area', 'fishing zone', 'fishing_region', 
                          'fishery area', 'fishery', 'fishing grounds'],
        'depth_range': ['depth range', 'depth', 'range', 'depth_range', 'water depth', 'bathymetry', 
                        'min depth', 'max depth', 'depth (m)'],
        'lifespan_years': ['lifespan', 'lifespan years', 'lifespan (yrs)', 'life expectancy', 'lifespan_years',
                          'years', 'age', 'longevity', 'lifespan in years'],
        'migration_patterns': ['migration patterns', 'migration', 'patterns', 'migration_patterns', 
                              'migratory behavior', 'movement pattern', 'seasonal movement'],
        'synonyms': ['synonyms', 'other names', 'alt names', 'alternative names', 'syn', 'synonym',
                    'alias', 'known as', 'also called'],
        'reproductive_type': ['reproductive type', 'reproduction', 'reproductive_type', 'repro type',
                             'breeding type', 'reproduction mode', 'reproduction method'],
        'habitat_type': ['habitat type', 'habitat', 'habitat_type', 'environment', 'living environment',
                        'ecosystem', 'biome', 'biotope'],
        'phylum': ['phylum', 'taxonomic phylum', 'phylum_name', 'division'],
        'diet_type': ['diet type', 'diet', 'feeding', 'diet_type', 'food', 'nutrition', 'eating habits',
                     'feeding strategy', 'trophic type']
    }
    
    # Extended oceanography mappings
    oceanography_mappings = {
        'data_set': ['data set', 'dataset', 'data_set', 'data collection', 'data series', 'survey', 'cruise', 
                    'expedition', 'campaign', 'project'],
        'version': ['version', 'ver', 'v', 'revision', 'release', 'update'],
        'location': ['location', 'coordinates', 'position', 'geo location', 'geographical location', 
                     'loc', 'site', 'place', 'collection site'],
        'location_lat': ['location_lat', 'lat', 'latitude', 'y', 'coord_y', 'geo_lat', 'lat_coord'],
        'location_lng': ['location_lng', 'lng', 'lon', 'longitude', 'x', 'coord_x', 'geo_lng', 'lng_coord', 'long'],
        'temperature_kelvin': ['temperature', 'temp', 'temperature (k)', 'kelvin', 'temperature_k', 
                              'water temperature', 'sea temperature'],
        'salinity_psu': ['salinity', 'salt', 'psu', 'salinity_psu', 'salt content', 'practical salinity',
                        'salinity (psu)'],
        'chlorophyll_mg_m3': ['chlorophyll', 'chl', 'chl-a', 'chlorophyll a', 'chlorophyll_a',
                             'chlorophyll concentration', 'phytoplankton', 'chl mg/m3']
    }
    
    # Extended eDNA mappings
    edna_mappings = {
        'sequence_id': ['sequence id', 'sequence', 'seq id', 'seqid', 'sequence_id', 'dna id', 
                       'sequence identifier', 'accession', 'accession number'],
        'dna_sequence': ['dna sequence', 'sequence', 'dna', 'nucleotides', 'bases', 'genetic sequence',
                        'genomic sequence', 'amplicon'],
        'location': ['location', 'coordinates', 'position', 'geo location', 'geographical location', 
                     'loc', 'site', 'place', 'collection site'],
        'location_lat': ['location_lat', 'lat', 'latitude', 'y', 'coord_y', 'geo_lat', 'lat_coord'],
        'location_lng': ['location_lng', 'lng', 'lon', 'longitude', 'x', 'coord_x', 'geo_lng', 'lng_coord', 'long'],
        'blast_matching': ['blast', 'blast match', 'blast result', 'blast hit', 'sequence similarity',
                          'sequence match', 'database match'],
        'sample_date': ['sample date', 'date', 'collection date', 'sampling date', 'date collected',
                       'date of collection', 'timestamp']
    }
    
    # Try to identify the table type based on key column presence with weighted scoring
    fish_score = 0
    oceanography_score = 0
    edna_score = 0
    
    # Data content analysis for more accurate classification
    # Sample first few rows to check content patterns (if data exists)
    try:
        data_sample = table_df.iloc[:5].astype(str)
    except:
        data_sample = pd.DataFrame()
    
    # Check for fish table indicators with weighted scoring
    fish_indicators = {
        'scientific name': 3, 'species': 2, 'fish': 2, 'class': 1, 'family': 2, 
        'diet': 2, 'habitat': 2, 'phylum': 3, 'kingdom': 2, 'taxonomy': 2
    }
    
    # Check for oceanography indicators with weighted scoring
    ocean_indicators = {
        'data set': 3, 'dataset': 3, 'temperature': 2, 'salinity': 3, 'depth': 1, 
        'pressure': 2, 'kelvin': 3, 'psu': 3, 'oxygen': 2, 'pH': 2, 'chlorophyll': 3
    }
    
    # Check for eDNA indicators with weighted scoring
    edna_indicators = {
        'sequence': 3, 'dna': 3, 'blast': 3, 'sample': 1, 'collector': 2,
        'genomic': 2, 'genetic': 2, 'pcr': 3, 'amplicon': 3, 'nucleotide': 3
    }
    
    # Check column names with weighted scoring
    for col in original_columns:
        col_lower = col.lower()
        
        # Fish indicators
        for indicator, weight in fish_indicators.items():
            if indicator in col_lower:
                fish_score += weight
        
        # Oceanography indicators
        for indicator, weight in ocean_indicators.items():
            if indicator in col_lower:
                oceanography_score += weight
        
        # eDNA indicators
        for indicator, weight in edna_indicators.items():
            if indicator in col_lower:
                edna_score += weight
    
    # Check data content for specific patterns (using the sample)
    if not data_sample.empty:
        # Look for DNA sequence patterns (ATCG)
        dna_pattern = re.compile(r'[ATCG]{10,}', re.IGNORECASE)
        for col in data_sample.columns:
            sample_text = ' '.join(data_sample[col].astype(str).tolist())
            if dna_pattern.search(sample_text):
                edna_score += 5  # Strong indicator of eDNA data
        
        # Look for numerical patterns typical in oceanography
        numeric_cols = 0
        for col in data_sample.columns:
            try:
                # Check if column can be converted to float
                pd.to_numeric(table_df[col].head())
                numeric_cols += 1
            except:
                pass
        
        # If more than 50% of columns are numeric, likely oceanography
        if numeric_cols > len(table_df.columns) * 0.5:
            oceanography_score += 3
            
        # Look for taxonomic naming patterns (Genus species)
        taxon_pattern = re.compile(r'\b[A-Z][a-z]+ [a-z]+\b')
        for col in data_sample.columns:
            sample_text = ' '.join(data_sample[col].astype(str).tolist())
            if taxon_pattern.search(sample_text):
                fish_score += 4  # Strong indicator of taxonomic/fish data
    
    # Determine the table type based on scores
    if fish_score > oceanography_score and fish_score > edna_score and fish_score >= 2:
        return 'fish'
    elif oceanography_score > fish_score and oceanography_score > edna_score and oceanography_score >= 2:
        return 'oceanography'
    elif edna_score > fish_score and edna_score > oceanography_score and edna_score >= 2:
        return 'edna'
    
    # If we got here, we couldn't determine the type
    return None
