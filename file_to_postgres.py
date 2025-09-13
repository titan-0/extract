import os
import pandas as pd
import PyPDF2
import pdfplumber
import re
from sqlalchemy import create_engine,text,inspect
from configparser import ConfigParser

# --- Supabase connection details from db.py ---
SUPABASE_DATABASE_URL = "postgresql://postgres.mqtghilvvxpvjnpzodkd:sianfkp63@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

# --- Table schemas from db.py ---
FISH_COLUMNS = [
    "scientific_name", "species", "class", "family", "location", "location_lat", "location_lng", "locality", "kingdom", "fishing_region", "depth_range", "lifespan_years", "migration_patterns", "synonyms", "reproductive_type", "habitat_type", "phylum", "diet_type"
]
OCEANOGRAPHY_COLUMNS = [
    "data_set", "version", "location", "location_lat", "location_lng", "max_depth", "temperature_kelvin", "salinity_psu", "dissolved_oxygen", "ph", "chlorophyll_mg_m3", "nutrients", "pressure_bar", "density_kg_m3", "turbidity", "alkalinity", "surface_currents"
]
EDNA_COLUMNS = [
    "sequence_id", "dna_sequence", "description", "blast_matching", "sample_date", "location", "location_lat", "location_lng", "collector", "sample_type", "species_detected", "quality_score", "status", "qr_code_link", "reference_link", "project", "notes"
]

# Helper function to determine which table a dataset belongs to
def best_table_match(columns, schemas, pk_synonyms):
    scores = {}
    for table, schema in schemas.items():
        pk_syns = pk_synonyms.get(table, [])
        pk_found = any(col.lower() in [syn.lower() for syn in pk_syns] for col in columns)
        other_matches = len(set([c.lower() for c in columns]) & set([c.lower() for c in schema["columns"]]))
        scores[table] = (pk_found, other_matches)
    # Prefer tables where pk_found is True and most other columns match
    best = max(scores.items(), key=lambda x: (x[1][0], x[1][1]))
    return best[0] if best[1][0] else None

PRIMARY_KEY_SYNONYMS = {
    "fish": ["scientific_name", "sci_name", "species_name", "fish_name","scientificName"],
    "oceanography": ["data_set", "dataset", "data_set_id", "version", "ver", "data_version"],
    "edna": ["sequence_id", "seq_id", "sequence", "id", "dna_id"]
}

schemas = {
    "fish": {
        "primary_key": "scientific_name",
        "columns": ["scientific_name", "species", "class", "family", "location", "location_lat", "location_lng", "locality", "kingdom", "fishing_region", "depth_range", "lifespan_years", "migration_patterns", "synonyms", "reproductive_type", "habitat_type", "phylum", "diet_type"]
    },
    "oceanography": {
        "primary_key": ["data_set", "version"],
        "columns": ["data_set", "version", "location", "location_lat", "location_lng", "max_depth", "temperature_kelvin", "salinity_psu", "dissolved_oxygen", "ph", "chlorophyll_mg_m3", "nutrients", "pressure_bar", "density_kg_m3", "turbidity", "alkalinity", "surface_currents"]
    },
    "edna": {
        "primary_key": "sequence_id",
        "columns": ["sequence_id", "dna_sequence", "description", "blast_matching", "sample_date", "location", "location_lat", "location_lng", "collector", "sample_type", "species_detected", "quality_score", "status", "qr_code_link", "reference_link", "project", "notes"]
    }
}

class DataImporter:
    def __init__(self, database_url=SUPABASE_DATABASE_URL):
        self.database_url = database_url
        self.conn = None
        self.engine = None
        self.connect_to_db()

    def connect_to_db(self):
        """Connect to Supabase PostgreSQL database"""
        try:
            self.engine = create_engine(self.database_url)
            self.conn = self.engine.raw_connection()
            print("Connected to Supabase database successfully")
        except Exception as error:
            print(f"Error connecting to Supabase: {error}")

    
    # Method moved outside class


    def read_file(self, file_path):
        """Read data from file based on extension"""
        _, extension = os.path.splitext(file_path)
        extension = extension.lower()
        
        if extension == '.csv':
            return self.read_csv(file_path)
        elif extension in ['.xls', '.xlsx']:
            return self.read_excel(file_path)
        elif extension == '.txt':
            return self.read_txt(file_path)
        else:
            raise ValueError(f"Unsupported file format: {extension}")
      
    def read_txt(self, file_path):
        """Read TXT file as table (tab or comma delimited)"""
        # Try tab first, then comma
        try:
            df = pd.read_csv(file_path, sep='\t')
            if df.shape[1] == 1:
                # Only one column, try comma
                df = pd.read_csv(file_path, sep=',')
            return df
        except Exception as e:
            print(f"TXT extraction failed: {e}")
            return pd.DataFrame()
            
    def read_csv(self, file_path):
        """Read CSV file"""
        return pd.read_csv(file_path)
        
    def read_excel(self, file_path):
        """Read Excel file"""
        return pd.read_excel(file_path)
        
    def read_pdf(self, file_path):
        """Extract tables from PDF file - returns a list of DataFrames"""
        
        try:
            tables = []
            all_text = ""
            table_regions = []
            
            with pdfplumber.open(file_path) as pdf:
                print(f"PDF has {len(pdf.pages)} pages")
                for page_idx, page in enumerate(pdf.pages):
                    # Extract all text from the page
                    page_text = page.extract_text()
                    if page_text:
                        all_text += f"\n--- Page {page_idx+1} ---\n{page_text}\n"
                    
                    page_tables = page.extract_tables()
                    print(f"Page {page_idx+1}: found {len(page_tables)} tables")
                    for table_idx, table in enumerate(page_tables):
                        if table and len(table) > 1:
                            headers = table[0]
                            data = table[1:]
                            df = pd.DataFrame(data, columns=headers)
                            tables.append(df)
                            print(f"  Table {table_idx+1}: {len(data)} rows, {len(headers)} columns")
                            
                            # Track table content for removal from free text
                            table_text = '\n'.join(['\t'.join(str(cell) if cell else '' for cell in row) for row in table])
                            table_regions.append(table_text)
                        else:
                            print(f"  Table {table_idx+1}: Empty or invalid table structure")
                
                # Extract and save non-table text
                if all_text.strip():
                    non_table_text = self._extract_non_table_text(all_text, table_regions)
                    self._save_extracted_text(file_path, non_table_text)
                
                if tables:
                    print(f"Extracted {len(tables)} tables from PDF")
                    return tables
                else:
                    print("No valid tables found in PDF.")
            
        except Exception as e:
            print(f"pdfplumber extraction failed: {e}")
            
        # Fallback to PyPDF2 for text extraction and manual parsing
        try:
            print("Falling back to PyPDF2 text extraction...")
            text = ""
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page_num in range(len(pdf_reader.pages)):
                    page_text = pdf_reader.pages[page_num].extract_text()
                    text += f"\n--- Page {page_num+1} ---\n{page_text}\n"
            
            # Save extracted text even if no tables found
            if text.strip():
                self._save_extracted_text(file_path, text)
                    
            # Very basic parsing - this would need customization for your PDF format
            lines = text.split('\n')
            data = []
            for line in lines:
                # Split by common delimiters and clean
                row = re.split(r'[,|\t]', line.strip())
                if row and len(row) > 1:  # Skip lines that don't seem like data
                    data.append(row)
                    
            if data:
                # Try to identify header row or use first row as header
                headers = data[0]
                df = pd.DataFrame(data[1:], columns=headers)
                return [df]  # Return as a list with one DataFrame to be consistent
            return []  # Empty list when no tables found
        except Exception as e:
            print(f"PDF extraction failed: {e}")
            return []  # Empty list when extraction fails

    def _extract_non_table_text(self, full_text, table_regions):
        """Extract non-table text by removing table content"""
        non_table_text = full_text
        
        # Remove table content from the full text (basic approach)
        for table_text in table_regions:
            lines_to_remove = table_text.split('\n')
            for line in lines_to_remove:
                if line.strip():
                    non_table_text = non_table_text.replace(line, '')
        
        # Advanced table pattern removal
        non_table_text = self._remove_table_patterns(non_table_text)
        
        # Clean up the text
        return self._clean_extracted_text(non_table_text)

    def _remove_table_patterns(self, text):
        """Remove table-like patterns from text"""
        import re
        
        lines = text.split('\n')
        filtered_lines = []
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Skip lines that look like table headers
            if re.match(r'^(Scientific Name|Species|Class|Family|Locality|Kingdom|Fishing Region|Depth Range|Lifespan|Diet Type)', line, re.IGNORECASE):
                continue
            
            # Skip lines with structured fish data patterns
            if re.match(r'^Fish_\d+_\d+\s+Species_\d+\s+Class_\d+', line):
                continue
            
            # Skip lines with multiple tab-separated scientific values
            parts = line.split()
            if len(parts) >= 5:
                # Check if it matches fish data pattern (Fish_X_Y Species_X Class_X Family_X...)
                if (len(parts) >= 8 and 
                    parts[0].startswith('Fish_') and 
                    parts[1].startswith('Species_') and 
                    parts[2].startswith('Class_')):
                    continue
                
                # Check if line has multiple "Animalia" or similar taxonomic terms
                if parts.count('Animalia') > 0 and any(part.startswith(('Region_', 'Locality_', 'Family_')) for part in parts):
                    continue
                
                # Check if line has depth measurements and diet types
                has_depth = any(part.endswith('m') and part[:-1].replace('.', '').isdigit() for part in parts)
                has_diet = any(part in ['Carnivore', 'Herbivore', 'Omnivore', 'Planktivore'] for part in parts)
                if has_depth and has_diet:
                    continue
            
            # Skip lines that are mostly repetitive keywords
            words = line.split()
            if len(words) > 5:
                unique_words = set(words)
                # If more than 70% of words are repeated, it's likely table-related filler text
                if len(unique_words) / len(words) < 0.3:
                    # Check if these are table-related keywords
                    table_keywords = {'fish', 'species', 'habitat', 'ocean', 'sea', 'river', 'lake', 'marine', 'freshwater', 'migration', 'ecosystem'}
                    if len(unique_words.intersection(table_keywords)) / len(unique_words) > 0.5:
                        continue
            
            # Keep lines that appear to be descriptive text
            filtered_lines.append(line)
        
        return '\n'.join(filtered_lines)

    def _clean_extracted_text(self, text):
        """Clean and format extracted text"""
        import re
        
        # Remove excessive whitespace and empty lines
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            if line and line not in cleaned_lines:  # Remove duplicates
                cleaned_lines.append(line)
        
        # Join lines and clean up spacing
        cleaned_text = '\n'.join(cleaned_lines)
        
        # Remove multiple consecutive newlines
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)
        
        return cleaned_text

    def _save_extracted_text(self, pdf_path, extracted_text):
        """Save extracted text to a file"""
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        
        # Create extracted_text folder if it doesn't exist
        extracted_text_folder = "extracted_text"
        if not os.path.exists(extracted_text_folder):
            os.makedirs(extracted_text_folder)
        
        # Use original PDF name for the text file
        text_file_path = os.path.join(extracted_text_folder, f"{base_name}.txt")
        
        try:
            with open(text_file_path, 'w', encoding='utf-8') as f:
                f.write(f"Extracted text from: {pdf_path}\n")
                f.write("=" * 50 + "\n\n")
                f.write(extracted_text)
            
            print(f"📝 Saved extracted text to: {text_file_path}")
            
        except Exception as e:
            print(f"Error saving extracted text: {e}")
            
    def detect_and_fix_data_misalignment(self, df, table_name):
        """Detect if data is misaligned with column headers and attempt to fix it"""
        if df.empty or table_name != "fish":  # Focus on fish table for now
            return df
            
        print(f"Checking data alignment for {table_name} table...")
        
        # Expected data patterns for fish table
        expected_patterns = {
            'scientific_name': lambda x: isinstance(x, str) and len(str(x)) > 2,
            'species': lambda x: isinstance(x, str) and len(str(x)) > 2,
            'class': lambda x: isinstance(x, str) and len(str(x)) > 2,
            'family': lambda x: isinstance(x, str) and len(str(x)) > 2,
            'location_lat': lambda x: isinstance(x, (int, float)) and -90 <= x <= 90,
            'location_lng': lambda x: isinstance(x, (int, float)) and -180 <= x <= 180,
            'locality': lambda x: isinstance(x, str) and len(str(x)) > 1,
            'kingdom': lambda x: isinstance(x, str) and str(x).lower() in ['animalia', 'animal'],
            'fishing_region': lambda x: isinstance(x, str),
            'depth_range': lambda x: isinstance(x, str) or isinstance(x, (int, float)),
            'lifespan_years': lambda x: isinstance(x, (int, float)) and 0 < x < 200,
            'migration_patterns': lambda x: isinstance(x, str),
        }
        
        # Check first row for obvious misalignments
        if len(df) > 0:
            first_row = df.iloc[0]
            misalignments = []
            
            for col, value in first_row.items():
                if col in expected_patterns:
                    if not expected_patterns[col](value):
                        misalignments.append((col, value, type(value).__name__))
            
            if misalignments:
                print(f"Detected potential data misalignments:")
                for col, val, val_type in misalignments:
                    print(f"  {col}: {val} ({val_type}) - doesn't match expected pattern")
                
                # Try to auto-correct based on the known correct order
                # Based on the error, the correct order should be:
                correct_order = [
                    'scientific_name', 'species', 'class', 'family', 'location_lat', 
                    'location_lng', 'locality', 'kingdom', 'fishing_region', 
                    'depth_range', 'lifespan_years', 'migration_patterns', 'synonyms',
                    'reproductive_type', 'habitat_type', 'phylum', 'diet_type'
                ]
                
                current_columns = list(df.columns)
                
                # If we have the same number of columns, try remapping
                if len(current_columns) == len(correct_order):
                    print("Attempting to fix column alignment...")
                    
                    # Create a new dataframe with corrected column mapping
                    corrected_df = pd.DataFrame()
                    for i, correct_col in enumerate(correct_order):
                        if i < len(current_columns):
                            corrected_df[correct_col] = df.iloc[:, i]  # Use positional mapping
                    
                    print("Data alignment corrected!")
                    return corrected_df
        
        return df

    def map_columns(self, df, table_name):
        """Map dataframe columns to Supabase table columns, using synonyms for primary key and other columns"""
        
        # First attempt to detect and fix data misalignment
        df = self.detect_and_fix_data_misalignment(df, table_name)
        
        # Use schemas from db.py
        table_columns = {
            "fish": FISH_COLUMNS,
            "oceanography": OCEANOGRAPHY_COLUMNS,
            "edna": EDNA_COLUMNS
        }.get(table_name, [])
        if not table_columns:
            print(f"Unknown table: {table_name}")
            return None
            
        # Extended column synonyms for all tables
        COLUMN_SYNONYMS = {
            "fish": {
                "scientific_name": ["scientific_name", "sci_name", "species_name", "fish_name", "scientificName", "Scientific Name"],
                "species": ["species", "common_name", "Species", "common name", "Common Name"],
                "class": ["class", "Class", "classification"],
                "family": ["family", "Family", "family_name"],
                "location": ["location", "coordinates", "position", "geo_location", "geographical_location", "loc", "site", "place"],
                "location_lat": ["location_lat", "lat", "latitude", "y", "coord_y", "geo_lat", "lat_coord","decimalLatitude","decimalLongitude"],
                "location_lng": ["location_lng", "lng", "lon", "longitude", "x", "coord_x", "geo_lng", "lng_coord", "long"],
                "diet_type": ["diet_type", "Diet Type", "diet", "feeding_type", "Diet"],
                "lifespan_years": ["lifespan_years", "Lifespan (yrs)", "lifespan", "life_expectancy", "age"],
                "depth_range": ["depth_range", "Depth Range", "depth", "water_depth","maximumDepthInMeters"],
                "reproductive_type": ["reproductive_type", "Reproductive Type", "reproduction", "repro_type", "breeding"],
                "habitat_type": ["habitat_type", "Habitat Type", "habitat", "environment", "ecosystem"]
            },
            "oceanography": {
                "data_set": ["data_set", "dataset", "data_set_id", "data collection", "Data Set", "survey", "cruise", "expedition", "campaign", "project", "study"],
                "version": ["version", "ver", "v", "Version", "revision", "release", "update", "data_version"],
                "location": ["location", "coordinates", "position", "geo_location", "geographical_location", "loc", "site", "place", "station", "sampling_site"],
                "location_lat": ["location_lat", "lat", "latitude", "y", "coord_y", "geo_lat", "lat_coord", "station_lat"],
                "location_lng": ["location_lng", "lng", "lon", "longitude", "x", "coord_x", "geo_lng", "lng_coord", "long", "station_lng"],
                "max_depth": ["max_depth", "maximum_depth", "depth_max", "bottom_depth", "water_depth", "depth", "bathymetry", "depth_m"],
                "temperature_kelvin": ["temperature_kelvin", "temperature", "temp", "temp_k", "kelvin", "water_temp", "sea_temp", "temperature_k"],
                "salinity_psu": ["salinity_psu", "salinity", "sal", "psu", "salt", "salt_content", "practical_salinity", "salinity_practical"],
                "dissolved_oxygen": ["dissolved_oxygen", "oxygen", "o2", "do", "dissolved_o2", "oxygen_content", "oxygen_mg_l", "oxygen_concentration"],
                "ph": ["ph", "pH", "acidity", "hydrogen_ion", "ph_level", "ph_value"],
                "chlorophyll_mg_m3": ["chlorophyll_mg_m3", "chlorophyll", "chl", "chl_a", "chlorophyll_a", "phytoplankton", "chl_mg_m3", "chlorophyll_concentration"],
                "nutrients": ["nutrients", "nutrition", "nutrient_content", "mineral_content", "chemical_composition", "nutrients_json"],
                "pressure_bar": ["pressure_bar", "pressure", "press", "bar", "water_pressure", "hydrostatic_pressure", "pressure_dbar"],
                "density_kg_m3": ["density_kg_m3", "density", "water_density", "seawater_density", "rho", "density_kg", "specific_gravity"],
                "turbidity": ["turbidity", "cloudiness", "clarity", "suspended_particles", "turbidity_ntu", "water_clarity"],
                "alkalinity": ["alkalinity", "alk", "total_alkalinity", "carbonate_alkalinity", "buffering_capacity"],
                "surface_currents": ["surface_currents", "currents", "current_speed", "flow", "water_flow", "current_velocity", "surface_flow"]
            },
            "edna": {
                "sequence_id": ["sequence_id", "seq_id", "sequence", "id", "dna_id", "Sequence ID", "accession", "accession_number", "sequence_identifier"],
                "dna_sequence": ["dna_sequence", "sequence", "dna", "DNA Sequence", "genetic_sequence", "nucleotides", "bases", "genomic_sequence", "amplicon"],
                "description": ["description", "desc", "sample_description", "sequence_description", "annotation", "comments", "details"],
                "blast_matching": ["blast_matching", "blast", "blast_match", "blast_result", "blast_hit", "sequence_similarity", "database_match", "homology"],
                "sample_date": ["sample_date", "date", "collection_date", "sampling_date", "date_collected", "date_of_collection", "timestamp", "collection_time"],
                "location": ["location", "coordinates", "position", "geo_location", "geographical_location", "loc", "site", "place", "sampling_site", "collection_site"],
                "location_lat": ["location_lat", "lat", "latitude", "y", "coord_y", "geo_lat", "lat_coord", "sampling_lat"],
                "location_lng": ["location_lng", "lng", "lon", "longitude", "x", "coord_x", "geo_lng", "lng_coord", "long", "sampling_lng"],
                "collector": ["collector", "collected_by", "sampler", "researcher", "scientist", "field_collector", "person"],
                "sample_type": ["sample_type", "type", "sample_category", "material", "substrate", "medium", "sample_kind"],
                "species_detected": ["species_detected", "species", "organisms", "taxa", "detected_species", "identified_species", "species_list"],
                "quality_score": ["quality_score", "quality", "score", "q_score", "confidence", "reliability", "quality_rating"],
                "status": ["status", "state", "condition", "processing_status", "sample_status", "analysis_status"],
                "qr_code_link": ["qr_code_link", "qr_code", "qr", "barcode", "identifier_code", "sample_code", "tracking_code"],
                "reference_link": ["reference_link", "reference", "ref", "link", "url", "external_link", "source_link"],
                "project": ["project", "study", "research_project", "project_name", "investigation", "program"],
                "notes": ["notes", "remarks", "comments", "observations", "additional_info", "memo", "footnotes"]
            }
        }
        
        # Build mapping: for each schema column, find matching file column by name or synonym
        mapped = {}
        file_cols_lower = {col.lower().replace(' ', '_'): col for col in df.columns}
        
        # First try to map columns using our extended synonyms
        for db_col in table_columns:
            # Get synonyms for this column if available
            synonyms = COLUMN_SYNONYMS.get(table_name, {}).get(db_col, [db_col])
            
            # Try each synonym
            for syn in synonyms:
                syn_lower = syn.lower().replace(' ', '_')
                if syn_lower in file_cols_lower:
                    mapped[db_col] = file_cols_lower[syn_lower]
                    break
            
            # If we didn't find a match with the synonyms, try direct match
            if db_col not in mapped and db_col.lower() in file_cols_lower:
                mapped[db_col] = file_cols_lower[db_col.lower()]
        # Build new DataFrame with only matching columns
        if not mapped:
            print(f"No matching columns for table {table_name}")
            return None
        # Fill missing columns with NaN
        for db_col in table_columns:
            if db_col not in mapped:
                mapped[db_col] = None
        # Build DataFrame with correct columns
        new_df = pd.DataFrame()
        for db_col in table_columns:
            if mapped[db_col] is not None:
                new_df[db_col] = df[mapped[db_col]]
            else:
                new_df[db_col] = pd.NA
        return new_df

    def process_location_columns(self, df):
        """Handle conversions between location point format and separate lat/lng columns"""
        import re
        
        # Check if we have location column with point data but missing lat/lng
        if "location" in df.columns and ("location_lat" not in df.columns or "location_lng" not in df.columns or 
                                        df["location_lat"].isna().all() or df["location_lng"].isna().all()):
            
            def extract_coordinates_from_point(location_str):
                """Extract lat, lng from various point formats"""
                if pd.isna(location_str) or str(location_str).strip() == "":
                    return None, None
                
                location_str = str(location_str).strip()
                
                # Pattern 1: POINT(lng lat) - PostGIS format
                point_match = re.search(r'POINT\s*\(\s*([-+]?\d*\.?\d+)\s+([-+]?\d*\.?\d+)\s*\)', location_str, re.IGNORECASE)
                if point_match:
                    lng, lat = float(point_match.group(1)), float(point_match.group(2))
                    return lat, lng
                
                # Pattern 2: [lat, lng] or (lat, lng)
                bracket_match = re.search(r'[\[\(]\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*[\]\)]', location_str)
                if bracket_match:
                    lat, lng = float(bracket_match.group(1)), float(bracket_match.group(2))
                    return lat, lng
                
                # Pattern 3: "lat,lng" or "lat, lng"
                comma_match = re.search(r'^([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)$', location_str)
                if comma_match:
                    lat, lng = float(comma_match.group(1)), float(comma_match.group(2))
                    return lat, lng
                
                # Pattern 4: "lat lng" (space separated)
                space_match = re.search(r'^([-+]?\d*\.?\d+)\s+([-+]?\d*\.?\d+)$', location_str)
                if space_match:
                    lat, lng = float(space_match.group(1)), float(space_match.group(2))
                    return lat, lng
                
                return None, None
            
            # Extract coordinates from location column
            coords = df["location"].apply(extract_coordinates_from_point)
            
            # Create or update lat/lng columns
            if "location_lat" not in df.columns:
                df["location_lat"] = pd.NA
            if "location_lng" not in df.columns:
                df["location_lng"] = pd.NA
            
            # Fill missing lat/lng values
            for idx, (lat, lng) in enumerate(coords):
                if lat is not None and lng is not None:
                    if pd.isna(df.loc[idx, "location_lat"]):
                        df.loc[idx, "location_lat"] = lat
                    if pd.isna(df.loc[idx, "location_lng"]):
                        df.loc[idx, "location_lng"] = lng
        
        # Check if we have lat/lng but missing location point
        if ("location_lat" in df.columns and "location_lng" in df.columns and 
            not df["location_lat"].isna().all() and not df["location_lng"].isna().all()):
            
            def create_point_from_coords(lat, lng):
                """Create POINT(lng lat) format from coordinates"""
                if pd.isna(lat) or pd.isna(lng):
                    return None
                try:
                    lat_float = float(lat)
                    lng_float = float(lng)
                    # Validate coordinate ranges
                    if -90 <= lat_float <= 90 and -180 <= lng_float <= 180:
                        return f"POINT({lng_float} {lat_float})"
                    else:
                        print(f"Warning: Invalid coordinates lat={lat_float}, lng={lng_float}")
                        return None
                except (ValueError, TypeError):
                    return None
            
            # Create or update location column
            if "location" not in df.columns:
                df["location"] = pd.NA
            
            # Fill missing location values
            for idx in df.index:
                if pd.isna(df.loc[idx, "location"]) or str(df.loc[idx, "location"]).strip() == "":
                    lat = df.loc[idx, "location_lat"]
                    lng = df.loc[idx, "location_lng"]
                    point = create_point_from_coords(lat, lng)
                    if point:
                        df.loc[idx, "location"] = point
        
        # Convert lat/lng to numeric if they exist
        if "location_lat" in df.columns:
            df["location_lat"] = pd.to_numeric(df["location_lat"], errors='coerce')
        if "location_lng" in df.columns:
            df["location_lng"] = pd.to_numeric(df["location_lng"], errors='coerce')

    def fix_special_columns(self, df, table_name):
        """Convert array, enum, and JSON columns to correct types for Supabase"""
        import json
        import ast
        import re
        
        # Handle location conversions for all table types
        self.process_location_columns(df)
        
        if table_name == "fish":
            if "location" in df.columns:
                # If value is NaN or empty, set to None
                df["location"] = df["location"].apply(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)
                
            if "synonyms" in df.columns:
                def to_pg_array(x):
                    if pd.isna(x) or str(x).strip() == "":
                        return None
                    if isinstance(x, list):
                                return "{" + ",".join(map(str, x)) + "}"
                    if isinstance(x, str):
            # Convert semicolon or comma separated string to PG array
                        items = [item.strip() for item in re.split(r"[;,]", x) if item.strip()]
                        return "{" + ",".join(items) + "}"
                    return x
                df["synonyms"] = df["synonyms"].apply(to_pg_array)
            
            # Handle enum columns - convert to lowercase since DB has lowercase values
            # Set default values for missing enum columns
            if "reproductive_type" not in df.columns or df["reproductive_type"].isna().all():
                df["reproductive_type"] = "oviparous"  # Default value (lowercase)
            if "habitat_type" not in df.columns or df["habitat_type"].isna().all():
                df["habitat_type"] = "marine"  # Default value (lowercase)
            
            # Convert enum values to lowercase to match database
            for enum_col in ["reproductive_type", "habitat_type", "diet_type"]:
                if enum_col in df.columns:
                    # Convert to lowercase and clean up
                    df[enum_col] = df[enum_col].apply(lambda x: str(x).strip().lower() if not pd.isna(x) else x)
        elif table_name == "oceanography":
            if "location" in df.columns:
                # If value is NaN or empty, set to None
                df["location"] = df["location"].apply(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)
                
            if "nutrients" in df.columns:
                df["nutrients"] = df["nutrients"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (json.dumps(ast.literal_eval(x)) if isinstance(x, str) and x.startswith("{") else x))
        elif table_name == "edna":
            if "location" in df.columns:
                # If value is NaN or empty, set to None
                df["location"] = df["location"].apply(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)
            if "blast_matching" in df.columns:
                def to_json(x):
                    if pd.isna(x) or str(x).strip() == "":
                        return None
                    if isinstance(x, dict):
                        return json.dumps(x)
                    try:
                        # Convert Python dict-string to JSON string
                        return json.dumps(eval(x)) if isinstance(x, str) and x.startswith("{") else x
                    except Exception:
                        return x  # fallback
                df["blast_matching"] = df["blast_matching"].apply(to_json)
            if "species_detected" in df.columns:
                df["species_detected"] = df["species_detected"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x)
        return df

    def get_enum_values(self, table_name, column_name):
        """Fetch valid enum values from the database for a given column."""
        try:
            query = f"SELECT t.enumlabel FROM pg_type typ JOIN pg_enum t ON t.enumtypid = typ.oid JOIN pg_attribute a ON a.atttypid = typ.oid WHERE a.attrelid = '{table_name}'::regclass AND a.attname = '{column_name}';"
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                values = [row[0] for row in result]
                
                # Return the actual values from database if found
                if values:
                    return values
                
                # If we can't get valid values from database, provide defaults based on actual DB values
                if column_name == "diet_type":
                    return ["carnivore", "herbivore", "omnivore", "planktivore", "detritivore"]
                elif column_name == "reproductive_type":
                    return ["oviparous", "viviparous", "ovoviviparous"]
                elif column_name == "habitat_type":
                    return ["freshwater", "marine", "brackish", "estuarine","benthic", "planktonic", "nektonic", "demersal", "mesopelagic"]
                
                return []
        except Exception as e:
            print(f"Could not fetch enum values for {table_name}.{column_name}: {e}")
            # Provide default values based on actual DB values (lowercase)
            if column_name == "diet_type":
                return ["carnivore", "herbivore", "omnivore", "planktivore", "detritivore"]
            elif column_name == "reproductive_type":
                return ["oviparous", "viviparous", "ovoviviparous"]
            elif column_name == "habitat_type":
                return ["freshwater", "marine", "brackish", "estuarine"]
            return []

    def filter_valid_rows(self, df, table_name):
        """Remove rows with missing required fields and invalid enum values."""
        required_fields = {
            "fish": ["scientific_name"],
            "edna": ["sequence_id", "dna_sequence"],
            "oceanography": ["data_set", "version"]
        }.get(table_name, [])
        # Remove rows with missing required fields
        for col in required_fields:
            if col in df.columns:
                df = df[df[col].notnull()]
        
        # Validate enum columns
        enum_columns = {
            "fish": ["diet_type", "reproductive_type", "habitat_type"],
        }.get(table_name, [])
        
        for col in enum_columns:
            if col in df.columns:
                valid_values = self.get_enum_values(table_name, col)
                if not valid_values:
                    # If we couldn't fetch valid values, don't filter by this column
                    print(f"Warning: Could not fetch valid values for {col}, skipping validation")
                    continue
                    
                # Check if values are valid (excluding null/NA values since columns are nullable)
                non_null_mask = df[col].notna() & (df[col] != '<NA>') & (df[col] != 'nan')
                if non_null_mask.any():
                    non_null_values = df.loc[non_null_mask, col]
                    invalid_mask = ~non_null_values.isin(valid_values)
                    invalid_values = non_null_values[invalid_mask].unique()
                    
                    if len(invalid_values) > 0:
                        print(f"Invalid {col} values found: {invalid_values}")
                        print(f"Valid values are: {valid_values}")
                        # Remove only rows with invalid non-null values
                        full_invalid_mask = non_null_mask & ~df[col].isin(valid_values)
                        df = df[~full_invalid_mask]
                
        return df

    def import_to_db(self, df, table_name, if_exists='append'):
        """Import dataframe to Supabase table"""
        if df is None or df.empty:
            print("No data to import")
            return False
        try:
            mapped_df = self.map_columns(df, table_name)
            if mapped_df is None:
                print(f"Column mapping failed for table '{table_name}'")
                return False
            # Fix array, enum, and JSON columns
            mapped_df = self.fix_special_columns(mapped_df, table_name)
            # Remove rows with missing required fields and invalid enums
            mapped_df = self.filter_valid_rows(mapped_df, table_name)
            if mapped_df.empty:
                print(f"No valid rows to import for table '{table_name}' after filtering.")
                return False
                # Flexible batch upsert for all tables
                # Detect primary key(s) from database
            
            insp = inspect(self.engine)
            pk_cols = insp.get_pk_constraint(table_name)['constrained_columns']
            if not pk_cols:
                print(f"No primary key found for table '{table_name}'. Upsert not possible.")
                return False
            columns = list(mapped_df.columns)
            set_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in pk_cols])
            pk_sql = ', '.join(pk_cols)
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([f':{col}' for col in columns])}) ON CONFLICT ({pk_sql}) DO UPDATE SET {set_clause}"
            values = mapped_df.to_dict(orient='records')
            with self.engine.begin() as conn:
                conn.execute(text(insert_sql), values)
            print(f"Successfully upserted {len(mapped_df)} rows to '{table_name}' on Supabase")
            return True
        except Exception as e:
            print(f"Error importing data to table '{table_name}': {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed")


# https://api.obis.org/v3/occurrence