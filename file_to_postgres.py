import os
import pandas as pd
import psycopg2
import PyPDF2
import pdfplumber
import re
import ast
import json
from sqlalchemy import create_engine,text,inspect
from configparser import ConfigParser

# --- Supabase connection details from db.py ---
SUPABASE_DATABASE_URL = "postgresql://postgres.mqtghilvvxpvjnpzodkd:sianfkp63@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"

# --- Table schemas from db.py ---
FISH_COLUMNS = [
    "scientific_name", "species", "class", "family", "location", "locality", "kingdom", "fishing_region", "depth_range", "lifespan_years", "migration_patterns", "synonyms", "reproductive_type", "habitat_type", "phylum", "diet_type"
]
OCEANOGRAPHY_COLUMNS = [
    "data_set", "version", "location", "max_depth", "temperature_kelvin", "salinity_psu", "dissolved_oxygen", "ph", "chlorophyll_mg_m3", "nutrients", "pressure_bar", "density_kg_m3", "turbidity", "alkalinity", "surface_currents"
]
EDNA_COLUMNS = [
    "sequence_id", "dna_sequence", "description", "blast_matching", "sample_date", "location", "collector", "sample_type", "species_detected", "quality_score", "status", "qr_code_link", "reference_link", "project", "notes"
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
        "columns": ["scientific_name", "species", "class", "family", "location", "locality", "kingdom", "fishing_region", "depth_range", "lifespan_years", "migration_patterns", "synonyms", "reproductive_type", "habitat_type", "phylum", "diet_type"]
    },
    "oceanography": {
        "primary_key": ["data_set", "version"],
        "columns": ["data_set", "version", "location", "max_depth", "temperature_kelvin", "salinity_psu", "dissolved_oxygen", "ph", "chlorophyll_mg_m3", "nutrients", "pressure_bar", "density_kg_m3", "turbidity", "alkalinity", "surface_currents"]
    },
    "edna": {
        "primary_key": "sequence_id",
        "columns": ["sequence_id", "dna_sequence", "description", "blast_matching", "sample_date", "location", "collector", "sample_type", "species_detected", "quality_score", "status", "qr_code_link", "reference_link", "project", "notes"]
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
            with pdfplumber.open(file_path) as pdf:
                print(f"PDF has {len(pdf.pages)} pages")
                for page_idx, page in enumerate(pdf.pages):
                    page_tables = page.extract_tables()
                    print(f"Page {page_idx+1}: found {len(page_tables)} tables")
                    for table_idx, table in enumerate(page_tables):
                        if table and len(table) > 1:
                            headers = table[0]
                            data = table[1:]
                            df = pd.DataFrame(data, columns=headers)
                            tables.append(df)
                            print(f"  Table {table_idx+1}: {len(data)} rows, {len(headers)} columns")
                        else:
                            print(f"  Table {table_idx+1}: Empty or invalid table structure")
                
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
                    text += pdf_reader.pages[page_num].extract_text()
                    
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
            
    def map_columns(self, df, table_name):
        """Map dataframe columns to Supabase table columns, using synonyms for primary key and other columns"""
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
                "diet_type": ["diet_type", "Diet Type", "diet", "feeding_type", "Diet"],
                "lifespan_years": ["lifespan_years", "Lifespan (yrs)", "lifespan", "life_expectancy", "age"],
                "depth_range": ["depth_range", "Depth Range", "depth", "water_depth"],
                "reproductive_type": ["reproductive_type", "Reproductive Type", "reproduction", "repro_type", "breeding"],
                "habitat_type": ["habitat_type", "Habitat Type", "habitat", "environment", "ecosystem"]
            },
            "oceanography": {
                "data_set": ["data_set", "dataset", "data_set_id", "data collection", "Data Set"],
                "version": ["version", "ver", "v", "Version", "revision"],
            },
            "edna": {
                "sequence_id": ["sequence_id", "seq_id", "sequence", "id", "dna_id", "Sequence ID"],
                "dna_sequence": ["dna_sequence", "sequence", "dna", "DNA Sequence", "genetic_sequence"],
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

    def fix_special_columns(self, df, table_name):
        """Convert array, enum, and JSON columns to correct types for Supabase"""
        import json
        import ast
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
    def fix_special_columns(self, df, table_name):
        """Convert array, enum, and JSON columns to correct types for Supabase"""
        import json
        import ast
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
                    return ["freshwater", "marine", "brackish", "estuarine"]
                
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
                    
                # Check if values are valid
                invalid_mask = ~df[col].isin(valid_values)
                invalid_values = df.loc[invalid_mask, col].unique()
                
                if len(invalid_values) > 0:
                    print(f"Invalid {col} values found: {invalid_values}")
                    print(f"Valid values are: {valid_values}")
                    # Remove invalid rows
                    df = df[~invalid_mask]
                
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


