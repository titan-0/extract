import os
import pandas as pd
import psycopg2
import PyPDF2
import tabula
import re
import ast
import json
from sqlalchemy import create_engine,text
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

    



    def read_file(self, file_path):
        """Read data from file based on extension"""
        _, extension = os.path.splitext(file_path)
        extension = extension.lower()
        
        if extension == '.csv':
            return self.read_csv(file_path)
        elif extension in ['.xls', '.xlsx']:
            return self.read_excel(file_path)
        elif extension == '.pdf':
            return self.read_pdf(file_path)
        else:
            raise ValueError(f"Unsupported file format: {extension}")
            
    def read_csv(self, file_path):
        """Read CSV file"""
        return pd.read_csv(file_path)
        
    def read_excel(self, file_path):
        """Read Excel file"""
        return pd.read_excel(file_path)
        
    def read_pdf(self, file_path):
        """Extract tables from PDF file"""
        # Try tabula-py for table extraction first
        try:
            tables = tabula.read_pdf(file_path, pages='all', multiple_tables=True)
            if tables and len(tables) > 0:
                # Return first table or combine tables as needed
                return tables[0]
        except Exception as e:
            print(f"Tabula extraction failed: {e}")
            
        # Fallback to PyPDF2 for text extraction and manual parsing
        try:
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
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"PDF extraction failed: {e}")
            return pd.DataFrame()
            
    def map_columns(self, df, table_name):
        """Map dataframe columns to Supabase table columns"""
        # Use schemas from db.py
        table_columns = {
            "fish": FISH_COLUMNS,
            "oceanography": OCEANOGRAPHY_COLUMNS,
            "edna": EDNA_COLUMNS
        }.get(table_name, [])
        if not table_columns:
            print(f"Unknown table: {table_name}")
            return None
        # Map columns by name (case-insensitive)
        mapped = {}
        for col in df.columns:
            for db_col in table_columns:
                if col.strip().lower() == db_col.lower():
                    mapped[db_col] = col
        # Build new DataFrame with only matching columns
        if not mapped:
            print(f"No matching columns for table {table_name}")
            return None
        return df[[mapped[c] for c in mapped]] if mapped else None

    def fix_special_columns(self, df, table_name):
        """Convert array, enum, and JSON columns to correct types for Supabase"""
        import json
        import ast
        if table_name == "fish":
            if "synonyms" in df.columns:
                df["synonyms"] = df["synonyms"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x)
            # Fix enums to lowercase
            for enum_col in ["reproductive_type", "habitat_type", "diet_type"]:
                if enum_col in df.columns:
                    df[enum_col] = df[enum_col].apply(lambda x: x.lower() if isinstance(x, str) else x)
        elif table_name == "oceanography":
            if "nutrients" in df.columns:
                df["nutrients"] = df["nutrients"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (json.dumps(ast.literal_eval(x)) if isinstance(x, str) and x.startswith("{") else x))
        elif table_name == "edna":
            if "blast_matching" in df.columns:
                df["blast_matching"] = df["blast_matching"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (json.dumps(ast.literal_eval(x)) if isinstance(x, str) and x.startswith("{") else x))
            if "species_detected" in df.columns:
                df["species_detected"] = df["species_detected"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith("[") else x)
        return df

    def get_enum_values(self, table_name, column_name):
        """Fetch valid enum values from the database for a given column."""
        try:
            query = f"SELECT t.enumlabel FROM pg_type typ JOIN pg_enum t ON t.enumtypid = typ.oid JOIN pg_attribute a ON a.atttypid = typ.oid WHERE a.attrelid = '{table_name}'::regclass AND a.attname = '{column_name}';"
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                return [row[0] for row in result]
        except Exception as e:
            print(f"Could not fetch enum values for {table_name}.{column_name}: {e}")
            return []

    def filter_valid_rows(self, df, table_name):
        """Remove rows with missing required fields and invalid enum values."""
        required_fields = {
            "fish": ["scientific_name", "diet_type", "reproductive_type", "habitat_type"],
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
                df = df[df[col].isin(valid_values)]
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
            # Insert data using SQLAlchemy
            mapped_df.to_sql(table_name, self.engine, if_exists=if_exists, index=False, method='multi')
            print(f"Successfully imported {len(mapped_df)} rows to '{table_name}' on Supabase")
            return True
        except Exception as e:
            print(f"Error importing data to table '{table_name}': {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()
            print("Database connection closed")

# Usage example
if __name__ == "__main__":
    importer = DataImporter()
    # Example usage:
    # df = importer.read_file("path/to/your/fish_data.csv")
    # importer.import_to_db(df, "fish")
    # df = importer.read_file("path/to/your/oceanography.xlsx")
    # importer.import_to_db(df, "oceanography")
    # df = importer.read_file("path/to/your/edna.csv")
    # importer.import_to_db(df, "edna")
    importer.close()