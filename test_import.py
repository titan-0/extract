import pdfplumber
from file_to_postgres import DataImporter, best_table_match
import pandas as pd
import os

# ...existing code...
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

# Initialize the importer
importer = DataImporter()


def get_column_names(file_path):
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path, nrows=0)
    elif file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path, nrows=0)
    elif file_path.endswith('.pdf'):
        with pdfplumber.open(file_path) as pdf:
            # Try to extract the first table from the first page
            first_page = pdf.pages[0]
            table = first_page.extract_table()
            if table and len(table) > 0:
                # First row is usually the header
                return table[0]
            else:
                return []
    elif file_path.endswith('.txt'):
        # Try tab first, then comma
        try:
            df = pd.read_csv(file_path, sep='\t', nrows=0)
            if df.shape[1] == 1:
                df = pd.read_csv(file_path, sep=',', nrows=0)
            return list(df.columns)
        except Exception as e:
            print(f"TXT column extraction failed: {e}")
            return []
    else:
        raise ValueError("Unsupported file type")
    return list(df.columns)
# List all test files and their target tables
test_cases = []

for fname in os.listdir("test_files"):
    fpath = os.path.join("test_files", fname)
    _, extension = os.path.splitext(fname)
    extension = extension.lower()
   
    
    if(extension in ['.csv', '.xlsx', '.xls', '.txt']):
        columns = get_column_names(fpath)
        print(f"File: {fname}, Columns: {columns}")
        table_name = best_table_match(columns, schemas, PRIMARY_KEY_SYNONYMS)
        print("Most likely table:", table_name)
        importer.import_to_db(importer.read_file(fpath), table_name)

    
    # Special handling for PDF files to show and process all tables
    if extension == '.pdf':
        print(f"Processing PDF file: {fname}")
        pdf_tables = importer.read_pdf(fpath)
        if pdf_tables:
            print(f"\nFound {len(pdf_tables)} tables in {fname}:")
            
            # Process each table in the PDF
            for i, table_df in enumerate(pdf_tables):
                print(f"\n--- Table {i+1} Preview ---")
                print(table_df.head())
                
                # Try to determine table type for each individual table
                table_columns = list(table_df.columns)
                current_table_type = best_table_match(table_columns, schemas, PRIMARY_KEY_SYNONYMS)
                
                if current_table_type:
                    print(f"Importing table {i+1} into {current_table_type}")
                    
                    # Process this specific table
                    success = importer.import_to_db(table_df, current_table_type)
                    if success:
                        print(f"✅ Successfully imported table {i+1} from {fname} into {current_table_type}")
                    else:
                        print(f"❌ Failed to import table {i+1} from {fname} into {current_table_type}")
                else:
                    print(f"❌ Could not determine target table for table {i+1} in {fname}")
            
            # Set flag to skip the general import logic since we've handled it here
            skip_general_import = True
            
        else:
            print("No valid tables found in PDF")
            continue  # Skip to next file
    
        
  # Move processed file to test_files2 directory
    src = fpath
    dst = os.path.join("test_files2", fname)
    os.rename(src, dst)

importer.close()




