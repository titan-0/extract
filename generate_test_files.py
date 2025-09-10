import pandas as pd
import numpy as np
import os

# Create directory for test files if it doesn't exist
os.makedirs('test_files', exist_ok=True)

# Sample 1: Complete data (name, roll, percentage)
data1 = {
    'name': ['John Doe', 'Jane Smith', 'Robert Johnson', 'Emily Williams', 'Michael Brown'],
    'roll': ['A001', 'A002', 'A003', 'A004', 'A005'],
    'percentage': [85.5, 92.3, 78.9, 88.7, 76.2]
}
df1 = pd.DataFrame(data1)
df1.to_excel('test_files/students_complete.xlsx', index=False)
print("Created: students_complete.xlsx")

# Sample 2: Only name and roll
data2 = {
    'name': ['Alex Turner', 'Maria Garcia', 'James Wilson', 'Sophia Martinez', 'Daniel Anderson'],
    'roll': ['B001', 'B002', 'B003', 'B004', 'B005']
}
df2 = pd.DataFrame(data2)
df2.to_excel('test_files/students_partial1.xlsx', index=False)
print("Created: students_partial1.xlsx")

# Sample 3: Only roll and percentage
data3 = {
    'roll': ['C001', 'C002', 'C003', 'C004', 'C005'],
    'percentage': [91.4, 82.7, 95.3, 79.8, 88.1]
}
df3 = pd.DataFrame(data3)
df3.to_excel('test_files/students_partial2.xlsx', index=False)
print("Created: students_partial2.xlsx")

# Sample 4: Different column names but same data
data4 = {
    'student_name': ['David Lee', 'Sarah Johnson', 'Kevin Chen', 'Amanda Miller', 'Jason Wong'],
    'student_id': ['D001', 'D002', 'D003', 'D004', 'D005'],
    'score': [77.3, 89.5, 94.2, 81.7, 86.9]
}
df4 = pd.DataFrame(data4)
df4.to_excel('test_files/students_renamed_columns.xlsx', index=False)
print("Created: students_renamed_columns.xlsx")

# Sample 5: Columns in different order
data5 = {
    'percentage': [75.6, 88.2, 92.7, 79.3, 84.1],
    'name': ['William Taylor', 'Olivia Thomas', 'Noah Jackson', 'Emma White', 'Liam Harris'],
    'roll': ['E001', 'E002', 'E003', 'E004', 'E005']
}
df5 = pd.DataFrame(data5)
df5.to_excel('test_files/students_reordered.xlsx', index=False)
print("Created: students_reordered.xlsx")

# --- Fish table tests ---
fish_data = {
    'scientific_name': ['Salmo salar', 'Oncorhynchus mykiss', 'Cyprinus carpio', None, 'Gadus morhua'],
    'species': ['Atlantic salmon', 'Rainbow trout', 'Common carp', 'Unknown', 'Atlantic cod'],
    'class': ['Actinopterygii', 'Actinopterygii', 'Actinopterygii', 'Actinopterygii', None],
    'family': ['Salmonidae', 'Salmonidae', 'Cyprinidae', 'Unknown', 'Gadidae'],
    'location': [None, None, None, None, None],  # Edge: missing location
    'locality': ['North Atlantic', 'North America', 'Europe', 'Asia', 'Arctic'],
    'kingdom': ['Animalia']*5,
    'fishing_region': ['Region1', 'Region2', 'Region3', 'Region4', 'Region5'],
    'depth_range': ['0-200m', '0-50m', '0-10m', '0-100m', '0-500m'],
    'lifespan_years': [5, 7, 20, 10, None],  # Edge: missing value
    'migration_patterns': ['anadromous', 'resident', 'migratory', 'unknown', 'migratory'],
    'synonyms': [['Salmo trutta'], [], ['Carpio'], ['Unknown'], ['Morhua']],
    'reproductive_type': ['Oviparous', 'Oviparous', 'Oviparous', 'Viviparous', 'Ovoviviparous'],
    'habitat_type': ['Freshwater', 'Freshwater', 'Freshwater', 'Marine', 'Marine'],
    'phylum': ['Chordata']*5,
    'diet_type': ['Carnivore', 'Omnivore', 'Herbivore', 'Detritivore', 'Planktivore']
}
pd.DataFrame(fish_data).to_csv('test_files/fish_test.csv', index=False)
print("Created: fish_test.csv")

# --- Oceanography table tests ---
ocean_data = {
    'data_set': ['OCEAN1', 'OCEAN2', 'OCEAN3', 'OCEAN4', 'OCEAN5'],
    'version': ['v1', 'v2', 'v3', 'v4', 'v5'],
    'location': [None]*5,  # Edge: missing location
    'max_depth': [1000, 2000, None, 1500, 500],  # Edge: missing value
    'temperature_kelvin': [285.15, 290.15, 295.15, 280.15, 275.15],
    'salinity_psu': [35, 34, 36, 33, None],  # Edge: missing value
    'dissolved_oxygen': [6.5, 7.2, 5.8, 8.0, 6.0],
    'ph': [8.1, 8.0, 7.9, 8.2, 8.1],
    'chlorophyll_mg_m3': [1.2, 0.8, 1.5, 1.0, 0.9],
    'nutrients': [{"nitrate": 2.5}, {"phosphate": 0.5}, {}, {"silicate": 1.0}, None],
    'pressure_bar': [100, 200, 150, 250, 50],
    'density_kg_m3': [1025, 1024, 1026, 1023, 1022],
    'turbidity': [0.5, 0.7, 0.6, 0.8, 0.4],
    'alkalinity': [2.3, 2.2, 2.4, 2.1, 2.0],
    'surface_currents': [1.5, 1.2, 1.8, 1.0, 1.3]
}
pd.DataFrame(ocean_data).to_csv('test_files/oceanography_test.csv', index=False)
print("Created: oceanography_test.csv")

# --- EDNA table tests ---
edna_data = {
    'sequence_id': ['SEQ1', 'SEQ2', 'SEQ3', 'SEQ4', 'SEQ5'],
    'dna_sequence': ['ATCG', 'GGTA', 'TTAA', 'CCGG', None],  # Edge: missing value
    'description': ['Sample1', 'Sample2', 'Sample3', 'Sample4', 'Sample5'],
    'blast_matching': [{"match": "species1"}, {"match": "species2"}, {}, None, {"match": "species5"}],
    'sample_date': ['2022-01-01', '2022-02-01', '2022-03-01', '2022-04-01', '2022-05-01'],
    'location': [None]*5,  # Edge: missing location
    'collector': ['Alice', 'Bob', 'Charlie', 'Dana', 'Eve'],
    'sample_type': ['water', 'sediment', 'tissue', 'water', 'sediment'],
    'species_detected': [['Salmo salar'], ['Oncorhynchus mykiss'], [], ['Cyprinus carpio'], ['Gadus morhua']],
    'quality_score': [99.5, 88.7, 77.3, 66.2, None],  # Edge: missing value
    'status': ['complete', 'pending', 'complete', 'pending', 'complete'],
    'qr_code_link': ['link1', 'link2', 'link3', 'link4', 'link5'],
    'reference_link': ['ref1', 'ref2', 'ref3', 'ref4', 'ref5'],
    'project': ['ProjA', 'ProjB', 'ProjC', 'ProjD', 'ProjE'],
    'notes': ['note1', 'note2', 'note3', 'note4', 'note5']
}
pd.DataFrame(edna_data).to_csv('test_files/edna_test.csv', index=False)
print("Created: edna_test.csv")

print("\nAll test files for fish, oceanography, and edna created successfully in the 'test_files' directory.")