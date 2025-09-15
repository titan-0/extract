import requests
import pandas as pd
import json

def get_obis_yearly_records(species, start_year=2000, end_year=2023, debug=False):
    """
    Get yearly occurrence counts from OBIS /statistics/years
    Returns DataFrame: Year, OBIS_Records
    """
    params = {
        "scientificname": species,
        "startdate": f"{start_year}-01-01",
        "enddate": f"{end_year}-12-31"
    }

    url_years = "https://api.obis.org/v3/statistics/years"
    r = requests.get(url_years, params=params, headers={"Accept": "application/json"})
    if debug:
        print("REQUEST URL:", r.url)
        print("STATUS:", r.status_code)

    if r.status_code != 200:
        if debug:
            print("Request failed:", r.text)
        return None

    try:
        data = r.json()
    except Exception as e:
        if debug:
            print("Invalid JSON:", e)
        return None

    if debug:
        print("Sample response:", json.dumps(data[:5], indent=2) if isinstance(data, list) else data)

    if isinstance(data, list) and data:
        df = pd.DataFrame(data)
        if "year" in df.columns and "records" in df.columns:
            df = df.rename(columns={"year": "Year", "records": "OBIS_Records"})
            df["Year"] = df["Year"].astype(int)
            df["OBIS_Records"] = df["OBIS_Records"].astype(int)
            return df.sort_values("Year").reset_index(drop=True)

    return None


# Example




def load_fao_data(file_path, country=None, species=None):
    """
    Load FAO FishStatJ CSV and filter by country and species if provided.
    Returns a DataFrame with columns: Year, Production
    """
    df = pd.read_csv(file_path)

    # Normalize column names (sometimes they contain spaces or extra chars)
    df.columns = df.columns.str.strip()

    # Optional filter by country
    if country:
        df = df[df['Country (Name)'].str.contains(country, case=False, na=False)]

    # Optional filter by species
    if species:
        df = df[df['ASFIS species (Name)'].str.contains(species, case=False, na=False)]

    # Extract year columns (they are numeric)
    year_cols = [c for c in df.columns if c.isdigit()]
    df_yearly = df[year_cols].sum().reset_index()
    df_yearly.columns = ["Year", "Production"]

    # Convert to numeric types
    df_yearly["Year"] = df_yearly["Year"].astype(int)
    df_yearly["Production"] = pd.to_numeric(df_yearly["Production"], errors="coerce").fillna(0).astype(int)

    return df_yearly.sort_values("Year").reset_index(drop=True)


# Example usage


