from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
from test_import import runn
from overall_plotting import predict_overall
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import requests

app = FastAPI()
UPLOAD_DIR = "test_files"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Database connection
DATABASE_URL = "postgresql://postgres.mqtghilvvxpvjnpzodkd:sianfkp63@aws-1-ap-south-1.pooler.supabase.com:5432/postgres"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Request model for city endpoint
class CityRequest(BaseModel):
    city_name: str
    start_year: int = 2000
    end_year: int = 2023
    future_years: int = 5
    radius_km: float = 100.0  # Search radius in kilometers

# Predefined city coordinates
CITY_COORDINATES = {
    "mumbai": {"lat": 19.0760, "lng": 72.8777},
    "chennai": {"lat": 13.0827, "lng": 80.2707},
    "kochi": {"lat": 9.9312, "lng": 76.2673},
    "kolkata": {"lat": 22.5726, "lng": 88.3639},
    "visakhapatnam": {"lat": 17.6868, "lng": 83.2185},
    "goa": {"lat": 15.2993, "lng": 74.1240},
    "new_york": {"lat": 40.7128, "lng": -74.0060},
    "london": {"lat": 51.5074, "lng": -0.1278},
    "tokyo": {"lat": 35.6762, "lng": 139.6503},
    "sydney": {"lat": -33.8688, "lng": 151.2093},
    "los_angeles": {"lat": 34.0522, "lng": -118.2437},
    "miami": {"lat": 25.7617, "lng": -80.1918}
}

def get_city_coordinates(city_name):
    """Get coordinates for a city."""
    city_key = city_name.lower().replace(" ", "_")
    
    if city_key in CITY_COORDINATES:
        coords = CITY_COORDINATES[city_key]
        return coords["lat"], coords["lng"]
    
    try:
        url = f"https://nominatim.openstreetmap.org/search?q={city_name}&format=json&limit=1"
        response = requests.get(url, headers={'User-Agent': 'MarineDataAPI/1.0'})
        
        if response.status_code == 200:
            data = response.json()
            if data:
                return float(data[0]['lat']), float(data[0]['lon'])
    except Exception as e:
        print(f"Geocoding error: {e}")
    
    return None, None

def get_species_near_location(lat, lng, radius_km=100, limit=5):
    """Get any 5 species near the given coordinates."""
    db = SessionLocal()
    try:
        query = text("""
            SELECT DISTINCT "scientificName"
            FROM fish 
            WHERE 
                "scientificName" IS NOT NULL 
                AND "decimalLatitude" IS NOT NULL 
                AND "decimalLongitude" IS NOT NULL
                AND ST_DWithin(
                    ST_GeogFromText('POINT(' || :lng || ' ' || :lat || ')'),
                    location,
                    :radius_meters
                )
            LIMIT :limit
        """)
        
        result = db.execute(query, {
            "lat": lat,
            "lng": lng, 
            "radius_meters": radius_km * 1000,
            "limit": limit
        })
        
        species_list = []
        for row in result:
            species_list.append(row.scientificName)
        
        return species_list
        
    except Exception as e:
        print(f"Database query error: {e}")
        return ["Thunnus albacares", "Katsuwonus pelamis", "Scomber scombrus"]
    finally:
        db.close()


@app.post("/upload")
def upload_file(file: UploadFile = File(...)):
    file_location = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_location, "wb") as f:
        f.write(file.file.read())
    runn()
    return JSONResponse({"filename": file.filename, "saved_to": file_location})

@app.post("/city-marine-data")
def get_city_marine_data(request: CityRequest):
    """
    Get marine species occurrence data for a given city.
    Uses city coordinates to find 5 species in the area from database.
    """
    # Get city coordinates
    lat, lng = get_city_coordinates(request.city_name)
    
    if lat is None or lng is None:
        raise HTTPException(
            status_code=404, 
            detail=f"Could not find coordinates for city '{request.city_name}'"
        )
    
    # Get 5 species near this location from database
    species_list = get_species_near_location(lat, lng, request.radius_km, 5)
    
    if not species_list:
        raise HTTPException(
            status_code=404,
            detail=f"No marine species found near '{request.city_name}' within {request.radius_km}km radius"
        )
    
    try:
        # Get the marine data using overall_plotting function
        marine_data = predict_overall(
            species_list=species_list,
            start_year=2000,
            end_year=2023,
            future_years=5
        )
        
        # Add metadata to response
        response = {
            "city": request.city_name,
            "coordinates": {"latitude": lat, "longitude": lng},
            "search_radius_km": request.radius_km,
            "species_analyzed": species_list,
            "data_period": {
                "start_year": 2000,
                "end_year": 2023,
                "future_years_predicted": 5
            },
            "marine_data": marine_data
        }
        
        return JSONResponse(content=response)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing marine data: {str(e)}")

@app.get("/city/{city_name}/coordinates")
def get_city_coords(city_name: str):
    """Get coordinates for a city."""
    lat, lng = get_city_coordinates(city_name)
    
    if lat is None or lng is None:
        raise HTTPException(
            status_code=404,
            detail=f"Could not find coordinates for city '{city_name}'"
        )
    
    return JSONResponse(content={
        "city": city_name,
        "latitude": lat,
        "longitude": lng
    })

@app.get("/location/{lat}/{lng}/species")
def get_species_by_coordinates(lat: float, lng: float, radius_km: float = 100, limit: int = 5):
    """Get species near given coordinates."""
    species_list = get_species_near_location(lat, lng, radius_km, limit)
    
    return JSONResponse(content={
        "coordinates": {"latitude": lat, "longitude": lng},
        "search_radius_km": radius_km,
        "species_found": species_list,
        "species_count": len(species_list)
    })



# To run: uvicorn upload_api:app --reload
# You can POST files to /upload from your frontend using FormData.
