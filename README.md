# Marine Data API Documentation

Version: 1.0  
Base URL: `http://localhost:8000`

## Overview

The Marine Data API provides access to fish and oceanographic data with features including:
- File upload and processing for marine data files
- Marine data retrieval by city location
- Species-specific marine data analytics
- Natural language querying of the marine database

## Authentication

Currently, this API does not require authentication.

---

## 1. Upload API

The Upload API allows users to upload files containing marine data for processing.

### Upload a File

**Endpoint:** `POST /upload`

**Description:** Upload a file containing marine data for processing and analysis.

**Request:**
- Content-Type: `multipart/form-data`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file | File | Yes | The file to be uploaded |

**Response:**

```json
{
  "filename": "example.csv",
  "saved_to": "test_files/example.csv"
}
```

**Status Codes:**
- `200 OK`: File uploaded successfully
- `422 Unprocessable Entity`: Invalid file format
- `500 Internal Server Error`: Server error

**Example:**

```bash
curl -X POST "http://localhost:8000/upload" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@fish_data.csv"
```

---

## 2. City Marine API

The City Marine API provides access to marine data specific to geographical locations.

### Get Marine Data by City

**Endpoint:** `POST /city-marine-data`

**Description:** Retrieves marine species occurrence data for a specified city, searching for species within a given radius.

**Request:**
- Content-Type: `application/json`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| city_name | string | Yes | - | Name of the city to search around |
| start_year | integer | No | 2000 | Start year for data analysis |
| end_year | integer | No | 2023 | End year for data analysis |
| future_years | integer | No | 5 | Number of years for future prediction |
| radius_km | float | No | 100.0 | Search radius in kilometers |

**Response:**

```json
{
  "city": "Mumbai",
  "coordinates": {
    "latitude": 19.076,
    "longitude": 72.8777
  },
  "search_radius_km": 100,
  "species_analyzed": [
    "Thunnus albacares", 
    "Katsuwonus pelamis", 
    "Scomber scombrus",
    "Rastrelliger kanagurta",
    "Stolephorus commersonii"
  ],
  "data_period": {
    "start_year": 2000,
    "end_year": 2023,
    "future_years_predicted": 5
  },
  "marine_data": {
    // Marine data analysis results
  }
}
```

**Status Codes:**
- `200 OK`: Data retrieved successfully
- `404 Not Found`: City coordinates or marine species not found
- `500 Internal Server Error`: Server error

**Example:**

```bash
curl -X POST "http://localhost:8000/city-marine-data" \
  -H "Content-Type: application/json" \
  -d '{
    "city_name": "Mumbai",
    "radius_km": 150
  }'
```

### Get City Coordinates

**Endpoint:** `GET /city/{city_name}/coordinates`

**Description:** Retrieves geographical coordinates for a specified city.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| city_name | string | Yes | Name of the city |

**Response:**

```json
{
  "city": "Mumbai",
  "latitude": 19.076,
  "longitude": 72.8777
}
```

**Status Codes:**
- `200 OK`: Coordinates retrieved successfully
- `404 Not Found`: City not found
- `500 Internal Server Error`: Server error

**Example:**

```bash
curl -X GET "http://localhost:8000/city/Mumbai/coordinates"
```

### Get Species by Coordinates

**Endpoint:** `GET /location/{lat}/{lng}/species`

**Description:** Retrieves marine species found near specified geographical coordinates.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| lat | float | Yes | - | Latitude coordinate |
| lng | float | Yes | - | Longitude coordinate |
| radius_km | float | No | 100 | Search radius in kilometers |
| limit | integer | No | 5 | Maximum number of species to return |

**Response:**

```json
{
  "coordinates": {
    "latitude": 19.076,
    "longitude": 72.8777
  },
  "search_radius_km": 100,
  "species_found": [
    "Thunnus albacares",
    "Katsuwonus pelamis",
    "Scomber scombrus",
    "Rastrelliger kanagurta",
    "Stolephorus commersonii"
  ],
  "species_count": 5
}
```

**Status Codes:**
- `200 OK`: Species retrieved successfully
- `500 Internal Server Error`: Server error

**Example:**

```bash
curl -X GET "http://localhost:8000/location/19.076/72.8777/species?radius_km=150&limit=10"
```

### Get Species Information

**Endpoint:** `POST /species`

**Description:** Retrieves detailed analysis for a specific marine species.

**Request:**
- Content-Type: `application/json`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| scientific_name | string | Yes | - | Scientific name of the species |
| start_year | integer | No | 2000 | Start year for data analysis |
| end_year | integer | No | 2023 | End year for data analysis |
| future_years | integer | No | 5 | Number of years for future prediction |

**Response:**

```json
{
  "species": "Thunnus albacares",
  "analysis_period": {
    "start_year": 2000,
    "end_year": 2023,
    "future_years_predicted": 5
  },
  "data": {
    // Species analysis data
  }
}
```

**Status Codes:**
- `200 OK`: Analysis retrieved successfully
- `500 Internal Server Error`: Error analyzing species

**Example:**

```bash
curl -X POST "http://localhost:8000/species" \
  -H "Content-Type: application/json" \
  -d '{
    "scientific_name": "Thunnus albacares",
    "future_years": 10
  }'
```

---

## 3. Natural Language Query API

The Natural Language Query API allows users to query the marine database using natural language.

### Process Natural Language Query

**Endpoint:** `POST /natural-query`

**Description:** Processes natural language queries about marine data, converting them to SQL and executing against the database.

**Request:**
- Content-Type: `application/json`

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| query | string | Yes | - | Natural language query about marine data |
| max_retries | integer | No | 3 | Maximum number of retry attempts if query fails |

**Response:**

```json
{
  "query": "Give me names of all fishes around Mumbai",
  "results_count": 15,
  "data": [
    {
      "scientificName": "Thunnus albacares",
      "species": "Yellowfin tuna",
      // Other fish data
    },
    // More results
  ],
  "status": "success"
}
```

**Status Codes:**
- `200 OK`: Query processed successfully
- `500 Internal Server Error`: Error processing query

**Example Queries:**
- "Give me names of all fishes around Mumbai"
- "Show me oceanography data near Chennai"
- "What species are found in the North Sea?"
- "List all carnivorous fish species in the database"
- "Find water temperature readings above 20°C in the Arabian Sea"

**Example:**

```bash
curl -X POST "http://localhost:8000/natural-query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Give me names of all fishes around Mumbai",
    "max_retries": 3
  }'
```

---

## Running the API Server

Start the API server with the following command:

```bash
uvicorn upload_api:app --reload
```

By default, the server runs on `http://localhost:8000`. You can access the Swagger UI documentation at `http://localhost:8000/docs`.

## Error Handling

All endpoints follow a standard error response format:

```json
{
  "detail": "Error message describing the issue"
}
```

Common HTTP status codes:
- `200`: Success
- `404`: Resource not found
- `422`: Validation error (missing or invalid parameters)
- `500`: Server error

## Data Model References

### Fish Schema
- `id`: Unique identifier (integer)
- `scientificName`: Scientific name of the species (string)
- `species`: Common name (string)
- `class`, `family`, `kingdom`, `phylum`: Taxonomic classification (string)
- `location`: Geographical point (PostGIS geography)
- `decimalLatitude`, `decimalLongitude`: Coordinates (float)
- `lifespan_years`: Lifespan in years (float)
- `migration_patterns`: Migration behavior (string)
- `habitat`: Habitat type (enum)
- `diet_type`: Diet classification (enum)
- `reproductive_type`: Reproduction classification (enum)

### Oceanography Schema
- `data_set`: Dataset identifier (string)
- `version`: Version identifier (string)
- `location`: Geographical point (PostGIS geography)
- `decimalLatitude`, `decimalLongitude`: Coordinates (float)
- `waterTemperature`: Water temperature (float)
- `salinity`: Salinity measurement (float)
- `dissolvedOxygen`: Dissolved oxygen level (float)
- `water_pH`: pH measurement (float)
- `nutrients`: Nutrient information (JSON)
