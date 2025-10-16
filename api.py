from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import FileResponse
from pydantic import BaseModel
import re
import requests
from urllib.parse import quote
import uvicorn
import pandas as pd
import os
from datetime import datetime
import time

app = FastAPI(title="Geocoding API", description="Get coordinates from addresses using Google Maps")

class AddressRequest(BaseModel):
    address: str

class CoordinateRequest(BaseModel):
    lat: float
    long: float

class CoordinateResponse(BaseModel):
    address: str
    lat: float
    long: float
    coordinates: str
    google_maps_url: str

class AddressResponse(BaseModel):
    lat: float
    long: float
    address: str
    formatted_address: str
    google_maps_url: str

def extract_lat_long(url):
    """
    Extract latitude and longitude from a Google Maps URL.
    If multiple coordinates are found, returns the first one (top result).
    """
    # Pattern 1: URL format with @latitude,longitude
    pattern1 = r'@(-?\d+\.\d+),(-?\d+\.\d+)'
    match1 = re.search(pattern1, url)
    if match1:
        lat = float(match1.group(1))
        lng = float(match1.group(2))
        return lat, lng

    # Pattern 2: URL format with !3d and !4d
    pattern2 = r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)'
    match2 = re.search(pattern2, url)
    if match2:
        lat = float(match2.group(1))
        lng = float(match2.group(2))
        return lat, lng

    return None, None

def extract_lat_long_from_html(html_content):
    """
    Extract coordinates from Google Maps HTML content.
    """
    patterns = [
        r'\[\d+,"[-\w]+",\d+,\d+,null,null,(-?\d+\.\d+),(-?\d+\.\d+)\]',
        r'"(-?\d+\.\d+),(-?\d+\.\d+)"',
        r'center=(-?\d+\.\d+),(-?\d+\.\d+)',
        r'@(-?\d+\.\d+),(-?\d+\.\d+)',
        r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)',
    ]

    for pattern in patterns:
        matches = re.findall(pattern, html_content)
        if matches:
            if isinstance(matches[0], tuple) and len(matches[0]) == 2:
                try:
                    lat = float(matches[0][0])
                    lng = float(matches[0][1])
                    if -90 <= lat <= 90 and -180 <= lng <= 180:
                        return lat, lng
                except ValueError:
                    continue

    return None, None

def get_coordinates(address):
    """
    Get coordinates from an address by searching Google Maps.
    """
    search_url = f"https://www.google.com/maps/search/{quote(address)}"

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(search_url, headers=headers, allow_redirects=True, timeout=10)

        url = response.url
        html_content = response.text

        # Try to extract from URL first
        lat, lng = extract_lat_long(url)

        # If not found, parse HTML
        if lat is None or lng is None:
            lat, lng = extract_lat_long_from_html(html_content)

        return lat, lng, url
    except Exception as e:
        return None, None, str(e)

def get_address_from_coordinates(lat, lng):
    """
    Get address from latitude and longitude (reverse geocoding).
    Uses Nominatim (OpenStreetMap) for free reverse geocoding.
    """
    try:
        # Use Nominatim API (OpenStreetMap) for reverse geocoding
        nominatim_url = f"https://nominatim.openstreetmap.org/reverse"
        params = {
            'lat': lat,
            'lon': lng,
            'format': 'json',
            'addressdetails': 1
        }
        headers = {
            'User-Agent': 'GeocodingAPI/1.0'
        }

        response = requests.get(nominatim_url, params=params, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()

            if 'display_name' in data:
                address = data['display_name']

                # Also try to construct a cleaner address from components
                formatted_parts = []
                addr = data.get('address', {})

                # Building/house
                if addr.get('building'):
                    formatted_parts.append(addr['building'])
                elif addr.get('house_number'):
                    formatted_parts.append(addr['house_number'])

                # Road/Street
                if addr.get('road'):
                    formatted_parts.append(addr['road'])
                elif addr.get('neighbourhood'):
                    formatted_parts.append(addr['neighbourhood'])

                # Suburb/Area
                if addr.get('suburb'):
                    formatted_parts.append(addr['suburb'])

                # City
                if addr.get('city'):
                    formatted_parts.append(addr['city'])
                elif addr.get('town'):
                    formatted_parts.append(addr['town'])
                elif addr.get('village'):
                    formatted_parts.append(addr['village'])

                # Postcode
                if addr.get('postcode'):
                    formatted_parts.append(addr['postcode'])

                # State
                if addr.get('state'):
                    formatted_parts.append(addr['state'])

                # Country
                if addr.get('country'):
                    formatted_parts.append(addr['country'])

                formatted_address = ', '.join(formatted_parts) if formatted_parts else address

                google_maps_url = f"https://www.google.com/maps/place/{lat},{lng}"

                return address, formatted_address, google_maps_url

        # Fallback: try Google Maps scraping
        place_url = f"https://www.google.com/maps/place/{lat},{lng}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(place_url, headers=headers, allow_redirects=True, timeout=10)

        from urllib.parse import unquote
        place_match = re.search(r'/place/([^/@]+)/@', response.url)
        if place_match:
            address = place_match.group(1).replace('+', ' ')
            address = unquote(address)
            return address, address, response.url

        # Return coordinates if nothing works
        return f"{lat}, {lng}", f"Location at {lat}, {lng}", place_url

    except Exception as e:
        return f"{lat}, {lng}", f"Location at {lat}, {lng}", f"https://www.google.com/maps/place/{lat},{lng}"

@app.get("/")
def home():
    """API documentation."""
    return {
        "message": "Geocoding API - Get coordinates from addresses and vice versa",
        "endpoints": {
            "/geocode": {
                "methods": ["GET", "POST"],
                "description": "Get coordinates from an address",
                "examples": {
                    "GET": "/geocode?address=Medanta Hospital Gurgaon Bridge",
                    "POST": '{"address": "Medanta Hospital Gurgaon Bridge"}'
                }
            },
            "/reverse-geocode": {
                "methods": ["GET", "POST"],
                "description": "Get address from coordinates",
                "examples": {
                    "GET": "/reverse-geocode?lat=28.4391604&long=77.0388113",
                    "POST": '{"lat": 28.4391604, "long": 77.0388113}'
                }
            },
            "/geocode-file": {
                "methods": ["POST"],
                "description": "Upload CSV/Excel file and get coordinates for addresses",
                "parameters": "file (CSV/Excel), address_column (column name with addresses)"
            },
            "/reverse-geocode-file": {
                "methods": ["POST"],
                "description": "Upload CSV/Excel file and get addresses from coordinates",
                "parameters": "file (CSV/Excel), lat_column, long_column"
            },
            "/health": {
                "methods": ["GET"],
                "description": "Health check endpoint"
            },
            "/docs": {
                "methods": ["GET"],
                "description": "Interactive API documentation (Swagger UI)"
            }
        }
    }

@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}

@app.get("/geocode", response_model=CoordinateResponse)
def geocode_get(address: str = Query(..., description="The address to geocode")):
    """
    Get coordinates from an address using GET method.

    Example: /geocode?address=Medanta Hospital Gurgaon Bridge
    """
    lat, lng, url = get_coordinates(address)

    if lat is not None and lng is not None:
        return CoordinateResponse(
            address=address,
            lat=lat,
            long=lng,
            coordinates=f"{lat}, {lng}",
            google_maps_url=url
        )
    else:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Could not extract coordinates",
                "address": address,
                "details": url if isinstance(url, str) else "No coordinates found"
            }
        )

@app.post("/geocode", response_model=CoordinateResponse)
def geocode_post(request: AddressRequest):
    """
    Get coordinates from an address using POST method.

    Example body: {"address": "Medanta Hospital Gurgaon Bridge"}
    """
    lat, lng, url = get_coordinates(request.address)

    if lat is not None and lng is not None:
        return CoordinateResponse(
            address=request.address,
            lat=lat,
            long=lng,
            coordinates=f"{lat}, {lng}",
            google_maps_url=url
        )
    else:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Could not extract coordinates",
                "address": request.address,
                "details": url if isinstance(url, str) else "No coordinates found"
            }
        )

@app.get("/reverse-geocode", response_model=AddressResponse)
def reverse_geocode_get(
    lat: float = Query(..., description="Latitude"),
    long: float = Query(..., description="Longitude")
):
    """
    Get address from coordinates using GET method.

    Example: /reverse-geocode?lat=28.4391604&long=77.0388113
    """
    address, formatted_address, url = get_address_from_coordinates(lat, long)

    if address is not None:
        return AddressResponse(
            lat=lat,
            long=long,
            address=address,
            formatted_address=formatted_address,
            google_maps_url=url
        )
    else:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Could not extract address",
                "coordinates": f"{lat}, {long}",
                "details": url if isinstance(url, str) else "No address found"
            }
        )

@app.post("/reverse-geocode", response_model=AddressResponse)
def reverse_geocode_post(request: CoordinateRequest):
    """
    Get address from coordinates using POST method.

    Example body: {"lat": 28.4391604, "long": 77.0388113}
    """
    address, formatted_address, url = get_address_from_coordinates(request.lat, request.long)

    if address is not None:
        return AddressResponse(
            lat=request.lat,
            long=request.long,
            address=address,
            formatted_address=formatted_address,
            google_maps_url=url
        )
    else:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Could not extract address",
                "coordinates": f"{request.lat}, {request.long}",
                "details": url if isinstance(url, str) else "No address found"
            }
        )

@app.post("/geocode-file")
async def geocode_file(
    file: UploadFile = File(..., description="CSV or Excel file with addresses"),
    address_column: str = Form(..., description="Name of the column containing addresses")
):
    """
    Upload a CSV or Excel file with addresses and get coordinates.

    The API will add 'latitude' and 'longitude' columns to your file and return a new file.

    Parameters:
    - file: CSV or Excel file (.csv, .xlsx, .xls)
    - address_column: The name of the column that contains addresses

    Returns:
    - A new CSV file with latitude and longitude columns added
    """
    # Create uploads directory if it doesn't exist
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)

    # Validate file extension
    filename = file.filename
    file_ext = os.path.splitext(filename)[1].lower()

    if file_ext not in ['.csv', '.xlsx', '.xls']:
        raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

    # Save uploaded file temporarily
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_input_path = f"uploads/input_{timestamp}{file_ext}"

    with open(temp_input_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)

    try:
        # Read file based on extension
        if file_ext == '.csv':
            df = pd.read_csv(temp_input_path)
        else:
            df = pd.read_excel(temp_input_path)

        # Check if address column exists
        if address_column not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{address_column}' not found. Available columns: {', '.join(df.columns)}"
            )

        # Initialize new columns
        df['latitude'] = None
        df['longitude'] = None

        # Process each address
        total_rows = len(df)
        for idx, row in df.iterrows():
            address = row[address_column]

            if pd.notna(address) and str(address).strip():
                print(f"Processing {idx + 1}/{total_rows}: {address}")

                lat, lng, _ = get_coordinates(str(address))

                df.at[idx, 'latitude'] = lat
                df.at[idx, 'longitude'] = lng

                # Add small delay to avoid rate limiting
                time.sleep(0.5)

        # Save output file
        output_filename = f"geocoded_{timestamp}.csv"
        output_path = f"outputs/{output_filename}"
        df.to_csv(output_path, index=False)

        # Clean up input file
        os.remove(temp_input_path)

        # Return the file
        return FileResponse(
            path=output_path,
            filename=output_filename,
            media_type='text/csv',
            headers={
                "Content-Disposition": f"attachment; filename={output_filename}"
            }
        )

    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_input_path):
            os.remove(temp_input_path)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.post("/reverse-geocode-file")
async def reverse_geocode_file(
    file: UploadFile = File(..., description="CSV or Excel file with coordinates"),
    lat_column: str = Form(..., description="Name of the column containing latitude"),
    long_column: str = Form(..., description="Name of the column containing longitude")
):
    """
    Upload a CSV or Excel file with coordinates and get addresses.

    The API will add 'address' column to your file and return a new file.

    Parameters:
    - file: CSV or Excel file (.csv, .xlsx, .xls)
    - lat_column: The name of the column that contains latitude
    - long_column: The name of the column that contains longitude

    Returns:
    - A new CSV file with address column added
    """
    # Create uploads directory if it doesn't exist
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("outputs", exist_ok=True)

    # Validate file extension
    filename = file.filename
    file_ext = os.path.splitext(filename)[1].lower()

    if file_ext not in ['.csv', '.xlsx', '.xls']:
        raise HTTPException(status_code=400, detail="Only CSV and Excel files are supported")

    # Save uploaded file temporarily
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    temp_input_path = f"uploads/input_{timestamp}{file_ext}"

    with open(temp_input_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)

    try:
        # Read file based on extension
        if file_ext == '.csv':
            df = pd.read_csv(temp_input_path)
        else:
            df = pd.read_excel(temp_input_path)

        # Check if coordinate columns exist
        if lat_column not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{lat_column}' not found. Available columns: {', '.join(df.columns)}"
            )

        if long_column not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{long_column}' not found. Available columns: {', '.join(df.columns)}"
            )

        # Initialize new column
        df['address'] = None

        # Process each coordinate pair
        total_rows = len(df)
        for idx, row in df.iterrows():
            lat = row[lat_column]
            lng = row[long_column]

            if pd.notna(lat) and pd.notna(lng):
                try:
                    lat_val = float(lat)
                    lng_val = float(lng)
                    print(f"Processing {idx + 1}/{total_rows}: {lat_val}, {lng_val}")

                    address, _, _ = get_address_from_coordinates(lat_val, lng_val)
                    df.at[idx, 'address'] = address

                    # Add delay to comply with Nominatim rate limit (1 req/sec)
                    time.sleep(1)
                except ValueError:
                    print(f"Invalid coordinates at row {idx + 1}: {lat}, {lng}")
                    df.at[idx, 'address'] = None

        # Save output file
        output_filename = f"reverse_geocoded_{timestamp}.csv"
        output_path = f"outputs/{output_filename}"
        df.to_csv(output_path, index=False)

        # Clean up input file
        os.remove(temp_input_path)

        # Return the file
        return FileResponse(
            path=output_path,
            filename=output_filename,
            media_type='text/csv',
            headers={
                "Content-Disposition": f"attachment; filename={output_filename}"
            }
        )

    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_input_path):
            os.remove(temp_input_path)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
