from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from typing import List, Optional
import logging
from contextlib import asynccontextmanager

from database import get_db, init_db
from models import DistrictData, DistrictStats
from services.mgnrega_service import MGNREGAService
from services.location_service import LocationService
from schemas import (
    DistrictDataResponse,
    DistrictStatsResponse,
    LocationRequest,
    ComparisonRequest,
    ComparisonResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up the application...")
    init_db()
    
    # Initialize services
    mgnrega_service = MGNREGAService()
    await mgnrega_service.start_scheduler()
    
    yield
    
    # Shutdown
    logger.info("Shutting down the application...")
    await mgnrega_service.stop_scheduler()

app = FastAPI(
    title="Our Voice, Our Rights - MGNREGA API",
    description="API for MGNREGA district-level data visualization",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
mgnrega_service = MGNREGAService()
location_service = LocationService()

@app.get("/")
async def root():
    return {"message": "Our Voice, Our Rights - MGNREGA API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "mgnrega-api"}

@app.post("/detect-district")
async def detect_district(location: LocationRequest, db: Session = Depends(get_db)):
    """Detect district based on GPS coordinates"""
    try:
        district_info = await location_service.get_district_from_coordinates(
            location.latitude, location.longitude
        )
        
        if not district_info:
            raise HTTPException(status_code=404, detail="District not found for given coordinates")
        
        return {
            "district": district_info["district"],
            "state": district_info["state"],
            "district_code": district_info.get("district_code"),
            "coordinates": {
                "latitude": location.latitude,
                "longitude": location.longitude
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error detecting district: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to detect district")

@app.get("/districts/{state}")
async def get_districts_by_state(state: str, db: Session = Depends(get_db)):
    """Get all districts in a state"""
    try:
        districts = await location_service.get_districts_by_state(state)
        return {"state": state, "districts": districts}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching districts for state {state}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch districts")

@app.get("/data/{district_code}")
async def get_district_data(
    district_code: str, 
    year: Optional[int] = None,
    db: Session = Depends(get_db)
) -> DistrictDataResponse:
    """Get MGNREGA data for a specific district"""
    try:
        data = await mgnrega_service.get_district_data(district_code, year, db)
        
        if not data:
            raise HTTPException(status_code=404, detail="District data not found")
        
        return DistrictDataResponse(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching data for district {district_code}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch district data")

@app.get("/stats/{district_code}")
async def get_district_stats(
    district_code: str,
    db: Session = Depends(get_db)
) -> DistrictStatsResponse:
    """Get key statistics for a district"""
    try:
        stats = await mgnrega_service.get_district_stats(district_code, db)
        
        if not stats:
            raise HTTPException(status_code=404, detail="District statistics not found")
        
        return DistrictStatsResponse(**stats)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching stats for district {district_code}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch district statistics")

@app.post("/compare")
async def compare_districts(
    comparison: ComparisonRequest,
    db: Session = Depends(get_db)
) -> ComparisonResponse:
    """Compare MGNREGA data between multiple districts"""
    try:
        if len(comparison.district_codes) < 2:
            raise HTTPException(status_code=400, detail="At least 2 districts required for comparison")
        
        if len(comparison.district_codes) > 5:
            raise HTTPException(status_code=400, detail="Maximum 5 districts allowed for comparison")
        
        comparison_data = await mgnrega_service.compare_districts(
            comparison.district_codes, 
            comparison.year,
            db
        )
        
        return ComparisonResponse(**comparison_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing districts: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to compare districts")

@app.get("/refresh-data")
async def refresh_data(db: Session = Depends(get_db)):
    """Manually trigger data refresh from APIs"""
    try:
        await mgnrega_service.refresh_all_data(db)
        return {"message": "Data refresh initiated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing data: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to refresh data")

@app.get("/cache-status")
async def get_cache_status(db: Session = Depends(get_db)):
    """Get cache status and last update times"""
    try:
        status = await mgnrega_service.get_cache_status(db)
        return status
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cache status: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to get cache status")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
