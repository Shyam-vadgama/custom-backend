from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

class LocationRequest(BaseModel):
    latitude: float = Field(..., ge=-90, le=90, description="Latitude coordinate")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude coordinate")

class DistrictInfo(BaseModel):
    district_code: str
    district_name: str
    state_name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class DistrictDataResponse(BaseModel):
    district_code: str
    district_name: str
    state_name: str
    year: int
    
    # Employment metrics
    total_job_cards: int
    active_job_cards: int
    total_workers: int
    active_workers: int
    
    # Work metrics
    total_person_days: float
    average_days_per_household: float
    households_completed_100_days: int
    
    # Financial metrics
    total_expenditure: float
    wage_expenditure: float
    material_expenditure: float
    average_wage_rate: float
    
    # Works metrics
    total_works: int
    completed_works: int
    ongoing_works: int
    
    # Performance metrics
    employment_provided_percentage: float
    timely_payment_percentage: float
    
    # Metadata
    last_updated: datetime
    is_cached: bool
    data_source: str

class DistrictStatsResponse(BaseModel):
    district_code: str
    district_name: str
    state_name: str
    
    # Key indicators
    performance_score: float
    employment_rank: int
    expenditure_rank: int
    
    # Trends
    employment_trend: float
    expenditure_trend: float
    
    # Comparisons
    state_average_comparison: float
    national_average_comparison: float
    
    # Summary
    total_beneficiaries: int
    total_investment: float
    
    # Metadata
    calculation_date: datetime
    last_updated: datetime

class ComparisonRequest(BaseModel):
    district_codes: List[str] = Field(..., min_items=2, max_items=5)
    year: Optional[int] = None
    metrics: Optional[List[str]] = None  # Specific metrics to compare

class ComparisonMetric(BaseModel):
    metric_name: str
    metric_label: str
    values: Dict[str, float]  # district_code -> value
    unit: str
    description: str

class ComparisonResponse(BaseModel):
    districts: List[DistrictInfo]
    year: int
    metrics: List[ComparisonMetric]
    summary: Dict[str, Any]
    generated_at: datetime

class VoiceNarrationRequest(BaseModel):
    district_code: str
    language: str = Field(default="en", pattern="^(en|hi)$")
    include_comparison: bool = False
    comparison_districts: Optional[List[str]] = None

class VoiceNarrationResponse(BaseModel):
    text: str
    language: str
    audio_url: Optional[str] = None
    duration_seconds: Optional[float] = None

class CacheStatusResponse(BaseModel):
    data_types: Dict[str, Dict[str, Any]]
    overall_status: str
    last_refresh: datetime
    next_scheduled_refresh: Optional[datetime] = None
    api_health: Dict[str, str]

class ErrorResponse(BaseModel):
    error: str
    message: str
    timestamp: datetime
    request_id: Optional[str] = None

# Language support schemas
class LanguageContent(BaseModel):
    en: str
    hi: str

class MultiLanguageResponse(BaseModel):
    content: LanguageContent
    current_language: str = "en"

# GPS and location schemas
class GPSCoordinates(BaseModel):
    latitude: float
    longitude: float
    accuracy: Optional[float] = None
    timestamp: Optional[datetime] = None

class LocationDetectionResponse(BaseModel):
    district: str
    state: str
    district_code: Optional[str] = None
    coordinates: GPSCoordinates
    confidence_score: Optional[float] = None
    alternative_matches: Optional[List[DistrictInfo]] = None
