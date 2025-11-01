from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean
from sqlalchemy.sql import func
from database import Base

class DistrictData(Base):
    """Model for storing MGNREGA district data"""
    __tablename__ = "district_data"
    
    id = Column(Integer, primary_key=True, index=True)
    district_code = Column(String(50), index=True, nullable=False)
    district_name = Column(String(100), nullable=False)
    state_name = Column(String(100), nullable=False)
    year = Column(Integer, nullable=False)
    
    # MGNREGA metrics
    total_job_cards = Column(Integer, default=0)
    active_job_cards = Column(Integer, default=0)
    total_workers = Column(Integer, default=0)
    active_workers = Column(Integer, default=0)
    
    # Work and employment data
    total_person_days = Column(Float, default=0.0)
    average_days_per_household = Column(Float, default=0.0)
    households_completed_100_days = Column(Integer, default=0)
    
    # Financial data
    total_expenditure = Column(Float, default=0.0)
    wage_expenditure = Column(Float, default=0.0)
    material_expenditure = Column(Float, default=0.0)
    average_wage_rate = Column(Float, default=0.0)
    
    # Works data
    total_works = Column(Integer, default=0)
    completed_works = Column(Integer, default=0)
    ongoing_works = Column(Integer, default=0)
    
    # Performance indicators
    employment_provided_percentage = Column(Float, default=0.0)
    timely_payment_percentage = Column(Float, default=0.0)
    
    # Metadata
    data_source = Column(String(100), default="data.gov.in")
    last_updated = Column(DateTime(timezone=True), server_default=func.now())
    is_cached = Column(Boolean, default=True)

class DistrictStats(Base):
    """Model for storing aggregated district statistics"""
    __tablename__ = "district_stats"
    
    id = Column(Integer, primary_key=True, index=True)
    district_code = Column(String(50), index=True, nullable=False)
    district_name = Column(String(100), nullable=False)
    state_name = Column(String(100), nullable=False)
    
    # Key performance indicators
    performance_score = Column(Float, default=0.0)
    employment_rank = Column(Integer, default=0)
    expenditure_rank = Column(Integer, default=0)
    
    # Trend data (year-over-year changes)
    employment_trend = Column(Float, default=0.0)  # Percentage change
    expenditure_trend = Column(Float, default=0.0)  # Percentage change
    
    # Comparative metrics
    state_average_comparison = Column(Float, default=0.0)  # How district compares to state average
    national_average_comparison = Column(Float, default=0.0)  # How district compares to national average
    
    # Summary statistics
    total_beneficiaries = Column(Integer, default=0)
    total_investment = Column(Float, default=0.0)
    
    # Metadata
    calculation_date = Column(DateTime(timezone=True), server_default=func.now())
    last_updated = Column(DateTime(timezone=True), server_default=func.now())

class LocationData(Base):
    """Model for storing district location and boundary data"""
    __tablename__ = "location_data"
    
    id = Column(Integer, primary_key=True, index=True)
    district_code = Column(String(50), unique=True, index=True, nullable=False)
    district_name = Column(String(100), nullable=False)
    state_name = Column(String(100), nullable=False)
    state_code = Column(String(10), nullable=False)
    
    # Geographic data
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    boundary_data = Column(Text, nullable=True)  # GeoJSON format
    
    # Administrative data
    district_headquarters = Column(String(100), nullable=True)
    area_sq_km = Column(Float, nullable=True)
    population = Column(Integer, nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

class CacheStatus(Base):
    """Model for tracking cache status and data freshness"""
    __tablename__ = "cache_status"
    
    id = Column(Integer, primary_key=True, index=True)
    data_type = Column(String(50), nullable=False)  # 'district_data', 'location_data', etc.
    last_api_fetch = Column(DateTime(timezone=True), nullable=True)
    last_successful_fetch = Column(DateTime(timezone=True), nullable=True)
    total_records = Column(Integer, default=0)
    failed_attempts = Column(Integer, default=0)
    is_stale = Column(Boolean, default=False)
    
    # API status
    api_status = Column(String(20), default="unknown")  # 'active', 'down', 'limited'
    error_message = Column(Text, nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())
