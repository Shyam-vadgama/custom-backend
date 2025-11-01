import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import requests
from sqlalchemy.orm import Session
from sqlalchemy import desc, and_
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from models import DistrictData, DistrictStats, CacheStatus
from services.data_gov_client import DataGovClient

logger = logging.getLogger(__name__)

class MGNREGAService:
    """Service for handling MGNREGA data operations"""
    
    def __init__(self):
        self.data_client = DataGovClient()
        self.scheduler = AsyncIOScheduler()
        self.cache_duration_hours = 24  # Cache data for 24 hours
        
    async def start_scheduler(self):
        """Start the background scheduler for data updates"""
        # Schedule data refresh every 6 hours
        self.scheduler.add_job(
            self._scheduled_data_refresh,
            'interval',
            hours=6,
            id='data_refresh',
            replace_existing=True
        )
        
        # Schedule cache cleanup daily
        self.scheduler.add_job(
            self._cleanup_old_cache,
            'cron',
            hour=2,  # Run at 2 AM
            id='cache_cleanup',
            replace_existing=True
        )
        
        self.scheduler.start()
        logger.info("MGNREGA service scheduler started")
    
    async def stop_scheduler(self):
        """Stop the background scheduler"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("MGNREGA service scheduler stopped")
    
    async def get_district_data(
        self, 
        district_code: str, 
        year: Optional[int] = None,
        db: Session = None
    ) -> Optional[Dict[str, Any]]:
        """Get MGNREGA data for a specific district"""
        try:
            current_year = year or datetime.now().year
            
            # Try to get from cache first
            cached_data = self._get_cached_district_data(db, district_code, current_year)
            
            if cached_data and not self._is_cache_stale(cached_data.last_updated):
                logger.info(f"Returning cached data for district {district_code}")
                return self._format_district_data(cached_data)
            
            # Fetch fresh data from API
            logger.info(f"Fetching fresh data for district {district_code}")
            fresh_data = await self.data_client.get_district_mgnrega_data(district_code, current_year)
            
            if fresh_data:
                # Save to cache and return formatted payload including metadata
                return self._save_district_data_to_cache(db, fresh_data, district_code, current_year)
            
            # If API fails, return cached data even if stale
            if cached_data:
                logger.warning(f"API failed, returning stale cached data for district {district_code}")
                return self._format_district_data(cached_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting district data for {district_code}: {str(e)}")
            return None
    
    async def get_district_stats(
        self, 
        district_code: str,
        db: Session = None
    ) -> Optional[Dict[str, Any]]:
        """Get aggregated statistics for a district"""
        try:
            # Try cache first
            cached_stats = db.query(DistrictStats).filter(
                DistrictStats.district_code == district_code
            ).first()
            
            if cached_stats and not self._is_cache_stale(cached_stats.last_updated):
                return self._format_district_stats(cached_stats)
            
            # Calculate fresh stats
            stats = await self._calculate_district_stats(district_code, db)
            
            if stats:
                # Save to cache
                self._save_district_stats_to_cache(db, stats, district_code)
                return stats
            
            # Return cached stats if available
            if cached_stats:
                return self._format_district_stats(cached_stats)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting district stats for {district_code}: {str(e)}")
            return None
    
    async def compare_districts(
        self,
        district_codes: List[str],
        year: Optional[int] = None,
        db: Session = None
    ) -> Dict[str, Any]:
        """Compare MGNREGA data between multiple districts"""
        try:
            current_year = year or datetime.now().year
            comparison_data = {
                "districts": [],
                "year": current_year,
                "metrics": [],
                "summary": {},
                "generated_at": datetime.now()
            }
            
            # Get data for all districts
            district_data = {}
            for district_code in district_codes:
                data = await self.get_district_data(district_code, current_year, db)
                if data:
                    district_data[district_code] = data
                    comparison_data["districts"].append({
                        "district_code": district_code,
                        "district_name": data["district_name"],
                        "state_name": data["state_name"]
                    })
            
            if not district_data:
                return comparison_data
            
            # Define comparison metrics
            metrics_config = [
                {
                    "name": "total_person_days",
                    "label": "Total Person Days",
                    "unit": "days",
                    "description": "Total person days of employment generated"
                },
                {
                    "name": "total_expenditure",
                    "label": "Total Expenditure",
                    "unit": "₹",
                    "description": "Total expenditure on MGNREGA works"
                },
                {
                    "name": "active_workers",
                    "label": "Active Workers",
                    "unit": "count",
                    "description": "Number of active workers"
                },
                {
                    "name": "average_days_per_household",
                    "label": "Avg Days per Household",
                    "unit": "days",
                    "description": "Average employment days per household"
                },
                {
                    "name": "employment_provided_percentage",
                    "label": "Employment Provided",
                    "unit": "%",
                    "description": "Percentage of employment provided"
                }
            ]
            
            # Calculate comparison metrics
            for metric_config in metrics_config:
                metric_name = metric_config["name"]
                values = {}
                
                for district_code, data in district_data.items():
                    values[district_code] = data.get(metric_name, 0)
                
                comparison_data["metrics"].append({
                    "metric_name": metric_name,
                    "metric_label": metric_config["label"],
                    "values": values,
                    "unit": metric_config["unit"],
                    "description": metric_config["description"]
                })
            
            # Generate summary
            comparison_data["summary"] = self._generate_comparison_summary(district_data)
            
            return comparison_data
            
        except Exception as e:
            logger.error(f"Error comparing districts: {str(e)}")
            raise
    
    async def refresh_all_data(self, db: Session):
        """Manually refresh all cached data"""
        try:
            logger.info("Starting manual data refresh")
            
            # Get all unique district codes from cache
            district_codes = db.query(DistrictData.district_code).distinct().all()
            district_codes = [code[0] for code in district_codes]
            
            current_year = datetime.now().year
            success_count = 0
            
            for district_code in district_codes:
                try:
                    fresh_data = await self.data_client.get_district_mgnrega_data(district_code, current_year)
                    if fresh_data:
                        self._save_district_data_to_cache(db, fresh_data, district_code, current_year)
                        success_count += 1
                except Exception as e:
                    logger.error(f"Failed to refresh data for district {district_code}: {str(e)}")
            
            # Update cache status
            self._update_cache_status(db, "district_data", success_count, len(district_codes))
            
            logger.info(f"Data refresh completed. {success_count}/{len(district_codes)} districts updated")
            
        except Exception as e:
            logger.error(f"Error during data refresh: {str(e)}")
            raise
    
    async def get_cache_status(self, db: Session) -> Dict[str, Any]:
        """Get current cache status"""
        try:
            cache_statuses = db.query(CacheStatus).all()
            
            status_data = {
                "data_types": {},
                "overall_status": "healthy",
                "last_refresh": datetime.now(),
                "api_health": {}
            }
            
            for cache_status in cache_statuses:
                status_data["data_types"][cache_status.data_type] = {
                    "last_fetch": cache_status.last_api_fetch,
                    "last_successful_fetch": cache_status.last_successful_fetch,
                    "total_records": cache_status.total_records,
                    "failed_attempts": cache_status.failed_attempts,
                    "is_stale": cache_status.is_stale,
                    "api_status": cache_status.api_status
                }
                
                status_data["api_health"][cache_status.data_type] = cache_status.api_status
            
            # Determine overall status
            if any(status.is_stale for status in cache_statuses):
                status_data["overall_status"] = "degraded"
            
            if any(status.api_status == "down" for status in cache_statuses):
                status_data["overall_status"] = "critical"
            
            return status_data
            
        except Exception as e:
            logger.error(f"Error getting cache status: {str(e)}")
            return {"overall_status": "error", "error": str(e)}
    
    # Private helper methods
    
    def _get_cached_district_data(self, db: Session, district_code: str, year: int):
        """Get cached district data"""
        return db.query(DistrictData).filter(
            and_(
                DistrictData.district_code == district_code,
                DistrictData.year == year
            )
        ).first()
    
    def _is_cache_stale(self, last_updated: datetime) -> bool:
        """Check if cached data is stale"""
        if not last_updated:
            return True
        
        stale_threshold = datetime.now() - timedelta(hours=self.cache_duration_hours)
        return last_updated < stale_threshold
    
    def _format_district_data(self, district_data: DistrictData) -> Dict[str, Any]:
        """Format district data for API response"""
        return {
            "district_code": district_data.district_code,
            "district_name": district_data.district_name,
            "state_name": district_data.state_name,
            "year": district_data.year,
            "total_job_cards": district_data.total_job_cards,
            "active_job_cards": district_data.active_job_cards,
            "total_workers": district_data.total_workers,
            "active_workers": district_data.active_workers,
            "total_person_days": district_data.total_person_days,
            "average_days_per_household": district_data.average_days_per_household,
            "households_completed_100_days": district_data.households_completed_100_days,
            "total_expenditure": district_data.total_expenditure,
            "wage_expenditure": district_data.wage_expenditure,
            "material_expenditure": district_data.material_expenditure,
            "average_wage_rate": district_data.average_wage_rate,
            "total_works": district_data.total_works,
            "completed_works": district_data.completed_works,
            "ongoing_works": district_data.ongoing_works,
            "employment_provided_percentage": district_data.employment_provided_percentage,
            "timely_payment_percentage": district_data.timely_payment_percentage,
            "last_updated": district_data.last_updated,
            "is_cached": district_data.is_cached,
            "data_source": district_data.data_source
        }
    
    def _format_district_stats(self, district_stats: DistrictStats) -> Dict[str, Any]:
        """Format district stats for API response"""
        return {
            "district_code": district_stats.district_code,
            "district_name": district_stats.district_name,
            "state_name": district_stats.state_name,
            "performance_score": district_stats.performance_score,
            "employment_rank": district_stats.employment_rank,
            "expenditure_rank": district_stats.expenditure_rank,
            "employment_trend": district_stats.employment_trend,
            "expenditure_trend": district_stats.expenditure_trend,
            "state_average_comparison": district_stats.state_average_comparison,
            "national_average_comparison": district_stats.national_average_comparison,
            "total_beneficiaries": district_stats.total_beneficiaries,
            "total_investment": district_stats.total_investment,
            "calculation_date": district_stats.calculation_date,
            "last_updated": district_stats.last_updated
        }
    
    def _save_district_data_to_cache(self, db: Session, data: Dict[str, Any], district_code: str, year: int) -> Dict[str, Any]:
        """Save district data to cache and return formatted record"""
        try:
            # Check if record exists
            existing = self._get_cached_district_data(db, district_code, year)
            
            if existing:
                # Update existing record
                for key, value in data.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                existing.last_updated = datetime.now()
                record = existing
            else:
                # Create new record
                record_payload = dict(data)
                record_payload["district_code"] = district_code
                record_payload["year"] = year
                record_payload.setdefault("is_cached", True)
                district_data = DistrictData(**record_payload)
                district_data.last_updated = datetime.now()
                db.add(district_data)
                record = district_data
            
            db.commit()
            db.refresh(record)
            logger.info(f"Saved district data to cache: {district_code}")
            return self._format_district_data(record)
            
        except Exception as e:
            logger.error(f"Error saving district data to cache: {str(e)}")
            db.rollback()
            raise
    
    def _save_district_stats_to_cache(self, db: Session, stats: Dict[str, Any], district_code: str):
        """Save district stats to cache"""
        try:
            existing = db.query(DistrictStats).filter(
                DistrictStats.district_code == district_code
            ).first()
            
            if existing:
                for key, value in stats.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                existing.last_updated = datetime.now()
            else:
                district_stats = DistrictStats(
                    district_code=district_code,
                    last_updated=datetime.now(),
                    **stats
                )
                db.add(district_stats)
            
            db.commit()
            
        except Exception as e:
            logger.error(f"Error saving district stats to cache: {str(e)}")
            db.rollback()
    
    async def _calculate_district_stats(self, district_code: str, db: Session) -> Optional[Dict[str, Any]]:
        """Calculate aggregated statistics for a district"""
        try:
            # Get recent data for the district
            recent_data = db.query(DistrictData).filter(
                DistrictData.district_code == district_code
            ).order_by(desc(DistrictData.year)).limit(3).all()
            
            if not recent_data:
                return None
            
            latest_data = recent_data[0]
            
            # Calculate performance score (simplified)
            performance_score = (
                (latest_data.employment_provided_percentage or 0) * 0.4 +
                (latest_data.timely_payment_percentage or 0) * 0.3 +
                min((latest_data.average_days_per_household or 0) / 100 * 100, 100) * 0.3
            )
            
            # Calculate trends if we have historical data
            employment_trend = 0.0
            expenditure_trend = 0.0
            
            if len(recent_data) >= 2:
                current = recent_data[0]
                previous = recent_data[1]
                
                if previous.total_person_days > 0:
                    employment_trend = ((current.total_person_days - previous.total_person_days) / 
                                     previous.total_person_days) * 100
                
                if previous.total_expenditure > 0:
                    expenditure_trend = ((current.total_expenditure - previous.total_expenditure) / 
                                       previous.total_expenditure) * 100
            
            return {
                "district_name": latest_data.district_name,
                "state_name": latest_data.state_name,
                "performance_score": round(performance_score, 2),
                "employment_rank": 0,  # Would need state/national data to calculate
                "expenditure_rank": 0,  # Would need state/national data to calculate
                "employment_trend": round(employment_trend, 2),
                "expenditure_trend": round(expenditure_trend, 2),
                "state_average_comparison": 0.0,  # Would need state average data
                "national_average_comparison": 0.0,  # Would need national average data
                "total_beneficiaries": latest_data.active_workers or 0,
                "total_investment": latest_data.total_expenditure or 0.0,
                "calculation_date": datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Error calculating district stats: {str(e)}")
            return None
    
    def _generate_comparison_summary(self, district_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary for district comparison"""
        try:
            if not district_data:
                return {}
            
            # Find best and worst performing districts
            employment_data = {code: data.get("total_person_days", 0) for code, data in district_data.items()}
            expenditure_data = {code: data.get("total_expenditure", 0) for code, data in district_data.items()}
            
            best_employment = max(employment_data, key=employment_data.get)
            worst_employment = min(employment_data, key=employment_data.get)
            
            best_expenditure = max(expenditure_data, key=expenditure_data.get)
            worst_expenditure = min(expenditure_data, key=expenditure_data.get)
            
            return {
                "best_employment_district": {
                    "district_code": best_employment,
                    "district_name": district_data[best_employment]["district_name"],
                    "value": employment_data[best_employment]
                },
                "worst_employment_district": {
                    "district_code": worst_employment,
                    "district_name": district_data[worst_employment]["district_name"],
                    "value": employment_data[worst_employment]
                },
                "highest_expenditure_district": {
                    "district_code": best_expenditure,
                    "district_name": district_data[best_expenditure]["district_name"],
                    "value": expenditure_data[best_expenditure]
                },
                "total_districts_compared": len(district_data),
                "comparison_year": district_data[list(district_data.keys())[0]]["year"]
            }
            
        except Exception as e:
            logger.error(f"Error generating comparison summary: {str(e)}")
            return {}
    
    def _update_cache_status(self, db: Session, data_type: str, success_count: int, total_count: int):
        """Update cache status after data refresh"""
        try:
            cache_status = db.query(CacheStatus).filter(
                CacheStatus.data_type == data_type
            ).first()
            
            if not cache_status:
                cache_status = CacheStatus(data_type=data_type)
                db.add(cache_status)
            
            cache_status.last_api_fetch = datetime.now()
            cache_status.total_records = success_count
            
            if success_count > 0:
                cache_status.last_successful_fetch = datetime.now()
                cache_status.api_status = "active"
                cache_status.is_stale = False
                cache_status.failed_attempts = 0
            else:
                cache_status.failed_attempts += 1
                if cache_status.failed_attempts >= 3:
                    cache_status.api_status = "down"
                    cache_status.is_stale = True
            
            cache_status.updated_at = datetime.now()
            db.commit()
            
        except Exception as e:
            logger.error(f"Error updating cache status: {str(e)}")
            db.rollback()
    
    async def _scheduled_data_refresh(self):
        """Scheduled background data refresh"""
        logger.info("Starting scheduled data refresh")
        # This would need a database session - implement based on your session management
        pass
    
    async def _cleanup_old_cache(self):
        """Clean up old cached data"""
        logger.info("Starting cache cleanup")
        # Implement cache cleanup logic
        pass
