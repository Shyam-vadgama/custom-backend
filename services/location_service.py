import logging
from typing import Dict, List, Optional, Tuple
import requests
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import json

logger = logging.getLogger(__name__)

class LocationService:
    """Service for handling location-based operations"""
    
    def __init__(self):
        self.geocoder = Nominatim(user_agent="mgnrega-app")
        self.district_cache = {}  # Simple in-memory cache for district data
        
        # Load Indian district data (simplified - in production, use a proper database)
        self.indian_districts = self._load_indian_districts()
    
    async def get_district_from_coordinates(
        self, 
        latitude: float, 
        longitude: float
    ) -> Optional[Dict[str, str]]:
        """Get district information from GPS coordinates"""
        try:
            # Create cache key
            cache_key = f"{latitude:.4f},{longitude:.4f}"
            
            # Check cache first
            if cache_key in self.district_cache:
                logger.info(f"Returning cached district for coordinates {latitude}, {longitude}")
                return self.district_cache[cache_key]
            
            # Try reverse geocoding with Nominatim
            location = self.geocoder.reverse(f"{latitude}, {longitude}", language='en')
            
            if location and location.raw:
                address = location.raw.get('address', {})
                
                # Extract district and state from address
                district = self._extract_district_from_address(address)
                state = self._extract_state_from_address(address)
                
                if district and state:
                    # Find district code
                    district_code = self._find_district_code(district, state)
                    
                    result = {
                        "district": district,
                        "state": state,
                        "district_code": district_code,
                        "full_address": location.address
                    }
                    
                    # Cache the result
                    self.district_cache[cache_key] = result
                    
                    logger.info(f"Found district {district}, {state} for coordinates {latitude}, {longitude}")
                    return result
            
            # Fallback: Find nearest district using distance calculation
            logger.info("Reverse geocoding failed, trying nearest district approach")
            nearest_district = self._find_nearest_district(latitude, longitude)
            
            if nearest_district:
                self.district_cache[cache_key] = nearest_district
                return nearest_district
            
            logger.warning(f"Could not determine district for coordinates {latitude}, {longitude}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting district from coordinates: {str(e)}")
            return None
    
    async def get_districts_by_state(self, state: str) -> List[Dict[str, str]]:
        """Get all districts in a given state"""
        try:
            state_districts = []
            
            for district_info in self.indian_districts:
                if district_info["state"].lower() == state.lower():
                    state_districts.append({
                        "district_code": district_info["district_code"],
                        "district_name": district_info["district"],
                        "state_name": district_info["state"],
                        "latitude": district_info.get("latitude"),
                        "longitude": district_info.get("longitude")
                    })
            
            return sorted(state_districts, key=lambda x: x["district_name"])
            
        except Exception as e:
            logger.error(f"Error getting districts for state {state}: {str(e)}")
            return []
    
    async def validate_coordinates(self, latitude: float, longitude: float) -> bool:
        """Validate if coordinates are within India's boundaries"""
        try:
            # India's approximate bounding box
            # Latitude: 6.0 to 37.6 degrees North
            # Longitude: 68.0 to 97.25 degrees East
            
            if not (6.0 <= latitude <= 37.6):
                return False
            
            if not (68.0 <= longitude <= 97.25):
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating coordinates: {str(e)}")
            return False
    
    def _extract_district_from_address(self, address: Dict) -> Optional[str]:
        """Extract district name from geocoded address"""
        # Try different possible keys for district
        district_keys = [
            'state_district', 'district', 'county', 
            'administrative_area_level_2', 'suburb'
        ]
        
        for key in district_keys:
            if key in address and address[key]:
                return address[key]
        
        return None
    
    def _extract_state_from_address(self, address: Dict) -> Optional[str]:
        """Extract state name from geocoded address"""
        # Try different possible keys for state
        state_keys = [
            'state', 'administrative_area_level_1', 'region'
        ]
        
        for key in state_keys:
            if key in address and address[key]:
                return address[key]
        
        return None
    
    def _find_district_code(self, district: str, state: str) -> Optional[str]:
        """Find district code for given district and state"""
        try:
            district_lower = district.lower()
            state_lower = state.lower()
            
            for district_info in self.indian_districts:
                if (district_info["district"].lower() == district_lower and 
                    district_info["state"].lower() == state_lower):
                    return district_info["district_code"]
            
            # Try partial matching
            for district_info in self.indian_districts:
                if (district_lower in district_info["district"].lower() and 
                    state_lower in district_info["state"].lower()):
                    return district_info["district_code"]
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding district code: {str(e)}")
            return None
    
    def _find_nearest_district(self, latitude: float, longitude: float) -> Optional[Dict[str, str]]:
        """Find nearest district using distance calculation"""
        try:
            user_location = (latitude, longitude)
            min_distance = float('inf')
            nearest_district = None
            
            for district_info in self.indian_districts:
                if district_info.get("latitude") and district_info.get("longitude"):
                    district_location = (district_info["latitude"], district_info["longitude"])
                    distance = geodesic(user_location, district_location).kilometers
                    
                    if distance < min_distance:
                        min_distance = distance
                        nearest_district = district_info
            
            if nearest_district and min_distance < 100:  # Within 100km
                return {
                    "district": nearest_district["district"],
                    "state": nearest_district["state"],
                    "district_code": nearest_district["district_code"],
                    "distance_km": round(min_distance, 2)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding nearest district: {str(e)}")
            return None
    
    def _load_indian_districts(self) -> List[Dict[str, str]]:
        """Load Indian districts data (simplified version)"""
        # In a real application, this would load from a comprehensive database
        # This is a simplified sample for demonstration
        return [
            {
                "district_code": "AP001",
                "district": "Anantapur",
                "state": "Andhra Pradesh",
                "latitude": 14.6819,
                "longitude": 77.6006
            },
            {
                "district_code": "AP002", 
                "district": "Chittoor",
                "state": "Andhra Pradesh",
                "latitude": 13.2172,
                "longitude": 79.1003
            },
            {
                "district_code": "AP003",
                "district": "Visakhapatnam",
                "state": "Andhra Pradesh",
                "latitude": 17.6868,
                "longitude": 83.2185
            },
            {
                "district_code": "AS001",
                "district": "Kamrup",
                "state": "Assam",
                "latitude": 26.1445,
                "longitude": 91.7362
            },
            {
                "district_code": "AS002",
                "district": "Dibrugarh",
                "state": "Assam",
                "latitude": 27.4728,
                "longitude": 94.9120
            },
            {
                "district_code": "BR001",
                "district": "Patna",
                "state": "Bihar",
                "latitude": 25.5941,
                "longitude": 85.1376
            },
            {
                "district_code": "BR002",
                "district": "Gaya",
                "state": "Bihar", 
                "latitude": 24.7914,
                "longitude": 85.0002
            },
            {
                "district_code": "CG001",
                "district": "Raipur",
                "state": "Chhattisgarh",
                "latitude": 21.2514,
                "longitude": 81.6296
            },
            {
                "district_code": "CG002",
                "district": "Bilaspur",
                "state": "Chhattisgarh",
                "latitude": 22.0797,
                "longitude": 82.1391
            },
            {
                "district_code": "DL001",
                "district": "Central Delhi",
                "state": "Delhi",
                "latitude": 28.6519,
                "longitude": 77.2315
            },
            {
                "district_code": "GJ001",
                "district": "Ahmedabad",
                "state": "Gujarat",
                "latitude": 23.0225,
                "longitude": 72.5714
            },
            {
                "district_code": "GJ002",
                "district": "Surat",
                "state": "Gujarat",
                "latitude": 21.1702,
                "longitude": 72.8311
            },
            {
                "district_code": "GJ003",
                "district": "Jamnagar",
                "state": "Gujarat",
                "latitude": 22.4707,
                "longitude": 70.0577
            },
            {
                "district_code": "HR001",
                "district": "Gurgaon",
                "state": "Haryana",
                "latitude": 28.4595,
                "longitude": 77.0266
            },
            {
                "district_code": "HR002",
                "district": "Faridabad",
                "state": "Haryana",
                "latitude": 28.4089,
                "longitude": 77.3178
            },
            {
                "district_code": "HP001",
                "district": "Shimla",
                "state": "Himachal Pradesh",
                "latitude": 31.1048,
                "longitude": 77.1734
            },
            {
                "district_code": "HP002",
                "district": "Kangra",
                "state": "Himachal Pradesh",
                "latitude": 32.0998,
                "longitude": 76.2691
            },
            {
                "district_code": "JH001",
                "district": "Ranchi",
                "state": "Jharkhand",
                "latitude": 23.3441,
                "longitude": 85.3096
            },
            {
                "district_code": "JH002",
                "district": "Dhanbad",
                "state": "Jharkhand",
                "latitude": 23.7957,
                "longitude": 86.4304
            },
            {
                "district_code": "KA001",
                "district": "Bangalore Urban",
                "state": "Karnataka",
                "latitude": 12.9716,
                "longitude": 77.5946
            },
            {
                "district_code": "KA002",
                "district": "Mysuru",
                "state": "Karnataka",
                "latitude": 12.2958,
                "longitude": 76.6394
            },
            {
                "district_code": "KL001",
                "district": "Thiruvananthapuram",
                "state": "Kerala",
                "latitude": 8.5241,
                "longitude": 76.9366
            },
            {
                "district_code": "KL002",
                "district": "Kozhikode",
                "state": "Kerala",
                "latitude": 11.2588,
                "longitude": 75.7804
            },
            {
                "district_code": "MP001",
                "district": "Bhopal",
                "state": "Madhya Pradesh",
                "latitude": 23.2599,
                "longitude": 77.4126
            },
            {
                "district_code": "MP002",
                "district": "Indore",
                "state": "Madhya Pradesh",
                "latitude": 22.7196,
                "longitude": 75.8577
            },
            {
                "district_code": "MH001",
                "district": "Mumbai",
                "state": "Maharashtra",
                "latitude": 19.0760,
                "longitude": 72.8777
            },
            {
                "district_code": "MH002",
                "district": "Pune",
                "state": "Maharashtra",
                "latitude": 18.5204,
                "longitude": 73.8567
            },
            {
                "district_code": "MH003",
                "district": "Nagpur",
                "state": "Maharashtra",
                "latitude": 21.1458,
                "longitude": 79.0882
            },
            {
                "district_code": "MH004",
                "district": "Nashik",
                "state": "Maharashtra",
                "latitude": 19.9975,
                "longitude": 73.7898
            },
            {
                "district_code": "OR001",
                "district": "Khordha",
                "state": "Odisha",
                "latitude": 20.2961,
                "longitude": 85.8245
            },
            {
                "district_code": "OR002",
                "district": "Sambalpur",
                "state": "Odisha",
                "latitude": 21.4669,
                "longitude": 83.9812
            },
            {
                "district_code": "PB001",
                "district": "Ludhiana",
                "state": "Punjab",
                "latitude": 30.9010,
                "longitude": 75.8573
            },
            {
                "district_code": "PB002",
                "district": "Amritsar",
                "state": "Punjab",
                "latitude": 31.6340,
                "longitude": 74.8723
            },
            {
                "district_code": "RJ001",
                "district": "Jaipur",
                "state": "Rajasthan",
                "latitude": 26.9124,
                "longitude": 75.7873
            },
            {
                "district_code": "RJ002",
                "district": "Jodhpur",
                "state": "Rajasthan",
                "latitude": 26.2389,
                "longitude": 73.0243
            },
            {
                "district_code": "TN001",
                "district": "Chennai",
                "state": "Tamil Nadu",
                "latitude": 13.0827,
                "longitude": 80.2707
            },
            {
                "district_code": "TN002",
                "district": "Coimbatore",
                "state": "Tamil Nadu",
                "latitude": 11.0168,
                "longitude": 76.9558
            },
            {
                "district_code": "TG001",
                "district": "Hyderabad",
                "state": "Telangana",
                "latitude": 17.3850,
                "longitude": 78.4867
            },
            {
                "district_code": "TG002",
                "district": "Warangal",
                "state": "Telangana",
                "latitude": 17.9689,
                "longitude": 79.5941
            },
            {
                "district_code": "UP001",
                "district": "Lucknow",
                "state": "Uttar Pradesh",
                "latitude": 26.8467,
                "longitude": 80.9462
            },
            {
                "district_code": "UP002",
                "district": "Kanpur Nagar",
                "state": "Uttar Pradesh",
                "latitude": 26.4499,
                "longitude": 80.3319
            },
            {
                "district_code": "UP003",
                "district": "Varanasi",
                "state": "Uttar Pradesh",
                "latitude": 25.3176,
                "longitude": 82.9739
            },
            {
                "district_code": "WB001",
                "district": "Kolkata",
                "state": "West Bengal",
                "latitude": 22.5726,
                "longitude": 88.3639
            },
            {
                "district_code": "WB002",
                "district": "Darjeeling",
                "state": "West Bengal",
                "latitude": 27.0360,
                "longitude": 88.2627
            }
        ]
