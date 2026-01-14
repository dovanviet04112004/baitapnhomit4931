"""
FastAPI Backend for Crypto Analytics Dashboard
Connects to PostgreSQL and serves metrics data
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'crypto_analytics'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}

# Initialize FastAPI
app = FastAPI(
    title="Crypto Analytics API",
    description="API for crypto batch data visualization",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic Models
class DailyMetric(BaseModel):
    date: str
    coin_id: str
    symbol: str
    name: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    return_pct_day: float
    volatility_day: float
    volume_sum_day: float
    market_cap_close: Optional[float] = None
    rank_close: Optional[int] = None

class WeeklyMetric(BaseModel):
    week_of_year: int
    week_start_date: str
    week_end_date: str
    coin_id: str
    symbol: str
    name: str
    open_price_week: float
    close_price_week: float
    high_price_week: float
    low_price_week: float
    return_pct_week: float
    volatility_week: float
    volume_sum_week: float

class MonthlyMetric(BaseModel):
    month: int
    month_start_date: str
    month_end_date: str
    coin_id: str
    symbol: str
    name: str
    open_price_month: float
    close_price_month: float
    high_price_month: float
    low_price_month: float
    return_pct_month: float
    volatility_month: float
    volume_sum_month: float

# Database connection helper
def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# Routes
@app.get("/")
async def serve_index():
    """Serve the main HTML page"""
    return FileResponse("index.html")

@app.get("/app.js")
async def serve_js():
    """Serve JavaScript file"""
    headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    }
    return FileResponse("app.js", media_type="application/javascript", headers=headers)

@app.get("/styles.css")
async def serve_css():
    """Serve CSS file"""
    headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0"
    }
    return FileResponse("styles.css", media_type="text/css", headers=headers)

@app.get("/api")
async def api_root():
    """API root endpoint"""
    return {
        "message": "Crypto Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "daily": "/api/daily-metrics",
            "weekly": "/api/weekly-metrics",
            "monthly": "/api/monthly-metrics",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/api/daily-metrics", response_model=List[DailyMetric])
async def get_daily_metrics(
    limit: int = 100,
    date: Optional[str] = None
):
    """
    Get daily metrics
    
    Parameters:
    - limit: Number of records to return (default: 100)
    - date: Specific date to filter (format: YYYY-MM-DD)
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        # Build query
        query = """
            SELECT 
                date::text,
                coin_id,
                symbol,
                name,
                open_price,
                close_price,
                high_price,
                low_price,
                return_pct_day,
                volatility_day,
                volume_sum_day,
                market_cap_close,
                rank_close
            FROM daily_metrics
        """
        
        params = []
        if date:
            query += " WHERE date = %s"
            params.append(date)
        else:
            # Get latest date if no date specified
            query += " WHERE date = (SELECT MAX(date) FROM daily_metrics)"
        
        query += " ORDER BY return_pct_day DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.get("/api/weekly-metrics", response_model=List[WeeklyMetric])
async def get_weekly_metrics(
    limit: int = 100,
    year: Optional[int] = None,
    week: Optional[int] = None
):
    """
    Get weekly metrics
    
    Parameters:
    - limit: Number of records to return (default: 100)
    - year: Filter by year
    - week: Filter by week number
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        query = """
            SELECT 
                week_of_year,
                week_start_date,
                week_end_date,
                coin_id,
                symbol,
                name,
                open_price_week,
                close_price_week,
                high_price_week,
                low_price_week,
                return_pct_week,
                volatility_week,
                volume_sum_week
            FROM weekly_metrics
        """
        
        params = []
        conditions = []
        
        if week:
            conditions.append("week_of_year = %s")
            params.append(week)
        elif not week:
            # Get latest week based on week_start_date
            conditions.append("""
                week_start_date = (
                    SELECT MAX(week_start_date) 
                    FROM weekly_metrics
                )
            """)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY return_pct_week DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

@app.get("/api/monthly-metrics", response_model=List[MonthlyMetric])
async def get_monthly_metrics(
    limit: int = 100,
    year: Optional[int] = None,
    month: Optional[int] = None
):
    """
    Get monthly metrics
    
    Parameters:
    - limit: Number of records to return (default: 100)
    - year: Filter by year
    - month: Filter by month (1-12)
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        query = """
            SELECT 
                month,
                month_start_date,
                month_end_date,
                coin_id,
                symbol,
                name,
                open_price_month,
                close_price_month,
                high_price_month,
                low_price_month,
                return_pct_month,
                volatility_month,
                volume_sum_month
            FROM monthly_metrics
        """
        
        params = []
        conditions = []
        
        if month:
            conditions.append("month = %s")
            params.append(month)
        elif not month:
            # Get latest month based on month_start_date
            conditions.append("""
                month_start_date = (
                    SELECT MAX(month_start_date) 
                    FROM monthly_metrics
                )
            """)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY return_pct_month DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()

# Run server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
