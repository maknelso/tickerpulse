"""
Contract-Driven Development and Schema Validation using Pydantic. 
This shows you're thinking about "System Robustness," not just "making it work."
"""

from pydantic import BaseModel, Field
from datetime import datetime

# Field is when you want more control over a field

# now StockTicker is a data model with validation
class StockTicker(BaseModel):
    ticker: str = Field(..., description="The stock symbol, e.g. AAPL")
    price: float = Field(gt=0.0, description="The current price, must be positive")
    timestamp: datetime = Field(default_factory=datetime.now)


