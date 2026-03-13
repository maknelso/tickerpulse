from pydantic import BaseModel, Field
from datetime import datetime

"""
Contract-Driven Development and Schema Validation using Pydantic. 
This shows you're thinking about "System Robustness," not just "making it work."



"""
class StockTick(BaseModel):
    # Field validation: symbol must be a string, price must be positive
    symbol: str
    price: float = Field(gt=0, description="The trade price must be greater than zero")
    timestamp: datetime