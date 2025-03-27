from pydantic import BaseModel, Field
from typing import Optional



class Currency(BaseModel):
    code: str = Field(...)
    name: str = Field(...)
    symbol: Optional[str]

class ExchangeRate(BaseModel):
    base_currency: str = Field(...)
    target_currency: str = Field(...)
    exchange_rate: float = Field(..., gt=0)