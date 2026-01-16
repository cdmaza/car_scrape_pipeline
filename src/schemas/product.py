from pydantic import BaseModel, field_validator
from datetime import datetime
from typing import Optional

class ProductSchema(BaseModel):
    product_name: str
    price_string: str  # We keep it as string here; Spark will clean it later
    scraped_at: datetime

    @field_validator('product_name')
    def name_must_not_be_empty(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('Product name is empty')
        return v