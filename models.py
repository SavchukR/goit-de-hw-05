from dataclasses import dataclass
from typing import Optional
import time

@dataclass
class SensorData:
    timestamp: time
    sensorId: str
    msgCode: Optional[str] = None
    temperature: Optional[int] = None
    humidity: Optional[int] = None
