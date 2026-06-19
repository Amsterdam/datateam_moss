from typing import *
from datetime import datetime
from dataclasses import dataclass

@dataclass
class TransformContext:
    """Context object die de configuratie bevat voor datatransformaties"""

    table_schema: Dict[str, Any]
    m_columns: Optional[List[str]] = None
    runtime: Optional[datetime] = None
    rename_columns: Optional[List[Dict[str, str]]] = None
    date_output_format: Optional[str] = None
    translate_comma_to_dot: bool = False