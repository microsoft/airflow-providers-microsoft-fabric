from __future__ import annotations

# Back-compat alias: expose the new class under the old name
from .job import MSFabricRunJobOperator as MSFabricRunItemOperator

# Re-export the canonical names too
from .base import MSFabricItemLink
from .job import MSFabricRunJobOperator
from .user_data_function import MSFabricRunUserDataFunctionOperator
from .semantic_model_refresh import MSFabricRunSemanticModelRefreshOperator


__all__ = [
    "MSFabricItemLink",
    "MSFabricRunItemOperator", # alias for back-compat
    "MSFabricRunJobOperator",
    "MSFabricRunUserDataFunctionOperator",
    "MSFabricRunSemanticModelRefreshOperator",
]
