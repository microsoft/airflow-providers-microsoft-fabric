from __future__ import annotations

from .base import MSFabricItemLink
from .job import MSFabricRunJobOperator
from .user_data_function import MSFabricRunUserDataFunctionOperator
from .semantic_model_refresh import MSFabricRunSemanticModelRefreshOperator

# Parameter helper classes
from .notebook_parameters import MSFabricNotebookJobParameters
from .pipeline_parameters import MSFabricPipelineJobParameters

# Back-compat alias: expose the new class under the old name
from .job import MSFabricRunJobOperator as MSFabricRunItemOperator



__all__ = [
    "MSFabricItemLink",
    "MSFabricRunItemOperator", # alias for back-compat
    "MSFabricRunJobOperator",
    "MSFabricRunUserDataFunctionOperator",
    "MSFabricRunSemanticModelRefreshOperator",
    # Parameter helper classes
    "MSFabricNotebookJobParameters",
    "MSFabricPipelineJobParameters",
]
