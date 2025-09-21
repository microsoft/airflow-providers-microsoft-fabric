from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class MSFabricPipelineJobParameters:
    """
    Pipeline parameters for the Fabric Job Scheduler API.

    Produces exactly:
    {
      "executionData": {
        "parameters": {
          "YourParameter1": "value1",
          "YourParameter2": 123,
          "YourParameter3": true
        }
      }
    }
    """
    _parameters: Dict[str, Any] = field(default_factory=dict)

    def set_parameter(self, name: str, value: Any) -> "MSFabricPipelineJobParameters":
        """
        Set a pipeline parameter.

        - name: parameter name
        - value: parameter value (any JSON-serializable type)
        """
        self._parameters[name] = value
        return self

    def to_dict(self) -> Dict[str, Any]:
        return {"executionData": {"parameters": self._parameters}}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ----------------- Example -----------------
if __name__ == "__main__":
    payload = (
        MSFabricPipelineJobParameters()
        .set_parameter("YourParameter1", "value1")
        .set_parameter("YourParameter2", 123)
        .set_parameter("YourParameter3", True)
    )

    print(payload.to_json())
