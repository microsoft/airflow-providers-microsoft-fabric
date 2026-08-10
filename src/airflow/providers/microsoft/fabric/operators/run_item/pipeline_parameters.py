from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

# Allowed parameter types per the Fabric Job Scheduler ``ItemJobParameterType`` enum.
# https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job#itemjobparametertype
ParamType = Literal[
    "VariableReference",
    "Integer",
    "Number",
    "Text",
    "Boolean",
    "DateTime",
    "Guid",
    "Automatic",
]


@dataclass
class MSFabricPipelineJobParameters:
    """
    Pipeline parameters for the Fabric Job Scheduler "Run On Demand Item Job" API.

    Produces a request body with a top-level ``parameters`` array, where each
    parameter is an object with ``name``, ``value`` and ``type``, matching:
    https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job#request-body

    {
      "parameters": [
        { "name": "YourParameter1", "value": "value1", "type": "Text" },
        { "name": "YourParameter2", "value": 123, "type": "Integer" },
        { "name": "YourParameter3", "value": true, "type": "Boolean" }
      ]
    }
    """
    _parameters: dict[str, dict[str, Any]] = field(default_factory=dict)

    def set_parameter(
        self, name: str, value: Any, ptype: ParamType | None = None
    ) -> MSFabricPipelineJobParameters:
        """
        Add or replace a pipeline parameter.

        - name: parameter name (must be unique per run)
        - value: parameter value (any JSON-serializable type)
        - ptype: optional explicit parameter type; inferred from ``value`` when omitted

        Type inference maps concrete Python types to the API enum:
        ``bool`` -> Boolean, ``int`` -> Integer, ``float`` -> Number,
        ``datetime`` -> DateTime, ``uuid.UUID`` -> Guid, everything else -> Text.
        Strings are always treated as ``Text`` (no shape guessing); pass ``ptype``
        explicitly to send a string as ``VariableReference``, ``Guid``,
        ``DateTime`` or ``Automatic``.
        """
        if ptype is None:
            ptype = self._infer_type(value)
        self._parameters[name] = {
            "name": name,
            "value": self._normalize_value(value),
            "type": ptype,
        }
        return self

    @staticmethod
    def _infer_type(value: Any) -> ParamType:
        # bool must be checked before int because bool is a subclass of int
        if isinstance(value, bool):
            return "Boolean"
        if isinstance(value, int):
            return "Integer"
        if isinstance(value, float):
            return "Number"
        if isinstance(value, datetime):
            return "DateTime"
        if isinstance(value, UUID):
            return "Guid"
        return "Text"

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        # Convert non-JSON-native types to the API's expected string form so the
        # payload stays serializable and matches the declared parameter type.
        if isinstance(value, datetime):
            # DateTime must be UTC in YYYY-MM-DDTHH:mm:ssZ format.
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc)
            return value.strftime("%Y-%m-%dT%H:%M:%SZ")
        if isinstance(value, UUID):
            return str(value)
        return value

    def to_dict(self) -> dict[str, Any]:
        return {"parameters": list(self._parameters.values())}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ----------------- Example -----------------
if __name__ == "__main__":
    payload = (
        MSFabricPipelineJobParameters()
        .set_parameter("YourParameter1", "value1")   # inferred -> Text
        .set_parameter("YourParameter2", 123)        # inferred -> Integer
        .set_parameter("YourParameter3", True)       # inferred -> Boolean
    )

    print(payload.to_json())
