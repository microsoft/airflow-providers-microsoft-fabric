from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Literal

# Allowed parameter types for Fabric notebook parameters
ParamType = Literal["string", "int", "float", "bool"]


@dataclass
class NotebookParams:
    """
    Holds typed parameters for Fabric notebook execution.
    Serializes to:
      "parameters": {
         "<name>": { "value": <value>, "type": "<string|int|float|bool>" }
      }
    """
    _params: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    def set(self, name: str, value: Any, ptype: Optional[ParamType] = None) -> "NotebookParams":
        """
        Add or replace a notebook parameter.

        - name: parameter name
        - value: parameter value (will be serialized as-is by json.dump)
        - ptype: optional explicit type; if omitted the type will be inferred
        """
        if ptype is None:
            ptype = self._infer_type(value)
        self._params[name] = {"value": value, "type": ptype}
        return self

    @staticmethod
    def _infer_type(value: Any) -> ParamType:
        # bool must be checked before int because bool is a subclass of int
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        return "string"

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._params)


@dataclass
class NotebookConfiguration:
    """
    Optional execution configuration block.
    Serializes to:
      "configuration": {
        "conf": {...},
        "environment": {"id": "...", "name": "..."},
        "defaultLakehouse": {"name": "...", "id": "...", "workspaceId": "..."},
        "useStarterPool": false,
        "useWorkspacePool": "..."
      }
    """
    conf: Dict[str, str] = field(default_factory=dict)
    environment_id: Optional[str] = None
    environment_name: Optional[str] = None
    default_lakehouse_name: Optional[str] = None
    default_lakehouse_id: Optional[str] = None
    default_lakehouse_workspace_id: Optional[str] = None
    use_starter_pool: Optional[bool] = None
    use_workspace_pool: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {}

        if self.conf:
            cfg["conf"] = dict(self.conf)

        if self.environment_id or self.environment_name:
            env: Dict[str, Any] = {}
            if self.environment_id:
                env["id"] = self.environment_id
            if self.environment_name:
                env["name"] = self.environment_name
            cfg["environment"] = env

        if (
            self.default_lakehouse_name
            or self.default_lakehouse_id
            or self.default_lakehouse_workspace_id
        ):
            dl: Dict[str, Any] = {}
            if self.default_lakehouse_name:
                dl["name"] = self.default_lakehouse_name
            if self.default_lakehouse_id:
                dl["id"] = self.default_lakehouse_id
            if self.default_lakehouse_workspace_id:
                dl["workspaceId"] = self.default_lakehouse_workspace_id
            cfg["defaultLakehouse"] = dl

        if self.use_starter_pool is not None:
            cfg["useStarterPool"] = self.use_starter_pool

        if self.use_workspace_pool:
            cfg["useWorkspacePool"] = self.use_workspace_pool

        return cfg


@dataclass
class MSFabricNotebookJobParameters:
    """
    Final payload builder for the Job Scheduler API (RunNotebook).
    Produces exactly:
    {
      "executionData": {
        "parameters": { ... },
        "configuration": { ... }  # omitted if empty
      }
    }

    Convenience mutators are provided so callers can work only with this
    class (e.g., NotebookJobParameters().set_parameter(...).set_conf(...)).
    """
    _parameters: NotebookParams = field(default_factory=NotebookParams)
    _configuration: Optional[NotebookConfiguration] = None

    # Parameter convenience
    def set_parameter(self, name: str, value: Any, ptype: Optional[ParamType] = None) -> "MSFabricNotebookJobParameters":
        """
        Proxy to NotebookParams.set(...) so callers only need to hold a NotebookJobParameters instance.
        """
        self._parameters.set(name, value, ptype)
        return self

    # Configuration convenience mutators
    def set_conf(self, key: str, value: str) -> "MSFabricNotebookJobParameters":
        """
        Add/replace a configuration key under configuration.conf.
        """
        if self._configuration is None:
            self._configuration = NotebookConfiguration()
        self._configuration.conf[key] = value
        return self

    def set_environment(self, environment_id: Optional[str] = None, environment_name: Optional[str] = None) -> "MSFabricNotebookJobParameters":
        """
        Set environment id and/or name.
        """
        if self._configuration is None:
            self._configuration = NotebookConfiguration()
        if environment_id is not None:
            self._configuration.environment_id = environment_id
        if environment_name is not None:
            self._configuration.environment_name = environment_name
        return self

    def set_default_lakehouse(
        self,
        name: Optional[str] = None,
        id: Optional[str] = None,
        workspace_id: Optional[str] = None,
    ) -> "MSFabricNotebookJobParameters":
        """
        Set default lakehouse name/id/workspaceId.
        """
        if self._configuration is None:
            self._configuration = NotebookConfiguration()
        if name is not None:
            self._configuration.default_lakehouse_name = name
        if id is not None:
            self._configuration.default_lakehouse_id = id
        if workspace_id is not None:
            self._configuration.default_lakehouse_workspace_id = workspace_id
        return self

    def set_use_starter_pool(self, use: bool) -> "MSFabricNotebookJobParameters":
        """
        Set useStarterPool flag.
        """
        if self._configuration is None:
            self._configuration = NotebookConfiguration()
        self._configuration.use_starter_pool = use
        return self

    def set_use_workspace_pool(self, pool_name: str) -> "MSFabricNotebookJobParameters":
        """
        Set useWorkspacePool value.
        """
        if self._configuration is None:
            self._configuration = NotebookConfiguration()
        self._configuration.use_workspace_pool = pool_name
        return self

    def to_dict(self) -> Dict[str, Any]:
        exec_data: Dict[str, Any] = {"parameters": self._parameters.to_dict()}

        if self._configuration:
            cfg = self._configuration.to_dict()
            if cfg:  # include only if non-empty
                exec_data["configuration"] = cfg

        return {"executionData": exec_data}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ----------------- Example -----------------
if __name__ == "__main__":
    job_param = MSFabricNotebookJobParameters()
    job_param = (job_param.set_parameter("parameterName", "new value", "string")
                 .set_parameter("threshold", 0.9)             # inferred -> float
                 .set_parameter("debug_mode", True)           # inferred -> bool
                 .set_conf("spark.conf1", "value")
                 .set_environment(
                     environment_id="<environment_id>",
                     environment_name="<environment_name>",
                 )
                 .set_default_lakehouse(
                     name="<lakehouse-name>",
                     id="<lakehouse-id>",
                     workspace_id="<workspace-id>",
                 )
                 .set_use_starter_pool(False)
                 .set_use_workspace_pool("<workspace-pool-name>")
                )

    print(job_param.to_json())
