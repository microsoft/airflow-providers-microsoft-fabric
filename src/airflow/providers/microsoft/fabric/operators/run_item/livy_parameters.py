"""
Fluent builder for the Livy **batch** request body (mirrors
``notebook_parameters.MSFabricNotebookJobParameters``).

Produces a Livy batch body per https://livy.apache.org/docs/latest/rest-api.html.
``file`` is required.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class MSFabricLivyBatchParameters:
    _file: Optional[str] = None
    _class_name: Optional[str] = None
    _name: Optional[str] = None
    _py_files: List[str] = field(default_factory=list)
    _jars: List[str] = field(default_factory=list)
    _files: List[str] = field(default_factory=list)
    _args: List[str] = field(default_factory=list)
    _conf: Dict[str, str] = field(default_factory=dict)
    _num_executors: Optional[int] = None
    _executor_cores: Optional[int] = None
    _executor_memory: Optional[str] = None
    _driver_cores: Optional[int] = None
    _driver_memory: Optional[str] = None

    def set_file(self, file: str) -> "MSFabricLivyBatchParameters":
        self._file = file
        return self

    def set_class_name(self, class_name: str) -> "MSFabricLivyBatchParameters":
        self._class_name = class_name
        return self

    def set_name(self, name: str) -> "MSFabricLivyBatchParameters":
        self._name = name
        return self

    def add_py_file(self, path: str) -> "MSFabricLivyBatchParameters":
        self._py_files.append(path)
        return self

    def add_jar(self, path: str) -> "MSFabricLivyBatchParameters":
        self._jars.append(path)
        return self

    def add_file(self, path: str) -> "MSFabricLivyBatchParameters":
        self._files.append(path)
        return self

    def add_arg(self, arg: str) -> "MSFabricLivyBatchParameters":
        self._args.append(arg)
        return self

    def set_executors(
        self, num: Optional[int] = None, cores: Optional[int] = None, memory: Optional[str] = None
    ) -> "MSFabricLivyBatchParameters":
        if num is not None:
            self._num_executors = num
        if cores is not None:
            self._executor_cores = cores
        if memory is not None:
            self._executor_memory = memory
        return self

    def set_driver(
        self, cores: Optional[int] = None, memory: Optional[str] = None
    ) -> "MSFabricLivyBatchParameters":
        if cores is not None:
            self._driver_cores = cores
        if memory is not None:
            self._driver_memory = memory
        return self

    def set_conf(self, key: str, value: str) -> "MSFabricLivyBatchParameters":
        self._conf[key] = value
        return self

    def to_dict(self) -> Dict[str, Any]:
        if not self._file:
            raise ValueError("A Livy batch requires a 'file' (absolute abfss:// path). Call set_file().")
        body: Dict[str, Any] = {"file": self._file}
        if self._class_name:
            body["className"] = self._class_name
        if self._name:
            body["name"] = self._name
        if self._py_files:
            body["pyFiles"] = list(self._py_files)
        if self._jars:
            body["jars"] = list(self._jars)
        if self._files:
            body["files"] = list(self._files)
        if self._args:
            body["args"] = list(self._args)
        if self._num_executors is not None:
            body["numExecutors"] = self._num_executors
        if self._executor_cores is not None:
            body["executorCores"] = self._executor_cores
        if self._executor_memory is not None:
            body["executorMemory"] = self._executor_memory
        if self._driver_cores is not None:
            body["driverCores"] = self._driver_cores
        if self._driver_memory is not None:
            body["driverMemory"] = self._driver_memory
        if self._conf:
            body["conf"] = dict(self._conf)
        return body

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)
