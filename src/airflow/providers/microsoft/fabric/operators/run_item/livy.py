"""
Microsoft Fabric **Livy** operators, built on the ``run_item`` base framework.

* ``MSFabricLivyBatchOperator`` — submit a Spark **batch** via the Livy API and
  monitor to completion. Subclasses ``BaseFabricRunItemOperator``, so it reuses
  the shared deferral / XCom / status-plugin / extra-link machinery. Supports
  deferrable mode via ``MSFabricLivyBatchTrigger``.
* ``MSFabricLivySessionOperator`` — run PySpark **code** in an interactive Livy
  session ("Livy submission") and return its stdout. Uses the synchronous
  run_item pattern (like the User Data Function operator).

Livy is Lakehouse-scoped, so ``item_id`` carries the ``lakehouse_id`` and
``item_type`` is ``"LivyBatch"`` / ``"LivySession"``.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Optional, Sequence

from airflow.providers.microsoft.fabric.hooks.run_item.base import MSFabricRunItemException
from airflow.providers.microsoft.fabric.hooks.run_item.livy import (
    LivyBatchConfig,
    LivySessionConfig,
    MSFabricLivyBatchHook,
    MSFabricLivySessionHook,
)
from airflow.providers.microsoft.fabric.hooks.run_item.model import ItemDefinition, RunItemTracker
from airflow.providers.microsoft.fabric.operators.run_item.base import (
    BaseFabricRunItemOperator,
    MSFabricItemLink,
)
from airflow.providers.microsoft.fabric.operators.run_item.livy_parameters import (
    MSFabricLivyBatchParameters,
)
from airflow.providers.microsoft.fabric.triggers.run_item.base import BaseFabricRunItemTrigger
from airflow.providers.microsoft.fabric.triggers.run_item.livy import MSFabricLivyBatchTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MSFabricLivyBatchOperator(BaseFabricRunItemOperator):
    """Run a Spark batch on Fabric through the Livy API (run_item integration)."""

    template_fields: Sequence[str] = (
        "fabric_conn_id",
        "workspace_id",
        "lakehouse_id",
        "file",
        "py_files",
        "jars",
        "args",
        "job_params",
        "timeout",
        "check_interval",
        "deferrable",
        "api_host",
        "scope",
        "link_base_url",
    )
    template_fields_renderers = {"job_params": "json", "conf": "json"}
    operator_extra_links = (MSFabricItemLink(),)

    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        lakehouse_id: str,
        file: Optional[str] = None,
        name: str = "airflow-fabric-livy-batch",
        py_files: Optional[list] = None,
        jars: Optional[list] = None,
        files: Optional[list] = None,
        args: Optional[list] = None,
        num_executors: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        driver_cores: Optional[int] = None,
        driver_memory: Optional[str] = None,
        conf: Optional[dict] = None,
        job_params: str = "",
        timeout: int = 60 * 60,
        check_interval: int = 30,
        deferrable: bool = True,
        wait_for_termination: bool = True,
        api_host: str = "https://api.fabric.microsoft.com",
        scope: str = "https://api.fabric.microsoft.com/.default",
        link_base_url: str = "https://app.fabric.microsoft.com",
        **kwargs,
    ) -> None:
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.file = file
        self.name = name
        self.job_type = "LivyBatch"
        self.py_files = py_files or []
        self.jars = jars or []
        self.files = files or []
        self.args = args or []
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_cores = driver_cores
        self.driver_memory = driver_memory
        self.conf = conf or {}
        self.job_params = job_params or ""
        self.timeout = timeout
        self.check_interval = check_interval
        self.deferrable = deferrable
        self.wait_for_termination = wait_for_termination
        self.api_host = api_host
        self.scope = scope
        self.link_base_url = link_base_url

        item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self.job_type,
            item_id=self.lakehouse_id,  # Livy is lakehouse-scoped
            item_name=self.name,
        )
        super().__init__(item=item, **kwargs)

    def build_body(self) -> dict:
        """Build the Livy batch body from ``job_params`` or the individual fields."""
        if self.job_params:
            body = json.loads(self.job_params) if isinstance(self.job_params, str) else dict(self.job_params)
        else:
            body = {}
            if self.num_executors is not None:
                body["numExecutors"] = self.num_executors
            if self.executor_cores is not None:
                body["executorCores"] = self.executor_cores
            if self.executor_memory is not None:
                body["executorMemory"] = self.executor_memory
            if self.driver_cores is not None:
                body["driverCores"] = self.driver_cores
            if self.driver_memory is not None:
                body["driverMemory"] = self.driver_memory
            if self.py_files:
                body["pyFiles"] = list(self.py_files)
            if self.jars:
                body["jars"] = list(self.jars)
            if self.files:
                body["files"] = list(self.files)
            if self.args:
                body["args"] = list(self.args)
            if self.conf:
                body["conf"] = dict(self.conf)
        body.setdefault("name", self.name)
        if self.file:
            body["file"] = self.file
        if not body.get("file"):
            raise MSFabricRunItemException("A Livy batch requires 'file' (absolute abfss:// app path).")
        return body

    def _config(self) -> LivyBatchConfig:
        return LivyBatchConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            lakehouse_id=self.lakehouse_id,
            api_host=self.api_host,
            api_scope=self.scope,
            livy_body=json.dumps(self.build_body()),
        )

    def create_hook(self) -> MSFabricLivyBatchHook:
        return MSFabricLivyBatchHook(config=self._config())

    def create_trigger(self, tracker: RunItemTracker) -> MSFabricLivyBatchTrigger:
        return MSFabricLivyBatchTrigger(config=self._config().to_dict(), tracker=tracker.to_dict())

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env=jinja_env)
        self.item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self.job_type,
            item_id=self.lakehouse_id,
            item_name=self.name,
        )

    def execute(self, context: "Context") -> None:
        self.log.info(
            "Starting Fabric Livy batch - workspace_id: %s, lakehouse_id: %s, file: %s",
            self.workspace_id, self.lakehouse_id, self.file,
        )
        hook = self.create_hook()
        asyncio.run(self._execute_core(context, self.deferrable, hook, self.wait_for_termination))


class MSFabricLivySessionOperator(BaseFabricRunItemOperator):
    """Run PySpark code in an interactive Fabric Livy session ("Livy submission")."""

    template_fields: Sequence[str] = (
        "fabric_conn_id",
        "workspace_id",
        "lakehouse_id",
        "code",
        "timeout",
        "check_interval",
        "api_host",
        "scope",
        "link_base_url",
    )
    template_fields_renderers = {"code": "python", "conf": "json"}
    operator_extra_links = (MSFabricItemLink(),)

    def __init__(
        self,
        *,
        fabric_conn_id: str,
        workspace_id: str,
        lakehouse_id: str,
        code: str,
        name: str = "airflow-fabric-livy-session",
        num_executors: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        driver_cores: Optional[int] = None,
        driver_memory: Optional[str] = None,
        conf: Optional[dict] = None,
        session_timeout: int = 900,
        timeout: int = 900,
        check_interval: int = 10,
        delete_session_on_finish: bool = True,
        api_host: str = "https://api.fabric.microsoft.com",
        scope: str = "https://api.fabric.microsoft.com/.default",
        link_base_url: str = "https://app.fabric.microsoft.com",
        **kwargs,
    ) -> None:
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.code = code
        self.name = name
        self.job_type = "LivySession"
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_cores = driver_cores
        self.driver_memory = driver_memory
        self.conf = conf or {}
        self.session_timeout = session_timeout
        self.timeout = timeout
        self.check_interval = check_interval
        self.delete_session_on_finish = delete_session_on_finish
        self.api_host = api_host
        self.scope = scope
        self.link_base_url = link_base_url

        item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self.job_type,
            item_id=self.lakehouse_id,
            item_name=self.name,
        )
        super().__init__(item=item, **kwargs)

    def build_session_body(self) -> dict:
        body: dict = {"name": self.name}
        if self.num_executors is not None:
            body["numExecutors"] = self.num_executors
        if self.executor_cores is not None:
            body["executorCores"] = self.executor_cores
        if self.executor_memory is not None:
            body["executorMemory"] = self.executor_memory
        if self.driver_cores is not None:
            body["driverCores"] = self.driver_cores
        if self.driver_memory is not None:
            body["driverMemory"] = self.driver_memory
        if self.conf:
            body["conf"] = dict(self.conf)
        return body

    def create_hook(self) -> MSFabricLivySessionHook:
        config = LivySessionConfig(
            fabric_conn_id=self.fabric_conn_id,
            timeout_seconds=self.timeout,
            poll_interval_seconds=self.check_interval,
            lakehouse_id=self.lakehouse_id,
            api_host=self.api_host,
            api_scope=self.scope,
            session_body=json.dumps(self.build_session_body()),
            code=self.code,
            session_timeout_seconds=self.session_timeout,
            delete_session_on_finish=self.delete_session_on_finish,
        )
        return MSFabricLivySessionHook(config=config)

    def create_trigger(self, tracker: RunItemTracker) -> BaseFabricRunItemTrigger:
        raise MSFabricRunItemException("Livy session does not support asynchronous execution.")

    def render_template_fields(self, context, jinja_env=None):
        super().render_template_fields(context, jinja_env=jinja_env)
        self.item = ItemDefinition(
            workspace_id=self.workspace_id,
            item_type=self.job_type,
            item_id=self.lakehouse_id,
            item_name=self.name,
        )

    def execute(self, context: "Context") -> None:
        self.log.info(
            "Starting Fabric Livy session - workspace_id: %s, lakehouse_id: %s",
            self.workspace_id, self.lakehouse_id,
        )
        hook = self.create_hook()
        asyncio.run(self._execute_core(context, False, hook))
