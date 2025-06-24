from airflow.plugins_manager import AirflowPlugin
from airflow.providers.microsoft.fabric.hooks.fabric import FabricHook

class FabricPlugin(AirflowPlugin):
    name = "microsoft-fabric-plugin"
    hooks = [FabricHook]


