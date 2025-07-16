from airflow.plugins_manager import AirflowPlugin
from airflow.providers.microsoft.fabric.hooks.run_item import MSFabricHook

class FabricPlugin(AirflowPlugin):
    name = "microsoft-fabric-plugin"
    hooks = [MSFabricHook]


