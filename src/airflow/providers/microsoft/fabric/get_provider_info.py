def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-microsoft-fabric",
        "name": "Provider for integrating with Microsoft Fabric services",
        "description": "Adds easy connectivity to Microsoft Fabric",

        "hooks": [
            {
                "integration-name": "microsoft-fabric", 
                "python-modules": ["airflow.providers.microsoft.fabric.hooks.fabric"]
            }
        ],

        "operators": [
            {
                "integration-name": "microsoft-fabric",
                "python-modules": ["airflow.providers.microsoft.fabric.operators.fabric"],
            }
        ],

        "connection-types": [
            {
                "connection-type": "microsoft-fabric",
                "hook-class-name": "airflow.providers.microsoft.fabric.hooks.fabric.FabricHook",
            }
        ],
    }