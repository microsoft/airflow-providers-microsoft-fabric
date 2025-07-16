def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-microsoft-fabric",
        "name": "Provider for integrating with Microsoft Fabric services",
        "description": "Adds easy connectivity to Microsoft Fabric",
        "hooks": [
            {
                "integration-name": "microsoft-fabric", 
                "python-modules": ["airflow.providers.microsoft.fabric.hooks.run_item"]
            }
        ],
        "operators": [
            {
                "integration-name": "microsoft-fabric",
                "python-modules": ["airflow.providers.microsoft.fabric.operators.run_item"],
            }
        ],
        "operator-extra-links": [
            "airflow.providers.microsoft.fabric.operators.run_item.MSFabricRunItemLink"
        ],
        "connection-types": [
            {
                "connection-type": "microsoft-fabric",
                "hook-class-name": "airflow.providers.microsoft.fabric.hooks.run_item.MSFabricHook",
            }
        ],
    }