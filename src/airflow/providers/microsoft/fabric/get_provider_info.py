def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-microsoft-fabric",
        "name": "Microsoft Fabric",
        "description": "Provider for running and monitoring Microsoft Fabric jobs using service principal authentication.",
        
        "hooks": [
        ],
        "operators": [
            {
                "integration-name": "microsoft-fabric",
                "python-modules": ["airflow.providers.microsoft.fabric.operators.run_item"],
            }
        ],
        "operator-extra-links": [
            "airflow.providers.microsoft.fabric.operators.run_item.MSFabricItemLink",
        ],
        "connection-types": [
            {
                "connection-type": "microsoft-fabric",
                "hook-class-name": "airflow.providers.microsoft.fabric.hooks.rest_connection.MSFabricRestConnection",
            }
        ],
        "triggers": [
            {
                "integration-name": "microsoft-fabric",
                "python-modules": ["airflow.providers.microsoft.fabric.triggers.run_item"],
            }
        ],
    }
