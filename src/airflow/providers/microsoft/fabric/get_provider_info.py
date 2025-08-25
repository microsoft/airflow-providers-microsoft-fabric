def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-microsoft-fabric",
        "name": "Microsoft Fabric",
        "description": "Provider for running and monitoring Microsoft Fabric jobs using service principal authentication.",
        
        "hooks": [
                        {
                "integration-name": "microsoft-fabric",
                "python-modules": [
                    "airflow.providers.microsoft.fabric.hooks.run_item", # import the package so the alias in __init__.py is registered
                    "airflow.providers.microsoft.fabric.hooks.run_item.job",
                    "airflow.providers.microsoft.fabric.hooks.run_item.user_data_function",
                    "airflow.providers.microsoft.fabric.hooks.run_item.semantic_model_refresh"],
            }

        ],
        "operators": [
            {
                "integration-name": "microsoft-fabric",
                "python-modules": [
                    "airflow.providers.microsoft.fabric.operators.run_item", # import the package so the alias in __init__.py is registered
                    "airflow.providers.microsoft.fabric.operators.run_item.job",
                    "airflow.providers.microsoft.fabric.operators.run_item.user_data_function",
                    "airflow.providers.microsoft.fabric.operators.run_item.semantic_model_refresh"],
            }
        ],
        "extra-links": [
            "airflow.providers.microsoft.fabric.operators.run_item.MSFabricItemLink",
        ],
        "connection-types": [
            {
                "connection-type": "microsoft-fabric",
                "hook-class-name": "airflow.providers.microsoft.fabric.hooks.connection.rest_connection.MSFabricRestConnection",
            }
        ],
        "triggers": [
            {
                "integration-name": "microsoft-fabric",
                "python-modules": [
                    "airflow.providers.microsoft.fabric.triggers.run_item.job",
                    "airflow.providers.microsoft.fabric.triggers.run_item.semantic_model_refresh"],
            }
        ],
    }
