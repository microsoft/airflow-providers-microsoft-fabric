from __future__ import annotations

ONELAKE_ENDPOINT: str = "https://onelake.dfs.fabric.microsoft.com"
ONELAKE_SCOPE: str = "https://storage.azure.com/.default"

DEFAULT_READ_CHUNK_SIZE: int = 4 * 1024 * 1024    # 4 MiB
DEFAULT_WRITE_BUFFER_SIZE: int = 4 * 1024 * 1024  # 4 MiB