"""Where the V1 jobs meet: the landing volume the feeder writes and ingestion reads, and the folders the proof
driver and the platform tasks pass files through. One catalog per environment and layer (ADR-012)."""


def landing(env: str) -> str:
    return f"/Volumes/{env}_landing/deliveries/files"


def hops(env: str) -> str:
    return f"/Volumes/{env}_ops/proof/run/hops"


def sources(env: str) -> str:
    return f"/Volumes/{env}_ops/proof/run/sources"


def reports(env: str, run_id: str) -> str:
    return f"/Volumes/{env}_ops/proof/run/reports/{run_id}"


SETUP = (
    "CREATE CATALOG IF NOT EXISTS {env}_landing",
    "CREATE CATALOG IF NOT EXISTS {env}_bronze",
    "CREATE CATALOG IF NOT EXISTS {env}_silver",
    "CREATE CATALOG IF NOT EXISTS {env}_gold",
    "CREATE CATALOG IF NOT EXISTS {env}_ops",
    "CREATE SCHEMA IF NOT EXISTS {env}_landing.deliveries",
    "CREATE VOLUME IF NOT EXISTS {env}_landing.deliveries.files",
    "CREATE SCHEMA IF NOT EXISTS {env}_ops.proof",
    "CREATE VOLUME IF NOT EXISTS {env}_ops.proof.run",
)
