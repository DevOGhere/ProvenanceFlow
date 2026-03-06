"""
ProvenanceFlow REST API.

Start with:
    uvicorn provenanceflow.api.app:app --host 0.0.0.0 --port 8000

OpenAPI docs available at http://localhost:8000/docs
"""
from fastapi import FastAPI

from .routers.health import router as health_router
from .routers.runs import router as runs_router

app = FastAPI(
    title="ProvenanceFlow API",
    version="1.0.0",
    description=(
        "REST interface for querying W3C PROV lineage records produced "
        "by the ProvenanceFlow pipeline."
    ),
)

app.include_router(health_router)
app.include_router(runs_router, prefix="/runs")
