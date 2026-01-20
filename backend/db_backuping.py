from fastapi import APIRouter, HTTPException, Header
from fastapi.responses import FileResponse
from pathlib import Path  # make sure this is pathlib.Path
import os

router = APIRouter()

@router.get("/download-db")
def download_duckdb():

    db_path = Path("/data/rdb_timestock1")
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database file not found")

    return FileResponse(
        path=db_path,
        media_type="application/octet-stream",
        filename=db_path.name
    )
