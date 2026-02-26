import sys
import os

# DLL Stability Patch for Windows
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['USE_TORCH'] = '1'
os.environ['TRANSFORMERS_OFFLINE'] = '0'
os.environ['TRANSFORMERS_NO_TENSORFLOW'] = '1'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Add project root to path for local imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

from src.rag.rag_engine import RAGSystem

# Global state for the RAG engine
ml_models = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize RAG System on startup
    print("Loading NLP models and building index...")
    kb_path = "data/raw/kb"
    rag = RAGSystem(kb_dir=kb_path)
    rag.load_and_chunk()
    rag.build_index()
    ml_models["rag"] = rag
    print("API is ready to accept requests.")
    yield
    # Clean up on shutdown
    ml_models.clear()
    print("Models unloaded.")

app = FastAPI(title="CALA Analytics API", lifespan=lifespan)

@app.middleware("http")
async def log_requests(request, call_next):
    if request.method == "POST":
        print(f"Incoming POST to {request.url.path}")
    return await call_next(request)

class QueryRequest(BaseModel):
    question: str

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.get("/kpis")
def get_kpis():
    try:
        # For simplicity in this local test, we read from the cleaned parquet
        df = pd.read_parquet("output/processed/atenciones_cleaned.parquet")
        kpis = {
            "total_atenciones": int(len(df)),
            "total_facturado": float(df['valor_facturado'].sum()),
            "promedio_facturado": float(df['valor_facturado'].mean()),
            "top_canales": df['canal_ingreso'].value_counts().to_dict()
        }
        return kpis
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ask")
def ask_rag(request: QueryRequest) -> Dict[str, Any]:
    start_time = time.time()
    try:
        if "rag" not in ml_models:
            raise HTTPException(status_code=503, detail="RAG Model is not loaded yet.")
        
        answer = ml_models["rag"].ask(request.question)
        latency = time.time() - start_time
        return {
            "answer": answer,
            "latency_seconds": round(latency, 4)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
