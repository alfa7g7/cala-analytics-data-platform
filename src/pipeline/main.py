import pandas as pd
import json
import os
import re
import argparse
from datetime import datetime
import unicodedata
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, ValidationError

class JsonDetalle(BaseModel):
    diagnostico: Optional[str] = Field(default=None)
    medico: Optional[str] = Field(default=None)

def remove_accents(input_str: Any) -> Optional[str]:
    if pd.isna(input_str) or not input_str:
        return None
    nfkd_form = unicodedata.normalize('NFKD', str(input_str))
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


class DataPipeline:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.quality_report = {
            "summary": {
                "critical_errors": 0,
                "cleanups_document": 0,
                "cleanups_city": 0,
                "duplicates_removed": 0
            },
            "details": {
                "critical_errors": [],
                "cleanups_document": [],
                "cleanups_city": []
            }
        }

    def log_critical(self, message: str) -> None:
        self.quality_report["summary"]["critical_errors"] += 1
        self.quality_report["details"]["critical_errors"].append(message)

    def log_cleanup(self, category: str, message: str) -> None:
        self.quality_report["summary"][f"cleanups_{category}"] += 1
        # Limit details to first 500 to keep report size manageable
        if len(self.quality_report["details"][f"cleanups_{category}"]) < 500:
            self.quality_report["details"][f"cleanups_{category}"].append(message)

    def normalize_document(self, doc: Any, record_id: Any, category: str = "general") -> Optional[str]:
        if pd.isna(doc):
            return None
        original = str(doc)
        cleaned = re.sub(r'\D', '', original)
        if original != cleaned:
            self.log_cleanup("document", f"ID {record_id} ({category}) - Cleaned '{original}' to '{cleaned}'")
        return cleaned

    def normalize_state(self, state: Any) -> str:
        if pd.isna(state):
            return "DESCONOCIDO"
        return str(state).upper().strip()

    def normalize_city(self, city: Any, record_id: Any) -> str:
        if pd.isna(city): return "Desconocido"
        original = str(city)
        # Apply accent removal for consistency (Bogotá -> Bogota)
        # and transform to Title Case
        clean_name = remove_accents(original)
        clean = clean_name.strip().title() if clean_name else "Desconocido"
        
        if original != clean:
            self.log_cleanup("city", f"ID {record_id} - Cleaned '{original}' to '{clean}'")
        return clean

    def parse_json_detalle(self, json_str: Any, record_id: Any) -> Dict[str, Optional[str]]:
        try:
            if pd.isna(json_str) or not str(json_str).strip():
                return {"diagnostico": None, "medico": None}
            data = json.loads(str(json_str))
            # Pro validation via Pydantic
            validated_data = JsonDetalle(**data)
            return validated_data.model_dump()
        except ValidationError as e:
            self.log_critical(f"ID {record_id} - Pydantic Validation Error: {e.errors()[0]['msg']}")
            return {"diagnostico": "ERROR_VALIDATION", "medico": "ERROR_VALIDATION"}
        except Exception as e:
            self.log_critical(f"ID {record_id} - Error parsing JSON: {str(e)}")
            return {"diagnostico": "ERROR_JSON", "medico": "ERROR_JSON"}

    def process_atenciones(self):
        df = pd.read_csv(os.path.join(self.input_dir, "atenciones.csv"))
        initial_count = len(df)
        
        # Deduplication: Keep latest fecha_atencion for same id_atencion
        df['fecha_atencion'] = pd.to_datetime(df['fecha_atencion'])
        df = df.sort_values(by=['id_atencion', 'fecha_atencion'], ascending=[True, False])
        df = df.drop_duplicates(subset=['id_atencion'], keep='first')
        
        self.quality_report["summary"]["duplicates_removed"] = initial_count - len(df)

        # Normalization
        df['documento_cliente'] = df.apply(lambda r: self.normalize_document(r['documento_cliente'], r['id_atencion'], "atenciones"), axis=1)
        df['estado'] = df['estado'].apply(self.normalize_state)
        
        # Parse JSON
        json_fields = df.apply(lambda row: self.parse_json_detalle(row['json_detalle'], row['id_atencion']), axis=1)
        df_json = pd.json_normalize(json_fields)
        df = pd.concat([df.reset_index(drop=True), df_json], axis=1)
        
        # Export
        output_path = os.path.join(self.output_dir, "atenciones_cleaned.parquet")
        df.drop(columns=['json_detalle']).to_parquet(output_path, index=False)
        return df

    def process_clientes(self):
        df = pd.read_csv(os.path.join(self.input_dir, "clientes.csv"))
        
        # Normalization
        df['documento'] = df.apply(lambda r: self.normalize_document(r['documento'], r['id_cliente'], "clientes"), axis=1)
        df['segmento'] = df['segmento'].str.upper()
        df['ciudad'] = df.apply(lambda r: self.normalize_city(r['ciudad'], r['id_cliente']), axis=1)
        
        # Export
        output_path = os.path.join(self.output_dir, "clientes_cleaned.parquet")
        df.to_parquet(output_path, index=False)
        return df

    def process_eventos(self):
        with open(os.path.join(self.input_dir, "eventos_app.json"), 'r') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        
        # Normalization: Ensure id_cliente is numeric/clean if it was a string
        # and convert timestamps to datetime
        if 'id_cliente' in df.columns:
            df['id_cliente'] = df.apply(lambda r: self.normalize_document(r['id_cliente'], r.get('id_evento', 'N/A'), "eventos"), axis=1)
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['fecha_proceso'] = df['timestamp'].dt.date
            
        # Export
        output_path = os.path.join(self.output_dir, "eventos_app_cleaned.parquet")
        df.to_parquet(output_path, index=False)
        return df

    def run(self):
        print("Starting Data Pipeline...")
        self.process_atenciones()
        self.process_clientes()
        self.process_eventos()
        
        # Quality Report
        report_path = os.path.join(self.output_dir, "quality_report.json")
        with open(report_path, 'w') as f:
            json.dump(self.quality_report, f, indent=4)
        print(f"Pipeline finished. Metrics: {self.quality_report['summary']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CALA Analytics Data Pipeline")
    parser.add_argument("--input", default="data/raw", help="Input directory")
    parser.add_argument("--output", default="output/processed", help="Output directory")
    args = parser.parse_args()
    
    pipeline = DataPipeline(args.input, args.output)
    pipeline.run()
