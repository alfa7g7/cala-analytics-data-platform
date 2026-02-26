import sys
import os
import time
import io
from contextlib import redirect_stdout

project_root = os.getcwd()
sys.path.append(project_root)
from src.rag.rag_engine import RAGSystem

# Preguntas exhaustivas cubriendo el 100% de la base de conocimientos
tech_queries = [
    # ---- de arquitectura_actual.md ----
    "que es la arquitectura actual?", 
    "usa bigquery?", 
    "usa airflow?", 
    "cual es la propuesta de cala analytics?",
    "orquestacion en la nube gcp",
    
    # ---- de definiciones.md ----
    "que es un cups?", 
    "que es la clasificacion unica de procedimientos?", 
    "donde esta el diagnostico dx?", 
    "que es el canal de ingreso?", 
    "que canales hay?", 
    "total atenciones", 
    "como se calcula el valor facturado?", 
    "que es el promedio por atencion?", 
    "que es la tasa de errores de calidad?", 
    "como se limpian los documentos?", 
    "que pasa con las ciudades?", 
    "como se manejan los duplicados?", 
    "que es el json_detalle?", 
    "donde se guardan los resultados?", 
    "que es la propuesta tecnica?", 
    "LOGIN", 
    "CLICK", 
    "ERROR", 
    "COMPRA",
    
    # ---- de faq_operativa.md ----
    "como validar una atencion?", 
    "como es el proceso de atenciones?", 
    "cuantos errores hubo?", 
    "quien es el medico?",
    
    # ---- de glosario_eventos.md ----
    "que indica error en glosario?",
    
    # ---- de instrucciones.md (Eliminado por ser archivo de prueba) ----
    # "cual es el objetivo de la prueba tecnica?", 
    # "cuales son los insumos entregados?", 
    # "que hace clientes.csv?", 
    # "que hace atenciones.csv?", 
    # "que es eventos_app.json?", 
    # "que hay en la carpeta kb?", 
    # "como es el pipeline de datos en python?", 
    # "normalizar documentos y ciudades", 
    # "parsear json embebido", 
    # "exportar a formato parquet", 
    # "como es el modelado en bigquery?", 
    # "particion por fecha_proceso", 
    # "clustering justificado", 
    # "consultas optimizadas", 
    # "orquestacion con airflow", 
    # "el dag debe ser idempotente", 
    # "despliegue en composer", 
    # "implementacion de rag faiss", 
    # "endpoint health kpis ask",
    
    # ---- de politicas.md ----
    "Políticas",
    "atenciones a cliente valido",
    
    # ---- de reporte_actual.md ----
    "cuantas atenciones procesadas con exito?",
    "registros descartados por duplicados",
    "errores de calidad json malformado",
    "cual es la facturacion total de la carga?",
    "distribucion por canal app web call_center",
    "estandarizacion de ciudades"
]

# 15 Preguntas incoherentes (ruido)
noise_queries = [
    "de que color es el sol?", "como hacer una pizza?", "quien gano el mundial?",
    "precio del dolar hoy", "clima en bogota", "cuanto es 2 mas 2",
    "que musica te gusta?", "tengo hambre", "donde esta mi gato?",
    "quien es el presidente?", "vendes helados?", "como volar un avion?",
    "receta de pasta", "el sol brilla mucho", "capital de francia"
]

def run_stress_test():
    rag = RAGSystem(kb_dir='data/raw/kb')
    rag.load_and_chunk()
    rag.build_index()
    
    print("\n" + "="*80)
    print("DETAILED STRESS TEST REPORT")
    print("="*80)
    
    tech_passed = 0
    for q in tech_queries:
        print(f"QUERY: {q}")
        f = io.StringIO()
        with redirect_stdout(f):
            ans = rag.ask(q)
        debug_output = f.getvalue().strip()
        is_denied = "Lo siento" in ans or "IA encontró un tema" in ans
        status = "FAIL" if is_denied else "PASS"
        if not is_denied: tech_passed += 1
        
        print(f"  Result: {status}")
        if debug_output: print(f"  Debug: {debug_output}")
        print("-" * 40)

    noise_rejected = 0
    for q in noise_queries:
        print(f"NOISE: {q}")
        f = io.StringIO()
        with redirect_stdout(f):
            ans = rag.ask(q)
        debug_output = f.getvalue().strip()
        is_denied = "Lo siento" in ans or "IA encontró un tema" in ans
        status = "PASS (Rejected)" if is_denied else "FAIL (Hallucinated)"
        if is_denied: noise_rejected += 1
        
        print(f"  Result: {status}")
        if debug_output: print(f"  Debug: {debug_output}")
        print("-" * 40)
    
    print("\n" + "="*80)
    print(f"FINAL SCORECARD")
    print(f"Tech Queries Accuracy: {tech_passed}/{len(tech_queries)}")
    print(f"Noise Rejection Rate: {noise_rejected}/{len(noise_queries)}")
    print("="*80)

if __name__ == "__main__":
    run_stress_test()
