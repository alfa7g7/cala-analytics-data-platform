import os
import sys

# DLL Stability Patch: Force CPU/Torch and block TensorFlow loading artifacts
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['USE_TORCH'] = '1'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['TRANSFORMERS_NO_TENSORFLOW'] = '1'

import glob
import unicodedata
import re
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import spacy

class RAGSystem:
    def __init__(self, kb_dir, model_name='paraphrase-multilingual-MiniLM-L12-v2'):
        self.kb_dir = kb_dir
        # Switch to a high-quality multilingual model
        self.model = SentenceTransformer(model_name, device='cpu')
        self.index = None
        self.chunks = []
        self.metadata = []
        
        # Load SpaCy for lemmatization
        try:
            self.nlp = spacy.load("es_core_news_lg")
        except:
            # Fallback if model not found (though it should be)
            self.nlp = None
            print("WARNING: SpaCy model 'es_core_news_lg' not found. Lemmatization disabled.")

    def load_and_chunk(self):
        files = glob.glob(os.path.join(self.kb_dir, "*.md"))
        for file_path in files:
            file_name = os.path.basename(file_path)
            content = ""
            for enc in ['utf-8', 'latin1', 'cp1252']:
                try:
                    with open(file_path, 'r', encoding=enc) as f:
                        content = f.read()
                        break
                except: continue
            
            if not content: continue
            
            content = content.replace('\r', '')
            lines = [line.strip() for line in content.split('\n')]
            current_chunk = []
            current_header = ""
            
            for line in lines:
                if not line:
                    if current_chunk:
                        chunk_text = " ".join(current_chunk).strip()
                        if current_header and not chunk_text.startswith(current_header):
                            chunk_text = f"{current_header}: {chunk_text}"
                        if len(chunk_text) > 15:
                            chunk_text = re.sub(r'\s+', ' ', chunk_text).strip()
                            self.chunks.append(chunk_text)
                            self.metadata.append({"file": file_name, "text": chunk_text})
                        current_chunk = []
                    continue
                
                if line.startswith('#'):
                    if current_chunk:
                        chunk_text = " ".join(current_chunk).strip()
                        if current_header and not chunk_text.startswith(current_header):
                            chunk_text = f"{current_header}: {chunk_text}"
                        if len(chunk_text) > 15:
                            chunk_text = re.sub(r'\s+', ' ', chunk_text).strip()
                            self.chunks.append(chunk_text)
                            self.metadata.append({"file": file_name, "text": chunk_text})
                        current_chunk = []
                    current_header = line.strip('# ')
                    continue
                
                # If it's a new bullet point, break the previous chunk
                if line.startswith('* ') or line.startswith('- ') or line.startswith('¿') or (len(line)>2 and line[1]=='.' and line[0].isdigit()) or line.startswith('•'):
                    if current_chunk:
                        chunk_text = " ".join(current_chunk).strip()
                        if current_header and not chunk_text.startswith(current_header):
                            chunk_text = f"{current_header}: {chunk_text}"
                        if len(chunk_text) > 15:
                            chunk_text = re.sub(r'\s+', ' ', chunk_text).strip()
                            self.chunks.append(chunk_text)
                            self.metadata.append({"file": file_name, "text": chunk_text})
                    current_chunk = [line]
                else:
                    current_chunk.append(line)
                    
            if current_chunk:
                chunk_text = " ".join(current_chunk).strip()
                if current_header and not chunk_text.startswith(current_header):
                    chunk_text = f"{current_header}: {chunk_text}"
                if len(chunk_text) > 15:
                    chunk_text = re.sub(r'\s+', ' ', chunk_text).strip()
                    self.chunks.append(chunk_text)
                    self.metadata.append({"file": file_name, "text": chunk_text})

    def build_index(self):
        print(f"Embedding {len(self.chunks)} chunks...")
        embeddings = self.model.encode(self.chunks, normalize_embeddings=True)
        dimension = embeddings.shape[1]
        self.index = faiss.IndexFlatL2(dimension)
        self.index.add(np.array(embeddings).astype('float32'))
        print("Index build successfully.")

    def query(self, text, k=3):
        if self.index is None:
            return [], []
        
        query_embedding = self.model.encode([text], normalize_embeddings=True)
        distances, indices = self.index.search(np.array(query_embedding).astype('float32'), k)
        
        results = []
        scores = []
        for d, idx in zip(distances[0], indices[0]):
            if idx != -1:
                results.append(self.metadata[idx])
                scores.append(float(d))
        
        return results, scores

    def get_lemmas(self, text):
        if not self.nlp:
            return text.lower().split()
        doc = self.nlp(text.lower())
        # Filter stopwords and punctuation
        return [token.lemma_ for token in doc if not token.is_stop and not token.is_punct and len(token.text) > 1]

    def ask(self, question):
        k = 15
        question_lower = question.lower()
        
        # Volvemos a usar la pregunta en minúscula para evitar problemas de sensibilidad a mayúsculas
        results, scores = self.query(question_lower, k=k)
        
        if not results:
            return (
                "Lo siento, la información solicitada no se encuentra en la base de conocimientos técnica."
            )
            
        def strip_accents(s):
            return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
        
        # Clean question words for exact matching or noise detection
        question_words = set([strip_accents(w.strip('?,.¿()¡!')) for w in question_lower.split() if len(w.strip('?,.¿()¡!')) > 1])
        question_lemmas = set([strip_accents(lemma) for lemma in self.get_lemmas(question_lower)])
        
        # Diccionario técnico para validar acrónimos y términos clave (Usamos lemas/raíces)
        tech_dictionary = {
            "cups", "dx", "kpi", "json", "parquet", "bigquery", "airflow", "cala", "web", "app", "call", 
            "medico", "diagnostico", "atencion", "facturado", "calidad", "error", "tasa", "arquitectura", 
            "propuesta", "pipeline", "transaccional", "procesamiento", "duplicado", "limpieza", 
            "ciudad", "clasificacion", "salud", "procedimiento", "cie-10", "identificar", "intervencion",
            "login", "click", "compra", "autenticacion", "interaccion", "digital", "politica", "total", 
            "promedio", "evento", "instruccion", "prueba", "cliente", "csv", "json_detalle", "orquestacion", 
            "gcp", "nube", "fallo", "sistema", "valido", "procesada", "descartado", "facturacion",
            "distribucion", "canal", "alfanumerico", "estandarizada", "reporte", "resultado", "financiero",
            "documento", "limpiar", "objetivo", "insumo", "particion", "consulta", "optimizado", 
            "idempotente", "despliegue", "composer", "faiss", "endpoint", "health", "ask", "tecnico", "tecnica"
        }
        
        # Bloqueo explícito de ruidos conocidos
        noise_triggers = {"sol", "pizza", "color", "clima", "mundial", "dolar", "helado", "avion", "gato", "presidente", "musica", "hambre", "francia", "pasta", "precio", "capital", "vendes", "volar", "receta", "cuanto", "mas"}
        
        # Validación técnica basada en lemas
        tech_overlap = question_lemmas.intersection(tech_dictionary)
        if not tech_overlap:
            tech_overlap = question_words.intersection(tech_dictionary)
        is_technical_query = bool(tech_overlap)
        
        # Bloqueo de ruido: Si tiene disparador de ruido Y NO es técnico, bloquear.
        if (question_words.intersection(noise_triggers)) and not is_technical_query:
             return "Lo siento, no tengo información sobre temas fuera del dominio de CALA Analytics. Mi especialidad son los procesos, KPIs y definiciones técnicas del proyecto."

        # ELEGIR EL MEJOR RESULTADO (CON RERANKING LEXICAL)
        scored_results = []
        for res, original_score in zip(results, scores):
            text_lower = res['text'].lower()
            text_lemmas = set([strip_accents(lemma) for lemma in self.get_lemmas(res['text'])])
            
            # Validar si este documento específico contiene la palabra técnica de la pregunta
            chunk_has_tech = False
            if is_technical_query:
                chunk_has_tech = any(w in text_lemmas or w in strip_accents(text_lower) for w in tech_overlap)
            
            # BONUS LEXICAL: Reranking para priorizar glosarios
            adjusted_score = original_score
            if chunk_has_tech:
                if res['file'] in ['definiciones.md', 'glosario_eventos.md']:
                    adjusted_score -= 0.6  # Fuerte bono para definiciones directas
                elif res['file'] in ['faq_operativa.md', 'politicas.md', 'instrucciones.md']:
                    adjusted_score -= 0.3  # Bono moderado
            
            adjusted_score = max(0.0, adjusted_score)
            scored_results.append((res, adjusted_score, original_score, chunk_has_tech))
            
        # Reordenamos por el score ajustado (menor es mejor en FAISS L2)
        scored_results.sort(key=lambda x: x[1])
        
        best_candidate = None
        best_score = 999
        
        for res, adjusted_score, original_score, chunk_has_tech in scored_results:
            current_threshold = 1.7 if chunk_has_tech else 1.15
            if adjusted_score <= current_threshold:
                best_candidate = res
                best_score = max(0.01, original_score) # Retenemos el puntaje real para el % de confianza
                break
        
        if best_candidate is None:
            return (
                "Lo siento, la información solicitada no se encuentra explícita en los manuales técnicos de CALA. "
                "Por favor, intenta con otra pregunta relacionada con KPIs o procesos del pipeline."
            )

        text = best_candidate['text']
        response = (
            f"Según mis archivos ({best_candidate['file']}):\n\n"
            f"{text}\n\n"
            f"---\n"
            f"Nota: Recuperación semántica multi-rango validada con {round(1/ (1 + best_score), 2)*100}% de confianza."
        )
        return response

if __name__ == "__main__":
    rag = RAGSystem(kb_dir="data/raw/kb")
    rag.load_and_chunk()
    rag.build_index()
    print(rag.ask("que es un cups?"))
