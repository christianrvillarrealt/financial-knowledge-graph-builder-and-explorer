import uuid
import pandas as pd
from typing import Dict
import openai

# ============================================================================ #
#                           STAGE D: ENTITY RESOLUTION
# ============================================================================ #

class EntityResolutionService:
    """Stage D: Clean, canonicalize, and prepare for Neo4j"""
    
    def __init__(self, llm_api_key: str):
        self.entity_cache = {}
        self.llm_client = openai.OpenAI(api_key=llm_api_key)
        self.VALID_LABELS = {"Person", "Company", "Product", "Event", "Entity"}

    def infer_type_llm(self, name: str) -> str:
        """Use LLM to assign Neo4j labels with consistent casing"""
        prompt = f"""
Given entity name, respond with ONLY: Person, Company, Product, Event, or Entity.
Entity: "{name}"
Respond with only the label.
"""
        try:
            response = self.llm_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            label = response.choices[0].message.content.strip()
            return label if label in self.VALID_LABELS else "Entity"
        except:
            return self._heuristic_type(name)

    def _heuristic_type(self, name: str) -> str:
        """Fallback type inference"""
        if not name: return "Entity"
        name_lower = name.lower()
        if any(w in name_lower for w in ["inc", "corp", "company", "bank", "fund"]):
            return "Company"
        if any(w in name_lower for w in ["mr", "dr", "ceo", "president"]):
            return "Person"
        return "Entity"

    def get_or_create_entity_id(self, name: str) -> str:
        """Create consistent UUID for entity"""
        key = name.strip().lower()
        if not key: return None
        if key not in self.entity_cache:
            self.entity_cache[key] = str(uuid.uuid5(uuid.NAMESPACE_DNS, key))
        return self.entity_cache[key]

    def clean_extraction_row(self, row: Dict) -> Dict:
        """Clean and normalize extracted data"""
        entities, relations = row.get("entities", []), row.get("relations", [])
        
        entity_objs = []
        for e in entities:
            e_str = str(e).strip()
            if not e_str: continue
            e_id = self.get_or_create_entity_id(e_str)
            e_type = self.infer_type_llm(e_str)
            entity_objs.append({
                "id": e_id, "name": e_str, "label": e_type,
                "source": row.get("source"), "article_id": row.get("article_id"),
                "published_at": row.get("published_at"),
            })

        relation_objs = []
        for r in relations:
            subj, rel, obj = r.get("subject"), r.get("relation"), r.get("object")
            if not all([subj, obj, rel]): continue
            
            sid = self.get_or_create_entity_id(subj)
            oid = self.get_or_create_entity_id(obj)
            relation_objs.append({
                "start_id": sid, "end_id": oid, "type": rel.upper().replace(" ", "_"),
                "confidence": 0.9, "article_id": row.get("article_id"),
                "source": row.get("source"), "published_at": row.get("published_at"),
            })
        
        return {"entities": entity_objs, "relations": relation_objs}

    def run_resolution(self, extracted_path: str) -> Dict[str, pd.DataFrame]:
        """Run entity resolution and create Neo4j-ready data"""
        print("🚀 STAGE D: Entity Resolution Started")
        
        df = pd.read_parquet(extracted_path)
        all_entities, all_relations = [], []
        
        for _, row in df.iterrows():
            cleaned = self.clean_extraction_row(row)
            all_entities.extend(cleaned["entities"])
            all_relations.extend(cleaned["relations"])
        
        # Create final DataFrames
        nodes_df = pd.DataFrame(all_entities).drop_duplicates(subset=["id"])
        rels_df = pd.DataFrame(all_relations).drop_duplicates(subset=["start_id", "end_id", "type"])
        
        print(f"✅ STAGE D Complete: {len(nodes_df)} nodes, {len(rels_df)} relations")
        return {"nodes": nodes_df, "relations": rels_df}