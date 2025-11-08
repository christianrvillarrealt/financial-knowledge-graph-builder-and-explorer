import json
import time
import pandas as pd
import re
from typing import Dict
import openai

# ============================================================================ #
#                           STAGE C: ENTITY EXTRACTION
# ============================================================================ #

class EntityExtractionService:
    """Stage C: Extract entities and relationships using LLM"""
    
    def __init__(self, api_key: str):
        self.client = openai.OpenAI(api_key=api_key)

    def extract_from_chunk(self, chunk_text: str, metadata: Dict) -> Dict:
        """Extract entities and relationships from text chunk"""
        prompt = self._build_extraction_prompt(chunk_text)
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=1500,
            )
            content = response.choices[0].message.content
            data = self._parse_llm_response(content)
            data.update({
                "chunk_id": metadata.get("chunk_id"),
                "article_id": metadata.get("article_id"),
                "source": metadata.get("source"),
                "published_at": metadata.get("published_at"),
                "title": metadata.get("title"),
            })
            return data
        except Exception as e:
            return {
                "chunk_id": metadata.get("chunk_id"),
                "article_id": metadata.get("article_id"),
                "error": str(e),
                "entities": [],
                "relations": [],
            }

    def _build_extraction_prompt(self, text: str) -> str:
        return f"""
Extract all entities and relationships from this financial news text.
Return JSON with "entities" array and "relations" array.
Each relation: {{"subject": "...", "relation": "...", "object": "..."}}

Text: {text}

Example: {{"entities": ["Apple", "CEO"], "relations": [{{"subject": "Apple", "relation": "has CEO", "object": "Tim Cook"}}]}}
"""

    def _parse_llm_response(self, content: str) -> Dict:
        """Parse LLM response to extract JSON"""
        match = re.search(r"```json\n?(.*?)```", content, re.DOTALL)
        if match:
            content = match.group(1).strip()
        
        try:
            return json.loads(content)
        except:
            start, end = content.find("{"), content.rfind("}") + 1
            if 0 <= start < end:
                try:
                    return json.loads(content[start:end])
                except:
                    pass
        return {"entities": [], "relations": []}

    def run_extraction(self, parquet_path: str, sample_size: int = 100) -> str:
        """Run entity extraction on preprocessed data"""
        print("🚀 STAGE C: Entity Extraction Started")
        
        df = pd.read_parquet(parquet_path)
        results = []
        
        # Process sample for efficiency
        sample_df = df.head(sample_size)
        
        for idx, row in sample_df.iterrows():
            metadata = {
                "chunk_id": row["chunk_id"],
                "article_id": row["article_id"], 
                "source": row["source"],
                "title": row["title"],
                "published_at": row["published_at"],
            }
            result = self.extract_from_chunk(row["chunk_text"], metadata)
            results.append(result)
            
            if (idx + 1) % 10 == 0:
                print(f"📝 Processed {idx + 1}/{len(sample_df)} chunks")
                time.sleep(2)  # Rate limiting
        
        # Save results
        output_path = parquet_path.replace(".parquet", "_extracted.parquet")
        pd.DataFrame(results).to_parquet(output_path, index=False)
        
        print(f"✅ STAGE C Complete: {len(results)} chunks extracted to {output_path}")
        return output_path