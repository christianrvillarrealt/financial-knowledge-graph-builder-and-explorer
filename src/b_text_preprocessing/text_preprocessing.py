import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from bs4 import BeautifulSoup
import unicodedata

# ============================================================================ #
#                           STAGE B: TEXT PREPROCESSING
# ============================================================================ #

class TextPreprocessor:
    """Stage B: Clean, normalize, and chunk text for LLM processing"""
    
    def __init__(self, base_dir: str, chunk_size: int = 1000, overlap: int = 100):
        self.base_dir = Path(base_dir)
        self.chunk_size = chunk_size
        self.overlap = overlap

    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        text = unicodedata.normalize("NFKD", text)
        text = BeautifulSoup(text, "html.parser").get_text()
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def create_chunks(self, text: str, title: str = "") -> List[Dict]:
        """Split text into overlapping chunks"""
        sentences = re.split(r"[.!?]+\s+", text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        chunks, current_chunk, current_length = [], [], 0
        if title:
            current_chunk.append(title)
            current_length = len(title)

        for sentence in sentences:
            sentence_length = len(sentence)
            if current_length + sentence_length > self.chunk_size and current_chunk:
                chunk_text = " ".join(current_chunk)
                chunks.append({
                    "chunk_id": f"chunk_{len(chunks)}",
                    "text": chunk_text,
                    "char_length": len(chunk_text),
                })
                # Keep overlap sentences
                overlap_sents = []
                overlap_chars = 0
                for s in reversed(current_chunk):
                    if overlap_chars + len(s) > self.overlap:
                        break
                    overlap_sents.insert(0, s)
                    overlap_chars += len(s)
                current_chunk = overlap_sents + [sentence]
                current_length = sum(len(s) for s in current_chunk)
            else:
                current_chunk.append(sentence)
                current_length += sentence_length

        if current_chunk:
            chunk_text = " ".join(current_chunk)
            chunks.append({
                "chunk_id": f"chunk_{len(chunks)}",
                "text": chunk_text,
                "char_length": len(chunk_text),
            })
        return chunks

    def process_articles(self, articles: List[Dict]) -> pd.DataFrame:
        """Process all articles into chunks"""
        all_chunks = []
        
        for article in articles:
            clean_text = self.clean_text(article.get("full_text", ""))
            chunks = self.create_chunks(clean_text, article.get("title", ""))
            
            for chunk in chunks:
                all_chunks.append({
                    "article_id": article["id"],
                    "source": article.get("source", "unknown"),
                    "title": article.get("title", ""),
                    "url": article.get("url", ""),
                    "published_at": article.get("published_at", ""),
                    "chunk_id": chunk["chunk_id"],
                    "chunk_text": chunk["text"],
                    "char_length": chunk["char_length"],
                    "tickers_mentioned": article.get("tickers_mentioned", []),
                })
        
        return pd.DataFrame(all_chunks)

    def run_preprocessing(self, articles: List[Dict]) -> str:
        """Run complete preprocessing"""
        print("🚀 STAGE B: Text Preprocessing Started")
        
        df_chunks = self.process_articles(articles)
        
        # Save processed data
        output_file = self.base_dir / "processed" / f"preprocessed_chunks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df_chunks.to_parquet(output_file, index=False)
        
        print(f"✅ STAGE B Complete: {len(df_chunks)} chunks saved to {output_file}")
        return str(output_file)