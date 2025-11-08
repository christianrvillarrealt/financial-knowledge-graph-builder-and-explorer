import json
import time
import hashlib
import requests
import gzip
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import kagglehub
import feedparser

# ============================================================================ #
#                           STAGE A: DATA INGESTION
# ============================================================================ #

class FinancialNewsIngestionService:
    """Stage A: Collect data from multiple financial news sources"""
    
    def __init__(self, base_data_dir: str):
        self.base_dir = Path(base_data_dir)
        self.raw_dir = self.base_dir / "raw"
        self.archival_dir = self.base_dir / "archival" 
        self.metadata_dir = self.base_dir / "metadata"
        self._create_directory_structure()
        
        self.kaggle_datasets = [
            "notlucasp/financial-news-headlines",
            "johoetter/labeled-stock-news-headlines", 
            "sbhatti/financial-sentiment-analysis",
            "subhojitmukherjee/financial-question-answering"
        ]

    def _create_directory_structure(self):
        """Create organized data directory structure"""
        directories = [
            self.raw_dir / "newsapi", self.raw_dir / "alpha_vantage",
            self.raw_dir / "fmp", self.raw_dir / "yahoo_rss",
            self.archival_dir / "kaggle_datasets", self.metadata_dir,
            self.base_dir / "processed", self.base_dir / "neo4j_ready"
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        print("✅ Directory structure created")

    def fetch_newsapi(self, api_key: str, query: str = "financial OR earnings OR stock", page_size: int = 100):
        """Fetch from NewsAPI"""
        print(f"📰 Fetching NewsAPI: {query}")
        url = "https://newsapi.org/v2/everything"
        params = {
            "q": query, "pageSize": page_size, "sortBy": "publishedAt",
            "apiKey": api_key, "language": "en",
            "domains": "reuters.com,bloomberg.com,wsj.com,ft.com,marketwatch.com"
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"❌ NewsAPI error: {response.status_code} - {response.text}")
                return []
                
            articles = response.json().get("articles", [])
            normalized = [self._normalize_article(art, "newsapi") for art in articles]
            print(f"✅ Fetched {len(normalized)} articles from NewsAPI")
            return normalized
        except Exception as e:
            print(f"❌ NewsAPI error: {e}")
            return []

    def fetch_alpha_vantage(self, api_key: str, tickers: List[str] = None):
        """Fetch from Alpha Vantage"""
        if tickers is None:
            tickers = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]
        
        print(f"📈 Fetching Alpha Vantage for {len(tickers)} tickers")
        all_articles = []
        
        for ticker in tickers:
            try:
                url = "https://www.alphavantage.co/query"
                params = {"function": "NEWS_SENTIMENT", "tickers": ticker, "apikey": api_key, "limit": 50}
                response = requests.get(url, params=params)
                
                if response.status_code != 200:
                    print(f"❌ Alpha Vantage error for {ticker}: {response.status_code}")
                    continue
                    
                data = response.json()
                feed_items = data.get("feed", [])
                
                for item in feed_items:
                    normalized = self._normalize_alpha_vantage_article(item, ticker)
                    all_articles.append(normalized)
                
                print(f"✅ Fetched {len(feed_items)} articles for {ticker}")
                time.sleep(12)  # Rate limiting
            except Exception as e:
                print(f"❌ Alpha Vantage error for {ticker}: {e}")
        
        return all_articles

    def fetch_yahoo_rss_news(self):
        """Fetch from Yahoo Finance RSS"""
        print("📰 Fetching Yahoo RSS news...")
        rss_feeds = {
            "AAPL": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AAPL&region=US&lang=en",
            "MSFT": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=MSFT&region=US&lang=en", 
            "GOOG": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOOG&region=US&lang=en",
            "TSLA": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=TSLA&region=US&lang=en",
            "AMZN": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AMZN&region=US&lang=en",
        }
        
        all_articles = []
        for feed_name, feed_url in rss_feeds.items():
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:10]:
                    article = self._normalize_rss_article(entry, feed_name)
                    all_articles.append(article)
                print(f"✅ Collected {len(feed.entries)} from {feed_name}")
                time.sleep(1)
            except Exception as e:
                print(f"❌ RSS error for {feed_name}: {e}")
        
        return all_articles

    def download_kaggle_datasets(self):
        """Download Kaggle datasets"""
        print("🚀 Downloading Kaggle datasets...")
        for dataset in self.kaggle_datasets:
            try:
                path = kagglehub.dataset_download(dataset)
                print(f"✅ Downloaded: {dataset} to {path}")
                time.sleep(2)
            except Exception as e:
                print(f"❌ Kaggle error for {dataset}: {e}")

    def _normalize_article(self, raw: Dict, source_type: str) -> Dict:
        """Normalize article to standard schema"""
        content = raw.get("content") or raw.get("description", "")
        url = raw.get("url", "")
        article_id = f"{source_type}_{hashlib.md5(url.encode()).hexdigest()[:16]}"
        
        return {
            "id": article_id, "source": raw.get("source", {}).get("name", "unknown"),
            "source_type": source_type, "url": url, "title": raw.get("title", ""),
            "full_text": content, "author": raw.get("author", ""), "language": "en",
            "published_at": self._normalize_date(raw.get("publishedAt")),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "tickers_mentioned": self._extract_tickers(content + " " + raw.get("title", "")),
            "ingestion_metadata": {"checksum": hashlib.sha256(content.encode()).hexdigest()}
        }

    def _normalize_alpha_vantage_article(self, raw: Dict, ticker: str) -> Dict:
        """Normalize Alpha Vantage article"""
        url = raw.get("url", "")
        article_id = f"alphavantage_{hashlib.md5(url.encode()).hexdigest()[:16]}"
        
        tickers_mentioned = []
        if "ticker_sentiment" in raw:
            for sentiment in raw["ticker_sentiment"]:
                if ticker_symbol := sentiment.get("ticker"):
                    tickers_mentioned.append(ticker_symbol)
        
        return {
            "id": article_id, "source": "Alpha Vantage", "source_type": "alpha_vantage",
            "url": url, "title": raw.get("title", ""), "full_text": raw.get("summary", ""),
            "published_at": self._normalize_date(raw.get("time_published")),
            "scraped_at": datetime.now(timezone.utc).isoformat(), "author": "",
            "tickers_mentioned": tickers_mentioned, "language": "en",
            "ingestion_metadata": {"checksum": hashlib.sha256(raw.get("summary", "").encode()).hexdigest()}
        }

    def _normalize_rss_article(self, entry: Dict, feed_name: str) -> Dict:
        """Normalize RSS article"""
        url_hash = hashlib.sha256(entry.link.encode()).hexdigest()[:16]
        article_id = f"yahoo_{url_hash}"
        
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            published_dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            published_iso = published_dt.isoformat()
        else:
            published_iso = datetime.now(timezone.utc).isoformat()
        
        return {
            "id": article_id, "source": "yahoo_finance_rss", "source_type": "yahoo_rss",
            "feed": feed_name, "url": entry.link, "title": entry.title, 
            "full_text": entry.summary, "author": getattr(entry, 'author', ''),
            "published_at": published_iso, "scraped_at": datetime.now(timezone.utc).isoformat(),
            "language": "en", "tickers_mentioned": [feed_name],
            "ingestion_metadata": {"checksum_sha256": hashlib.sha256(entry.summary.encode()).hexdigest()}
        }

    def _normalize_date(self, date_str: Optional[str]) -> str:
        """Normalize date to ISO format"""
        if not date_str:
            return datetime.now(timezone.utc).isoformat()
        try:
            if 'Z' in date_str:
                date_str = date_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except:
            return datetime.now(timezone.utc).isoformat()

    def _extract_tickers(self, text: str) -> List[str]:
        """Extract stock tickers from text"""
        patterns = [r'\$([A-Z]{1,5})\b', r'\(([A-Z]{1,5}):\s', r'\b([A-Z]{2,5})\s+stock']
        tickers = set()
        for pattern in patterns:
            matches = re.findall(pattern, text.upper())
            tickers.update(matches)
        return list(tickers)

    def run_ingestion(self, api_keys: Dict):
        """Run complete data ingestion"""
        print("🚀 STAGE A: Data Ingestion Started")
        all_articles = []
        
        # Fetch from APIs
        if api_keys.get('newsapi'):
            all_articles.extend(self.fetch_newsapi(api_keys['newsapi']))
        
        if api_keys.get('alpha_vantage'):
            time.sleep(60)  # Rate limiting
            all_articles.extend(self.fetch_alpha_vantage(api_keys['alpha_vantage']))
        
        all_articles.extend(self.fetch_yahoo_rss_news())
        
        # Download Kaggle datasets
        self.download_kaggle_datasets()
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for source_type in ["newsapi", "alpha_vantage", "yahoo_rss"]:
            source_articles = [a for a in all_articles if a["source_type"] == source_type]
            if source_articles:
                output_file = self.raw_dir / source_type / f"{timestamp}.jsonl.gz"
                with gzip.open(output_file, 'wt', encoding='utf-8') as f:
                    for article in source_articles:
                        f.write(json.dumps(article) + '\n')
                print(f"💾 Saved {len(source_articles)} {source_type} articles")
        
        print(f"✅ STAGE A Complete: {len(all_articles)} total articles")
        return all_articles