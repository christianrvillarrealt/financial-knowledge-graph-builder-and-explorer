#!/usr/bin/env python3
"""
Complete Financial Knowledge Graph Construction Pipeline
Integrates Stages A-F: Data Ingestion → Preprocessing → Extraction → Resolution → Neo4j Loading → Enhanced Query Service
"""

import os
import json
import gzip
import argparse
from pathlib import Path
from typing import Dict, List

from load_env import *

from src.a_data_ingestion.data_ingestion import FinancialNewsIngestionService
from src.b_text_preprocessing.text_preprocessing import TextPreprocessor
from src.c_entity_extraction.entity_extraction import EntityExtractionService
from src.d_entity_resolution.entity_resolution import EntityResolutionService
from src.e_neo4j_loading.neo4j_loading import Neo4jGraphLoader
from src.f_query_service.natural_language_query_service import NaturalLanguageQueryService

# Validate required environment variables
def validate_config():
    """Validate required environment variables"""
    required_vars = ["OPENAI_API_KEY", "NEWSAPI_KEY", "ALPHA_VANTAGE_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("\nPlease create a .env file with the following variables:")
        print("OPENAI_API_KEY=your_openai_key")
        print("NEWSAPI_KEY=your_newsapi_key")
        print("ALPHA_VANTAGE_KEY=your_alpha_vantage_key")
        print("NEO4J_URI=bolt://localhost:7687")
        print("NEO4J_USER=neo4j")
        print("NEO4J_PASSWORD=your_password")
        print("DOCKER_CONTAINER=financial-neo4j")
        print("BASE_DATA_DIR=./financial_news_data")
        return False
    
    print("✅ Configuration validated successfully")
    return True

# ============================================================================ #
#                           MAIN PIPELINE CONTROLLER
# ============================================================================ #

class KnowledgeGraphPipeline:
    """Complete pipeline controller with all stages A-F"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.base_dir = Path(config["base_data_dir"])
        self.query_service = None
    
    def run_complete_pipeline(
        self, 
        run_ingestion: bool = True, 
        sample_size: int = 50,
        skip_to_stage: str = None,
        interactive_mode: bool = True
    ):
        """
        Run the complete A-F pipeline with enhanced query service
        
        Args:
            run_ingestion: If True, fetch fresh data. If False, use existing data
            sample_size: Number of chunks to process for entity extraction
            skip_to_stage: Skip to specific stage (A, B, C, D, E, F)
            interactive_mode: If True, start interactive query session after loading
        """
        print("╔" + "═" * 78 + "╗")
        print("║" + " " * 15 + "FINANCIAL KNOWLEDGE GRAPH PIPELINE" + " " * 29 + "║")
        print("╚" + "═" * 78 + "╝")
        print()
        
        articles = None
        processed_path = None
        extracted_path = None
        neo4j_data = None
        
        try:
            # ================================================================ #
            # STAGE A: DATA INGESTION
            # ================================================================ #
            if skip_to_stage is None or skip_to_stage == 'A':
                print("\n┌" + "─" * 78 + "┐")
                print("│ STAGE A: DATA INGESTION" + " " * 54 + "│")
                print("└" + "─" * 78 + "┘")
                
                if run_ingestion:
                    ingestion = FinancialNewsIngestionService(self.config["base_data_dir"])
                    api_keys = {
                        'newsapi': self.config["newsapi_key"],
                        'alpha_vantage': self.config["alpha_vantage_key"],
                        'fmp': self.config["fmp_key"]
                    }
                    articles = ingestion.run_ingestion(api_keys)
                    print(f"\n✅ Stage A Complete: {len(articles)} articles collected")
                else:
                    print("ℹ️  Skipping data ingestion, loading existing articles...")
                    articles = self._load_existing_articles()
                    print(f"✅ Loaded {len(articles)} existing articles")
            
            # ================================================================ #
            # STAGE B: TEXT PREPROCESSING
            # ================================================================ #
            if skip_to_stage is None or skip_to_stage in ['A', 'B']:
                print("\n┌" + "─" * 78 + "┐")
                print("│ STAGE B: TEXT PREPROCESSING" + " " * 50 + "│")
                print("└" + "─" * 78 + "┘")
                
                if articles is None:
                    articles = self._load_existing_articles()
                
                preprocessor = TextPreprocessor(self.config["base_data_dir"])
                processed_path = preprocessor.run_preprocessing(articles)
                print(f"\n✅ Stage B Complete: Text preprocessed and saved to {processed_path}")
            
            # ================================================================ #
            # STAGE C: ENTITY EXTRACTION
            # ================================================================ #
            if skip_to_stage is None or skip_to_stage in ['A', 'B', 'C']:
                print("\n┌" + "─" * 78 + "┐")
                print("│ STAGE C: ENTITY EXTRACTION" + " " * 51 + "│")
                print("└" + "─" * 78 + "┘")
                
                if processed_path is None:
                    processed_path = self._get_latest_processed_file()
                
                extractor = EntityExtractionService(self.config["openai_api_key"])
                extracted_path = extractor.run_extraction(processed_path, sample_size)
                print(f"\n✅ Stage C Complete: Entities extracted and saved to {extracted_path}")
            
            # ================================================================ #
            # STAGE D: ENTITY RESOLUTION
            # ================================================================ #
            if skip_to_stage is None or skip_to_stage in ['A', 'B', 'C', 'D']:
                print("\n┌" + "─" * 78 + "┐")
                print("│ STAGE D: ENTITY RESOLUTION" + " " * 51 + "│")
                print("└" + "─" * 78 + "┘")
                
                if extracted_path is None:
                    extracted_path = self._get_latest_extracted_file()
                
                resolver = EntityResolutionService(self.config["openai_api_key"])
                neo4j_data = resolver.run_resolution(extracted_path)
                print(f"\n✅ Stage D Complete: {len(neo4j_data['nodes'])} nodes, {len(neo4j_data['relations'])} relations")
            
            # ================================================================ #
            # STAGE E: NEO4J LOADING
            # ================================================================ #
            if skip_to_stage is None or skip_to_stage in ['A', 'B', 'C', 'D', 'E']:
                print("\n┌" + "─" * 78 + "┐")
                print("│ STAGE E: NEO4J LOADING" + " " * 55 + "│")
                print("└" + "─" * 78 + "┘")
                
                if neo4j_data is None:
                    print("⚠️  No neo4j_data available. Please run previous stages first.")
                    return False
                
                loader = Neo4jGraphLoader(
                    self.config["neo4j_uri"],
                    self.config["neo4j_user"], 
                    self.config["neo4j_password"],
                    self.config["docker_container"]
                )
                
                loading_success = loader.run_loading(neo4j_data)
                
                if not loading_success:
                    print("\n❌ Stage E Failed: Could not load data into Neo4j")
                    print("Please check Neo4j connection and try again")
                    return False
                
                print(f"\n✅ Stage E Complete: Data loaded into Neo4j successfully")
            
            # ================================================================ #
            # STAGE F: NATURAL LANGUAGE QUERY SERVICE
            # ================================================================ #
            print("\n┌" + "─" * 78 + "┐")
            print("│ STAGE F: NATURAL LANGUAGE QUERY SERVICE" + " " * 38 + "│")
            print("└" + "─" * 78 + "┘")
            
            print("\n🚀 Initializing Natural Language Query Service...")
            self.query_service = NaturalLanguageQueryService(
                neo4j_uri=self.config["neo4j_uri"],
                neo4j_user=self.config["neo4j_user"],
                neo4j_password=self.config["neo4j_password"],
                openai_api_key=self.config["openai_api_key"]
            )
            
            print("✅ Query Service Initialized")
            
            # Run some example queries to test the system
            print("\n📊 Testing query service with sample queries...")
            self._run_test_queries()
            
            # ================================================================ #
            # PIPELINE COMPLETE
            # ================================================================ #
            print("\n╔" + "═" * 78 + "╗")
            print("║" + " " * 20 + "🎉 PIPELINE COMPLETE! 🎉" + " " * 34 + "║")
            print("╚" + "═" * 78 + "╝")
            
            # Enter interactive mode if requested
            if interactive_mode:
                print("\n🎯 Starting Interactive Query Mode...")
                print("   Type your natural language queries to explore the knowledge graph")
                print("   Type 'exit' to quit\n")
                self.query_service.interactive_query_session()
            
            return True
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Pipeline interrupted by user")
            return False
        except Exception as e:
            print(f"\n\n❌ Pipeline error: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            # Clean up
            if self.query_service:
                self.query_service.close()
    
    def _run_test_queries(self):
        """Run test queries to validate the system"""
        test_queries = [
            "Show me 5 companies in the knowledge graph",
            "Find entities related to Apple"
        ]
        
        for query in test_queries:
            print(f"\n🔍 Test Query: '{query}'")
            result = self.query_service.query(query, explain=False)
            
            if result["success"]:
                print(f"   ✅ Found {result['result_count']} results")
            else:
                print(f"   ⚠️  Query returned no results")
    
    def _load_existing_articles(self) -> List[Dict]:
        """Load existing articles from raw data storage"""
        articles = []
        raw_dir = self.base_dir / "raw"
        
        if not raw_dir.exists():
            print(f"⚠️  Raw data directory not found: {raw_dir}")
            return articles
        
        for source_dir in raw_dir.iterdir():
            if source_dir.is_dir():
                for jsonl_file in source_dir.glob("*.jsonl.gz"):
                    try:
                        with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                            for line in f:
                                if line.strip():
                                    articles.append(json.loads(line))
                    except Exception as e:
                        print(f"⚠️  Error reading {jsonl_file}: {e}")
        
        return articles
    
    def _get_latest_processed_file(self) -> str:
        """Get the most recent preprocessed file"""
        processed_dir = self.base_dir / "processed"
        if not processed_dir.exists():
            raise FileNotFoundError("No processed files found. Please run Stage B first.")
        
        files = list(processed_dir.glob("preprocessed_chunks_*.parquet"))
        if not files:
            raise FileNotFoundError("No preprocessed files found. Please run Stage B first.")
        
        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        print(f"ℹ️  Using latest preprocessed file: {latest_file}")
        return str(latest_file)
    
    def _get_latest_extracted_file(self) -> str:
        """Get the most recent extracted file"""
        processed_dir = self.base_dir / "processed"
        if not processed_dir.exists():
            raise FileNotFoundError("No extracted files found. Please run Stage C first.")
        
        files = list(processed_dir.glob("*_extracted.parquet"))
        if not files:
            raise FileNotFoundError("No extracted files found. Please run Stage C first.")
        
        latest_file = max(files, key=lambda p: p.stat().st_mtime)
        print(f"ℹ️  Using latest extracted file: {latest_file}")
        return str(latest_file)
    
    def query_graph(self, natural_language_query: str):
        """Query the knowledge graph using natural language"""
        if self.query_service is None:
            print("⚠️  Query service not initialized. Please run the pipeline first.")
            return None
        
        return self.query_service.query(natural_language_query, explain=True)

# ============================================================================ #
#                           WEB SERVICE API (Optional)
# ============================================================================ #

_global_query_service = None

def init_query_service_for_web():
    """Initialize query service for web interface integration"""
    global _global_query_service
    
    if not validate_config():
        raise Exception("Configuration validation failed")
    
    _global_query_service = NaturalLanguageQueryService(
        neo4j_uri=CONFIG["neo4j_uri"],
        neo4j_user=CONFIG["neo4j_user"],
        neo4j_password=CONFIG["neo4j_password"],
        openai_api_key=CONFIG["openai_api_key"]
    )
    return _global_query_service

def web_query(nl_query: str) -> Dict:
    """Execute natural language query for web interface"""
    if _global_query_service is None:
        raise Exception("Query service not initialized. Call init_query_service_for_web() first.")
    return _global_query_service.query(nl_query)

def get_sample_queries_for_web() -> List[str]:
    """Get sample queries for web interface"""
    if _global_query_service is None:
        raise Exception("Query service not initialized. Call init_query_service_for_web() first.")
    return _global_query_service.get_sample_queries()

# ============================================================================ #
#                           COMMAND LINE INTERFACE
# ============================================================================ #

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Financial Knowledge Graph Construction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline with fresh data
  python main.py --full
  
  # Run with existing data (skip ingestion)
  python main.py --use-existing
  
  # Run with smaller sample for testing
  python main.py --full --sample-size 20
  
  # Skip to specific stage
  python main.py --skip-to F
  
  # Run without interactive mode
  python main.py --full --no-interactive
        """
    )
    
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run complete pipeline with fresh data ingestion"
    )
    
    parser.add_argument(
        "--use-existing",
        action="store_true",
        help="Use existing data (skip data ingestion)"
    )
    
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50,
        help="Number of chunks to process for entity extraction (default: 50)"
    )
    
    parser.add_argument(
        "--skip-to",
        type=str,
        choices=['A', 'B', 'C', 'D', 'E', 'F'],
        help="Skip to specific stage (A=Ingestion, B=Preprocessing, C=Extraction, D=Resolution, E=Loading, F=Query)"
    )
    
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="Skip interactive query mode after pipeline completion"
    )
    
    parser.add_argument(
        "--query",
        type=str,
        help="Execute a single query and exit (requires existing knowledge graph)"
    )
    
    return parser.parse_args()

# ============================================================================ #
#                           MAIN EXECUTION
# ============================================================================ #

def main():
    """Main execution function"""
    args = parse_arguments()
    
    # Print banner
    print("\n" + "=" * 80)
    print(" " * 15 + "FINANCIAL KNOWLEDGE GRAPH CONSTRUCTION")
    print(" " * 20 + "Stages A-F Pipeline with NL Query")
    print("=" * 80 + "\n")
    
    # Validate configuration
    if not validate_config():
        return 1
    
    # Initialize pipeline
    pipeline = KnowledgeGraphPipeline(CONFIG)
    
    # Handle single query mode
    if args.query:
        print(f"🔍 Executing query: '{args.query}'\n")
        query_service = NaturalLanguageQueryService(
            neo4j_uri=CONFIG["neo4j_uri"],
            neo4j_user=CONFIG["neo4j_user"],
            neo4j_password=CONFIG["neo4j_password"],
            openai_api_key=CONFIG["openai_api_key"]
        )
        result = query_service.query(args.query, explain=True)
        print(f"\n📊 Results:\n{json.dumps(result, indent=2, default=str)}")
        query_service.close()
        return 0
    
    # Determine run mode
    if args.full:
        run_ingestion = True
    elif args.use_existing:
        run_ingestion = False
    else:
        # Interactive prompt
        print("Choose pipeline mode:")
        print("1. Full pipeline (fetch fresh data)")
        print("2. Use existing data (skip data ingestion)")
        choice = input("\nEnter choice (1 or 2): ").strip()
        run_ingestion = (choice == "1")
    
    # Run pipeline
    success = pipeline.run_complete_pipeline(
        run_ingestion=run_ingestion,
        sample_size=args.sample_size,
        skip_to_stage=args.skip_to,
        interactive_mode=not args.no_interactive
    )
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())