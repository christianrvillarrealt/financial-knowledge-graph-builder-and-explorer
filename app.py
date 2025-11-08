#!/usr/bin/env python3
"""
Flask Web Application for Financial Knowledge Graph
Integrates with the complete A-F pipeline
"""

from flask import Flask, render_template, request, jsonify
from neo4j import GraphDatabase
import os
import traceback
import threading
import json
from dotenv import load_dotenv
from pathlib import Path

# ============================================================================ #
#                           LOAD ENVIRONMENT VARIABLES
# ============================================================================ #

load_dotenv()

# ============================================================================ #
#                           IMPORT PIPELINE MODULES
# ============================================================================ #

try:
    from src.a_data_ingestion.data_ingestion import FinancialNewsIngestionService
    from src.b_text_preprocessing.text_preprocessing import TextPreprocessor
    from src.c_entity_extraction.entity_extraction import EntityExtractionService
    from src.d_entity_resolution.entity_resolution import EntityResolutionService
    from src.e_neo4j_loading.neo4j_loading import Neo4jGraphLoader
    from src.f_query_service.natural_language_query_service import NaturalLanguageQueryService
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure all modules are in the src/ directory")

# ============================================================================ #
#                           CONFIGURATION
# ============================================================================ #

app = Flask(__name__)

CONFIG = {
    "openai_api_key": os.getenv("OPENAI_API_KEY"),
    "newsapi_key": os.getenv("NEWSAPI_KEY"),
    "alpha_vantage_key": os.getenv("ALPHA_VANTAGE_KEY"),
    "fmp_key": os.getenv("FMP_KEY"),
    "neo4j_uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    "neo4j_user": os.getenv("NEO4J_USER", "neo4j"),
    "neo4j_password": os.getenv("NEO4J_PASSWORD", "financial2025"),
    "docker_container": os.getenv("DOCKER_CONTAINER", "financial-neo4j"),
    "base_data_dir": os.getenv("BASE_DATA_DIR", "./financial_news_data")
}

# Validate configuration
if not CONFIG["openai_api_key"]:
    print("❌ ERROR: OPENAI_API_KEY is required")
    print("Please set it in your .env file")

# ============================================================================ #
#                           GLOBAL STATE
# ============================================================================ #

query_service = None
pipeline_status = {
    "running": False,
    "current_stage": "",
    "stage_name": "",
    "progress": 0,
    "message": "Ready to start pipeline",
    "error": "",
    "completed": False,
    "stats": {
        "articles_collected": 0,
        "chunks_created": 0,
        "entities_extracted": 0,
        "nodes_created": 0,
        "relationships_created": 0
    }
}

# ============================================================================ #
#                           INITIALIZE QUERY SERVICE
# ============================================================================ #

def init_query_service():
    """Initialize the natural language query service"""
    global query_service
    try:
        query_service = NaturalLanguageQueryService(
            neo4j_uri=CONFIG["neo4j_uri"],
            neo4j_user=CONFIG["neo4j_user"],
            neo4j_password=CONFIG["neo4j_password"],
            openai_api_key=CONFIG["openai_api_key"]
        )
        print("✅ Query service initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Error initializing query service: {e}")
        return False

# Initialize on startup
init_query_service()

# ============================================================================ #
#                           PIPELINE EXECUTION
# ============================================================================ #

def run_pipeline_async(sample_size=50, run_ingestion=True):
    """Run the complete pipeline in background thread"""
    global pipeline_status, query_service
    
    try:
        pipeline_status["running"] = True
        pipeline_status["error"] = ""
        pipeline_status["completed"] = False
        
        base_dir = Path(CONFIG["base_data_dir"])
        
        # ================================================================ #
        # STAGE A: DATA INGESTION
        # ================================================================ #
        pipeline_status.update({
            "current_stage": "A",
            "stage_name": "Data Ingestion",
            "progress": 10,
            "message": "Collecting financial news articles..."
        })
        
        if run_ingestion:
            ingestion = FinancialNewsIngestionService(CONFIG["base_data_dir"])
            api_keys = {
                'newsapi': CONFIG["newsapi_key"],
                'alpha_vantage': CONFIG["alpha_vantage_key"],
                'fmp': CONFIG["fmp_key"]
            }
            articles = ingestion.run_ingestion(api_keys)
        else:
            # Load existing articles
            articles = []
            raw_dir = base_dir / "raw"
            if raw_dir.exists():
                import gzip
                for source_dir in raw_dir.iterdir():
                    if source_dir.is_dir():
                        for jsonl_file in source_dir.glob("*.jsonl.gz"):
                            with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                                for line in f:
                                    if line.strip():
                                        articles.append(json.loads(line))
        
        pipeline_status["stats"]["articles_collected"] = len(articles)
        pipeline_status["message"] = f"Collected {len(articles)} articles"
        
        # ================================================================ #
        # STAGE B: TEXT PREPROCESSING
        # ================================================================ #
        pipeline_status.update({
            "current_stage": "B",
            "stage_name": "Text Preprocessing",
            "progress": 30,
            "message": "Preprocessing and chunking text..."
        })
        
        preprocessor = TextPreprocessor(CONFIG["base_data_dir"])
        processed_path = preprocessor.run_preprocessing(articles)
        
        # Count chunks
        import pandas as pd
        df_chunks = pd.read_parquet(processed_path)
        pipeline_status["stats"]["chunks_created"] = len(df_chunks)
        pipeline_status["message"] = f"Created {len(df_chunks)} text chunks"
        
        # ================================================================ #
        # STAGE C: ENTITY EXTRACTION
        # ================================================================ #
        pipeline_status.update({
            "current_stage": "C",
            "stage_name": "Entity Extraction",
            "progress": 50,
            "message": f"Extracting entities from {sample_size} chunks..."
        })
        
        extractor = EntityExtractionService(CONFIG["openai_api_key"])
        extracted_path = extractor.run_extraction(processed_path, sample_size)
        
        df_extracted = pd.read_parquet(extracted_path)
        pipeline_status["stats"]["entities_extracted"] = len(df_extracted)
        pipeline_status["message"] = f"Extracted entities from {len(df_extracted)} chunks"
        
        # ================================================================ #
        # STAGE D: ENTITY RESOLUTION
        # ================================================================ #
        pipeline_status.update({
            "current_stage": "D",
            "stage_name": "Entity Resolution",
            "progress": 70,
            "message": "Resolving and canonicalizing entities..."
        })
        
        resolver = EntityResolutionService(CONFIG["openai_api_key"])
        neo4j_data = resolver.run_resolution(extracted_path)
        
        pipeline_status["stats"]["nodes_created"] = len(neo4j_data["nodes"])
        pipeline_status["stats"]["relationships_created"] = len(neo4j_data["relations"])
        pipeline_status["message"] = f"Created {len(neo4j_data['nodes'])} nodes and {len(neo4j_data['relations'])} relationships"
        
        # ================================================================ #
        # STAGE E: NEO4J LOADING
        # ================================================================ #
        pipeline_status.update({
            "current_stage": "E",
            "stage_name": "Neo4j Loading",
            "progress": 90,
            "message": "Loading data into Neo4j..."
        })
        
        loader = Neo4jGraphLoader(
            CONFIG["neo4j_uri"],
            CONFIG["neo4j_user"],
            CONFIG["neo4j_password"],
            CONFIG["docker_container"]
        )
        
        loading_success = loader.run_loading(neo4j_data)
        
        if not loading_success:
            raise Exception("Failed to load data into Neo4j")
        
        # ================================================================ #
        # STAGE F: INITIALIZE QUERY SERVICE
        # ================================================================ #
        pipeline_status.update({
            "current_stage": "F",
            "stage_name": "Query Service",
            "progress": 95,
            "message": "Initializing query service..."
        })
        
        # Reinitialize query service to pick up new data
        init_query_service()
        
        # ================================================================ #
        # COMPLETE
        # ================================================================ #
        pipeline_status.update({
            "running": False,
            "current_stage": "Complete",
            "stage_name": "Complete",
            "progress": 100,
            "message": "Pipeline completed successfully!",
            "completed": True
        })
        
    except Exception as e:
        pipeline_status.update({
            "running": False,
            "error": str(e),
            "message": f"Pipeline failed: {str(e)}",
            "completed": False
        })
        print(f"Pipeline error: {traceback.format_exc()}")

# ============================================================================ #
#                           ROUTES - MAIN PAGE
# ============================================================================ #

@app.route("/")
def index():
    """Main page"""
    return render_template("index.html")

# ============================================================================ #
#                           ROUTES - PIPELINE CONTROL
# ============================================================================ #

@app.route("/api/pipeline/start", methods=["POST"])
def start_pipeline():
    """Start the knowledge graph construction pipeline"""
    global pipeline_status
    
    if pipeline_status["running"]:
        return jsonify({"error": "Pipeline is already running"}), 400
    
    data = request.json or {}
    sample_size = data.get("sample_size", 50)
    run_ingestion = data.get("run_ingestion", True)
    
    # Reset status
    pipeline_status = {
        "running": True,
        "current_stage": "Starting",
        "stage_name": "Initializing",
        "progress": 0,
        "message": "Starting pipeline...",
        "error": "",
        "completed": False,
        "stats": {
            "articles_collected": 0,
            "chunks_created": 0,
            "entities_extracted": 0,
            "nodes_created": 0,
            "relationships_created": 0
        }
    }
    
    # Start pipeline in background thread
    thread = threading.Thread(target=run_pipeline_async, args=(sample_size, run_ingestion))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "success": True,
        "message": "Pipeline started successfully"
    })

@app.route("/api/pipeline/status", methods=["GET"])
def get_pipeline_status():
    """Get current pipeline status"""
    return jsonify(pipeline_status)

# ============================================================================ #
#                           ROUTES - GRAPH STATISTICS
# ============================================================================ #

@app.route("/api/graph/stats", methods=["GET"])
def get_graph_stats():
    """Get graph statistics"""
    try:
        if not query_service:
            return jsonify({"error": "Query service not initialized"}), 500
        
        with query_service.driver.session() as session:
            # Get node count
            node_result = session.run("MATCH (n) RETURN count(n) as count")
            node_count = node_result.single()["count"]
            
            # Get relationship count
            rel_result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
            rel_count = rel_result.single()["count"]
            
            # Get label distribution
            label_result = session.run("""
                MATCH (n)
                UNWIND labels(n) AS label
                RETURN label, count(*) as count
                ORDER BY count DESC
                LIMIT 10
            """)
            labels = [{"label": record["label"], "count": record["count"]} 
                     for record in label_result]
            
            # Get relationship types
            rel_type_result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(*) as count
                ORDER BY count DESC
                LIMIT 10
            """)
            rel_types = [{"type": record["type"], "count": record["count"]} 
                        for record in rel_type_result]
            
            # Get source distribution
            source_result = session.run("""
                MATCH (n)
                WHERE n.source IS NOT NULL
                RETURN n.source as source, count(*) as count
                ORDER BY count DESC
                LIMIT 10
            """)
            sources = [{"source": record["source"], "count": record["count"]} 
                      for record in source_result]
            
            return jsonify({
                "success": True,
                "node_count": node_count,
                "relationship_count": rel_count,
                "labels": labels,
                "relationship_types": rel_types,
                "sources": sources
            })
            
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route("/api/graph/has_data", methods=["GET"])
def graph_has_data():
    """Check if the graph has any data"""
    try:
        if not query_service:
            return jsonify({"has_data": False, "error": "Query service not initialized"})
        
        with query_service.driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count LIMIT 1")
            count = result.single()["count"]
            
            return jsonify({
                "has_data": count > 0,
                "node_count": count
            })
            
    except Exception as e:
        return jsonify({
            "has_data": False,
            "error": str(e)
        })

# ============================================================================ #
#                           ROUTES - SCHEMA INFORMATION
# ============================================================================ #

@app.route("/api/schema", methods=["GET"])
def get_schema():
    """Get detailed schema information"""
    try:
        if not query_service:
            return jsonify({"error": "Query service not initialized"}), 500
        
        schema_info = {
            "node_properties": {
                "all_entities": [
                    {"name": "id", "type": "UUID", "description": "Unique identifier"},
                    {"name": "name", "type": "string", "description": "Entity name"},
                    {"name": "label", "type": "string", "description": "Entity type (Person, Company, Product, Event, Entity)"},
                    {"name": "article_id", "type": "string", "description": "Source article identifier"},
                    {"name": "source", "type": "string", "description": "Data source (MarketWatch, Bloomberg, etc.)"},
                    {"name": "published_at", "type": "ISO datetime", "description": "Publication timestamp"}
                ]
            },
            "relationship_properties": [
                {"name": "type", "type": "string", "description": "Relationship type (HAS, RELATED_TO, ANNOUNCED, etc.)"},
                {"name": "article_id", "type": "string", "description": "Source article identifier"},
                {"name": "source", "type": "string", "description": "Data source"},
                {"name": "published_at", "type": "ISO datetime", "description": "Publication timestamp"},
                {"name": "confidence", "type": "float", "description": "Confidence score (0.0-1.0)"}
            ],
            "node_labels": ["Person", "Company", "Product", "Event", "Entity"],
            "common_relationship_types": [
                "HAS", "RELATED_TO", "ANNOUNCED", "ACQUIRED", "INVESTED_IN",
                "PARTNERED_WITH", "APPOINTED", "RESIGNED", "LAUNCHED", "INCREASED", "DECREASED"
            ],
            "example_queries": [
                "Find all companies",
                "Show me entities related to Apple",
                "What products are mentioned?",
                "Find entities from MarketWatch",
                "Show me all relationships of type HAS"
            ]
        }
        
        return jsonify({
            "success": True,
            "schema": schema_info,
            "schema_context": query_service.schema_context if query_service else None
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# ============================================================================ #
#                           ROUTES - NATURAL LANGUAGE QUERIES
# ============================================================================ #

@app.route("/api/query", methods=["POST"])
def api_query():
    """Execute natural language query"""
    try:
        if not query_service:
            return jsonify({"error": "Query service not initialized"}), 500
        
        data = request.json
        nl_query = data.get("query", "")
        
        if not nl_query:
            return jsonify({"error": "Empty query"}), 400
        
        # Execute query using the natural language query service
        result = query_service.query(nl_query, explain=True)
        
        if result["success"]:
            # Format for frontend
            nodes = []
            edges = []
            
            for record in result["results"]:
                for key, value in record.items():
                    # Check if it's a node
                    if isinstance(value, dict) and 'name' in value:
                        nodes.append({
                            "id": value.get("id", ""),
                            "label": value.get("name", ""),
                            "type": value.get("label", "Entity"),
                            "source": value.get("source", ""),
                            "properties": value
                        })
            
            return jsonify({
                "success": True,
                "cypher_query": result["cypher"],
                "reasoning": result["explanation"],
                "nodes": nodes,
                "edges": edges,
                "raw_results": result["results"],
                "result_count": result["result_count"],
                "return_type": result.get("return_type", "unknown")
            })
        else:
            return jsonify({
                "success": False,
                "error": result.get("error", "Query failed"),
                "explanation": result.get("explanation", "")
            }), 500
            
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": str(e),
            "suggestion": "Try rephrasing your query or check the schema for valid entities."
        }), 500

@app.route("/api/query/analyze", methods=["POST"])
def analyze_query():
    """Analyze a query without executing it"""
    try:
        if not query_service:
            return jsonify({"error": "Query service not initialized"}), 500
        
        data = request.json
        nl_query = data.get("query", "")
        
        if not nl_query:
            return jsonify({"error": "Empty query"}), 400
        
        # Translate to Cypher
        translation = query_service.translate_query(nl_query)
        
        return jsonify({
            "success": True,
            "analysis": {
                "original_query": nl_query,
                "generated_cypher": translation.get("cypher", ""),
                "explanation": translation.get("explanation", ""),
                "return_type": translation.get("return_type", "unknown"),
                "parameters": translation.get("parameters", {})
            }
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# ============================================================================ #
#                           ROUTES - ENTITY SEARCH
# ============================================================================ #

@app.route("/api/search/entities", methods=["POST"])
def search_entities():
    """Search for entities by name"""
    try:
        if not query_service:
            return jsonify({"error": "Query service not initialized"}), 500
        
        data = request.json
        search_term = data.get("search_term", "")
        
        if not search_term:
            return jsonify({"error": "Empty search term"}), 400
        
        # Use query service to search
        cypher = f"""
        MATCH (n)
        WHERE toLower(n.name) CONTAINS toLower('{search_term}')
        RETURN n.name as name, labels(n) as labels, n.id as id, 
               n.source as source, n.label as entity_type
        ORDER BY n.name
        LIMIT 20
        """
        
        results = query_service.execute_query(cypher)
        
        entities = []
        for record in results:
            entities.append({
                "name": record.get("name", ""),
                "labels": record.get("labels", []),
                "id": record.get("id", ""),
                "source": record.get("source", ""),
                "entity_type": record.get("entity_type", "Entity")
            })
        
        return jsonify({
            "success": True,
            "search_term": search_term,
            "results": entities,
            "total_found": len(entities)
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# ============================================================================ #
#                           ROUTES - EXAMPLE QUERIES
# ============================================================================ #

@app.route("/api/examples", methods=["GET"])
def get_examples():
    """Get example queries"""
    if query_service:
        examples = query_service.get_sample_queries()
    else:
        examples = [
            "Find all companies mentioned in articles",
            "Show me entities related to Apple",
            "Find all articles from MarketWatch",
        ]
    
    return jsonify({
        "success": True,
        "examples": examples
    })

# ============================================================================ #
#                           ROUTES - GRAPH VISUALIZATION DATA
# ============================================================================ #

@app.route("/api/graph/sample", methods=["GET"])
def get_graph_sample():
    """Get a sample of the graph for visualization"""
    try:
        if not query_service:
            return jsonify({"error": "Query service not initialized"}), 500
        
        cypher = """
        MATCH (n)
        WITH n LIMIT 50
        OPTIONAL MATCH (n)-[r]->(m)
        RETURN n, r, m
        LIMIT 100
        """
        
        results = query_service.execute_query(cypher)
        
        nodes = []
        edges = []
        node_ids = set()
        
        for record in results:
            # Add source node
            if 'n' in record and record['n']:
                node_data = dict(record['n'].items()) if hasattr(record['n'], 'items') else record['n']
                if isinstance(node_data, dict):
                    node_id = node_data.get('id', '')
                    if node_id and node_id not in node_ids:
                        nodes.append({
                            "id": node_id,
                            "label": node_data.get("name", "Unknown"),
                            "type": node_data.get("label", "Entity"),
                            "source": node_data.get("source", ""),
                            "properties": node_data
                        })
                        node_ids.add(node_id)
            
            # Add relationship and target node
            if 'r' in record and record['r'] and 'm' in record and record['m']:
                rel_data = dict(record['r'].items()) if hasattr(record['r'], 'items') else {}
                target_data = dict(record['m'].items()) if hasattr(record['m'], 'items') else record['m']
                
                if isinstance(target_data, dict):
                    target_id = target_data.get('id', '')
                    if target_id and target_id not in node_ids:
                        nodes.append({
                            "id": target_id,
                            "label": target_data.get("name", "Unknown"),
                            "type": target_data.get("label", "Entity"),
                            "source": target_data.get("source", ""),
                            "properties": target_data
                        })
                        node_ids.add(target_id)
        
        return jsonify({
            "success": True,
            "nodes": nodes,
            "edges": edges,
            "total_nodes": len(nodes),
            "total_edges": len(edges)
        })
        
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# ============================================================================ #
#                           MAIN
# ============================================================================ #

if __name__ == "__main__":
    # Ensure templates directory exists
    os.makedirs("templates", exist_ok=True)
    
    print("=" * 80)
    print("🚀 Starting Financial Knowledge Graph Web Server")
    print("=" * 80)
    print()
    print("📊 Access the application at: http://localhost:8080")
    print()
    print("🔧 Features:")
    print("   ✓ Complete A-F pipeline execution")
    print("   ✓ Natural language query interface")
    print("   ✓ Graph statistics and visualization")
    print("   ✓ Entity search functionality")
    print("   ✓ Schema information and debugging")
    print("   ✓ Query analysis and suggestions")
    print()
    print("=" * 80)
    
    app.run(debug=True, host="0.0.0.0", port=8080)