import os
from dotenv import load_dotenv

# ============================================================================ #
#                           LOAD ENVIRONMENT VARIABLES
# ============================================================================ #

# Load environment variables from .env file
load_dotenv()

# ============================================================================ #
#                           CONFIGURATION
# ============================================================================ #

# API Keys (loaded from environment variables)
CONFIG = {
    "openai_api_key": os.getenv("OPENAI_API_KEY"),
    "newsapi_key": os.getenv("NEWSAPI_KEY"),
    "alpha_vantage_key": os.getenv("ALPHA_VANTAGE_KEY"), 
    "fmp_key": os.getenv("FMP_KEY"),
    "neo4j_uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    "neo4j_user": os.getenv("NEO4J_USER", "neo4j"),
    "neo4j_password": os.getenv("NEO4J_PASSWORD", "financial2025"),
    "docker_container": os.getenv("DOCKER_CONTAINER", "financial-neo4j"),
    "base_data_dir": os.getenv("BASE_DATA_DIR", "/home/chrisrvt/Projects/BIT_2025_Computer_Science_and_Technology/big-data-analysis-technology/10_knowledge_graph/financial_news_data")
}