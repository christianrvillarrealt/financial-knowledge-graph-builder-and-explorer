import pandas as pd
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict
from neo4j import GraphDatabase

from load_env import *

# ============================================================================ #
#                           STAGE E: NEO4J LOADING
# ============================================================================ #

class Neo4jGraphLoader:
    """Stage E: Load data into Neo4j with proper schema"""
    
    def __init__(self, uri: str, user: str, password: str, docker_container: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.docker_container = docker_container
        self.import_dir = "/var/lib/neo4j/import"

    def load_nodes_csv(self, csv_path: str):
        """Load nodes with proper labels"""
        print("🔍 Loading nodes into Neo4j...")
        cypher = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{csv_path}' AS row
        MERGE (n:Entity {{id: row.id}})
        SET n.name = row.name,
            n.label = row.label,
            n.source = row.source,
            n.article_id = row.article_id,
            n.published_at = row.published_at
        WITH n, row
        CALL apoc.create.addLabels(n, [row.label]) YIELD node
        RETURN count(node) AS nodes_created
        """
        try:
            with self.driver.session() as session:
                result = session.run(cypher)
                created = result.single()["nodes_created"]
                print(f"✅ Nodes created: {created}")
                return True
        except Exception as e:
            print(f"❌ Node loading error: {e}")
            return False

    def load_relationships_csv(self, csv_path: str):
        """Load relationships"""
        print("🔍 Loading relationships into Neo4j...")
        cypher = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{csv_path}' AS row
        MATCH (source {{id: row.start_id}})
        MATCH (target {{id: row.end_id}})
        CALL apoc.merge.relationship(
            source, 
            row.type,
            {{
                confidence: toFloat(coalesce(row.confidence, 1.0)),
                source: row.source,
                article_id: row.article_id,
                published_at: row.published_at
            }},
            {{}},
            target
        ) YIELD rel
        RETURN count(rel) as relationships_created
        """
        try:
            with self.driver.session() as session:
                result = session.run(cypher)
                created = result.single()["relationships_created"]
                print(f"✅ Relationships created: {created}")
                return True
        except Exception as e:
            print(f"❌ Relationship loading error: {e}")
            return False

    def copy_to_docker(self, src_path: str, dst_name: str):
        """Copy file to Neo4j container"""
        dst_path = f"{self.docker_container}:{self.import_dir}/{dst_name}"
        try:
            subprocess.run(["docker", "cp", src_path, dst_path], check=True)
            print(f"📥 Copied to container: {dst_name}")
            return dst_name
        except subprocess.CalledProcessError as e:
            print(f"❌ Docker copy failed: {e}")
            return None

    def run_loading(self, neo4j_data: Dict[str, pd.DataFrame]):
        """Run complete Neo4j loading"""
        print("🚀 STAGE E: Neo4j Loading Started")
        
        # Save CSVs
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nodes_csv = Path(CONFIG["base_data_dir"]) / "neo4j_ready" / f"nodes_{timestamp}.csv"
        rels_csv = Path(CONFIG["base_data_dir"]) / "neo4j_ready" / f"relations_{timestamp}.csv"
        
        neo4j_data["nodes"].to_csv(nodes_csv, index=False)
        neo4j_data["relations"].to_csv(rels_csv, index=False)
        
        print(f"💾 Saved: {nodes_csv} ({len(neo4j_data['nodes'])} nodes)")
        print(f"💾 Saved: {rels_csv} ({len(neo4j_data['relations'])} relations)")
        
        # Copy to Docker and load
        nodes_docker = self.copy_to_docker(str(nodes_csv), "nodes_latest.csv")
        rels_docker = self.copy_to_docker(str(rels_csv), "relations_latest.csv")
        
        if nodes_docker and rels_docker:
            if self.load_nodes_csv("nodes_latest.csv") and self.load_relationships_csv("relations_latest.csv"):
                print("✅ STAGE E Complete: Data loaded into Neo4j")
                return True
        return False