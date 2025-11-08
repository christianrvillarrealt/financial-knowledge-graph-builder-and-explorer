import json
import re
from typing import Dict, List, Optional
from neo4j import GraphDatabase
import openai

# ============================================================================ #
#                    STAGE F: NATURAL LANGUAGE QUERY SERVICE
# ============================================================================ #

class NaturalLanguageQueryService:
    """Stage F: Translate natural language queries to Cypher and execute"""
    
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str, openai_api_key: str):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.llm_client = openai.OpenAI(api_key=openai_api_key)
        self.schema_context = self._get_schema_context()
        
    def _get_schema_context(self) -> str:
        """Build schema context for LLM"""
        return """
Neo4j Knowledge Graph Schema:

NODE PROPERTIES:
- id: UUID (unique identifier)
- name: string (entity name)
- label: string (entity type: Person, Company, Product, Event, Entity)
- article_id: string (source article identifier)
- source: string (data source: MarketWatch, Bloomberg, Reuters, etc.)
- published_at: ISO datetime string
- Additional node labels: Person, Company, Product, Event, Entity

RELATIONSHIP PROPERTIES:
- <id>: integer (relationship unique identifier)
- relationship_type or type: string (uppercase with underscores, e.g., HAS, RELATED_TO, ANNOUNCED, ACQUIRED)
- article_id: string (source article identifier)
- source: string (data source)
- published_at: ISO datetime string
- confidence: float (0.0-1.0, typically 0.9)

COMMON RELATIONSHIP TYPES:
- HAS, RELATED_TO, ANNOUNCED, ACQUIRED, INVESTED_IN, PARTNERED_WITH, 
  APPOINTED, RESIGNED, LAUNCHED, INCREASED, DECREASED, REPORTED

EXAMPLE QUERIES:
1. Find all entities: MATCH (n) RETURN n LIMIT 25
2. Find by name: MATCH (n) WHERE n.name CONTAINS 'Apple' RETURN n
3. Find relationships: MATCH (a)-[r]->(b) WHERE a.name = 'Apple' RETURN a, r, b
4. Filter by date: MATCH (n) WHERE n.published_at >= '2025-11-01' RETURN n
5. Filter by source: MATCH (n) WHERE n.source = 'MarketWatch' RETURN n
"""

    def translate_query(self, natural_language_query: str) -> Dict:
        """Translate natural language to Cypher using LLM"""
        prompt = f"""
You are a Neo4j Cypher query expert. Convert the natural language query to a valid Cypher query.

{self.schema_context}

RULES:
1. Use MATCH, WHERE, RETURN clauses appropriately
2. Use CONTAINS for partial text matching (case-insensitive with toLower())
3. Always include LIMIT to prevent overwhelming results (default 50, max 100)
4. For date filtering, use >= or <= with ISO format strings
5. Return nodes with their properties: RETURN n, n.name, n.label, n.source, n.published_at
6. For relationships, return all three: RETURN a, r, b or RETURN a.name, type(r), b.name
7. Use WHERE clauses for filtering by properties
8. Avoid complex aggregations unless explicitly requested
9. Use toLower() for case-insensitive string matching
10. Always validate that property names match the schema

Natural Language Query: "{natural_language_query}"

Respond with ONLY a JSON object in this format:
{{
  "cypher": "MATCH (n) WHERE ... RETURN ...",
  "explanation": "This query finds...",
  "parameters": {{}},
  "return_type": "nodes" or "relationships" or "mixed"
}}

No markdown, no preamble, just the JSON object.
"""
        
        try:
            response = self.llm_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=800
            )
            
            content = response.choices[0].message.content.strip()
            
            # Parse JSON response
            result = self._parse_json_response(content)
            
            # Validate Cypher query
            if not result.get("cypher"):
                raise ValueError("No Cypher query generated")
            
            return result
            
        except Exception as e:
            print(f"❌ Translation error: {e}")
            return {
                "cypher": None,
                "explanation": f"Failed to translate query: {str(e)}",
                "error": str(e)
            }
    
    def _parse_json_response(self, content: str) -> Dict:
        """Parse LLM JSON response with fallback"""
        # Try direct JSON parse
        try:
            return json.loads(content)
        except:
            pass
        
        # Extract JSON from markdown code blocks
        match = re.search(r"```json\n?(.*?)```", content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1).strip())
            except:
                pass
        
        # Extract JSON object
        start = content.find("{")
        end = content.rfind("}") + 1
        if 0 <= start < end:
            try:
                return json.loads(content[start:end])
            except:
                pass
        
        return {
            "cypher": None,
            "explanation": "Failed to parse LLM response",
            "error": "JSON parsing failed"
        }
    
    def execute_query(self, cypher_query: str, parameters: Dict = None) -> List[Dict]:
        """Execute Cypher query and return results"""
        if parameters is None:
            parameters = {}
        
        try:
            with self.driver.session() as session:
                result = session.run(cypher_query, parameters)
                
                records = []
                for record in result:
                    record_dict = {}
                    for key in record.keys():
                        value = record[key]
                        
                        # Handle Neo4j node objects
                        if hasattr(value, 'items'):
                            record_dict[key] = dict(value.items())
                        # Handle Neo4j relationship objects
                        elif hasattr(value, 'type'):
                            record_dict[key] = {
                                "type": value.type,
                                "properties": dict(value.items())
                            }
                        else:
                            record_dict[key] = value
                    
                    records.append(record_dict)
                
                return records
                
        except Exception as e:
            print(f"❌ Query execution error: {e}")
            return [{"error": str(e), "query": cypher_query}]
    
    def query(self, natural_language_query: str, explain: bool = False) -> Dict:
        """Full pipeline: translate and execute natural language query"""
        print(f"🔍 Processing query: '{natural_language_query}'")
        
        # Translate to Cypher
        translation = self.translate_query(natural_language_query)
        
        if not translation.get("cypher"):
            return {
                "success": False,
                "error": translation.get("error", "Translation failed"),
                "explanation": translation.get("explanation"),
                "results": []
            }
        
        cypher_query = translation["cypher"]
        parameters = translation.get("parameters", {})
        
        if explain:
            print(f"📝 Generated Cypher: {cypher_query}")
            print(f"📝 Explanation: {translation.get('explanation')}")
        
        # Execute query
        results = self.execute_query(cypher_query, parameters)
        
        return {
            "success": True,
            "query": natural_language_query,
            "cypher": cypher_query,
            "explanation": translation.get("explanation"),
            "return_type": translation.get("return_type"),
            "results": results,
            "result_count": len(results)
        }
    
    def get_sample_queries(self) -> List[str]:
        """Return sample natural language queries for users"""
        return [
            "Find all companies mentioned in articles",
            "Show me entities related to Apple",
            "What are the relationships between Tesla and its CEO?",
            "Find all articles from MarketWatch published after November 1st 2025",
            "Show me all persons who announced something",
            "What products are mentioned in the knowledge graph?",
            "Find entities related to retirement age",
            "Show all relationships of type HAS",
            "Find companies that acquired other companies",
            "What are the most recent entities added?",
            "Show me all articles about Microsoft",
            "Find all entities from Bloomberg",
            "What events are mentioned in the knowledge graph?"
        ]
    
    def interactive_query_session(self):
        """Start an interactive query session"""
        print("=" * 80)
        print("🚀 Natural Language Knowledge Graph Query Service")
        print("=" * 80)
        print("\nSample queries:")
        for i, sample in enumerate(self.get_sample_queries()[:5], 1):
            print(f"{i}. {sample}")
        print("\nType 'samples' to see all sample queries")
        print("Type 'exit' to quit\n")
        
        while True:
            try:
                user_input = input("📝 Enter your query: ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() == 'exit':
                    print("👋 Goodbye!")
                    break
                
                if user_input.lower() == 'samples':
                    print("\n📚 All sample queries:")
                    for i, sample in enumerate(self.get_sample_queries(), 1):
                        print(f"{i}. {sample}")
                    print()
                    continue
                
                # Execute query
                result = self.query(user_input, explain=True)
                
                if result["success"]:
                    print(f"\n✅ Found {result['result_count']} results")
                    print(f"📊 Return type: {result.get('return_type', 'unknown')}")
                    
                    # Display results (limit to first 5 for readability)
                    for i, record in enumerate(result["results"][:5], 1):
                        print(f"\nResult {i}:")
                        print(json.dumps(record, indent=2, default=str))
                    
                    if result['result_count'] > 5:
                        print(f"\n... and {result['result_count'] - 5} more results")
                else:
                    print(f"\n❌ Query failed: {result.get('error')}")
                
                print("\n" + "-" * 80 + "\n")
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}\n")
    
    def close(self):
        """Close Neo4j driver connection"""
        if self.driver:
            self.driver.close()
            print("✅ Neo4j connection closed")