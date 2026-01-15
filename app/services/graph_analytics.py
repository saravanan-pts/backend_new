import logging
import json
from typing import List, Dict, Any, Set
from datetime import datetime

from app.repositories.graph_repository import GraphRepository
# Use the existing OpenAI client from your extractor
from app.services.openai_extractor import client as openai_client, AZURE_OPENAI_DEPLOYMENT

logger = logging.getLogger(__name__)

class GraphAnalytics:
    def __init__(self):
        self.repo = GraphRepository()

    async def detect_communities(self) -> Dict[str, Any]:
        """
        1. Simple Clustering: Finds connected groups of entities.
        2. AI Summary: Asks OpenAI to find the 'theme' of each group.
        3. Persistence: Saves 'Community' nodes back to the graph.
        """
        logger.info("[Analytics] Starting Community Detection...")
        
        # 1. Fetch all relationships to see the structure
        relationships = self.repo.get_relationships()
        
        # 2. Simple Clustering (Heuristic: Connected components)
        clusters = self._simple_clustering(relationships)
        logger.info(f"[Analytics] Detected {len(clusters)} potential communities.")

        communities_created = []

        # 3. Generate Summaries for each Cluster
        for cluster_id, entity_ids in clusters.items():
            # Skip small clusters (less than 3 nodes) to save tokens
            if len(entity_ids) < 3:
                continue

            summary_node = await self._generate_community_summary(cluster_id, entity_ids)
            if summary_node:
                communities_created.append(summary_node)
        
        logger.info("[Analytics] Completed.")
        return {
            "communities_detected": len(clusters),
            "new_community_nodes": communities_created
        }

    async def _generate_community_summary(self, cluster_id: str, entity_ids: str[]) -> str:
        """Fetch group data, ask AI for a theme, and save as a Community node."""
        try:
            # Fetch the actual labels/content for these entities using Gremlin
            id_list = [f"'{eid}'" for eid in entity_ids]
            query = f"g.V({','.join(id_list)}).valueMap(true)"
            entity_data = self.repo.client.submit(query).all().result()
            
            if not entity_data:
                return None

            # Format context for AI
            context_text = "\n".join([f"{e.get('label', 'Unknown')}: {json.dumps(e)}" for e in entity_data])

            # Ask AI to summarize this "Community"
            prompt = f"""
            You are analyzing a 'Community' detected in a Knowledge Graph.
            
            Entities in this community:
            {context_text[:6000]}

            Task:
            1. Identify the common theme connecting these entities.
            2. Write a detailed summary of what this group represents.
            3. Assign a specialized label (e.g., "Compliance_Cluster_A").

            Return ONLY valid JSON: {{ "theme": "...", "summary": "...", "label": "..." }}
            """

            response = await openai_client.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # 4. Save the "Community" as a Node in the Graph
            community_id = f"community_{cluster_id}"
            community_props = {
                "label": result.get("label", "Unknown Community"),
                "theme": result.get("theme", ""),
                "summary": result.get("summary", ""),
                "member_count": len(entity_ids),
                "generated_at": str(datetime.now()),
                "pk": "Community"
            }
            
            self.repo.create_entity(community_id, "Community", community_props)

            # Link members to the Community
            for member_id in entity_ids:
                self.repo.create_relationship(member_id, community_id, "BELONGS_TO", {"confidence": 1.0})

            return community_id

        except Exception as e:
            logger.error(f"Failed to summarize community {cluster_id}: {e}")
            return None

    def _simple_clustering(self, relationships: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Connected Components algorithm to group IDs."""
        node_to_cluster = {}
        clusters = {}
        cluster_count = 0

        for rel in relationships:
            u = rel.get("outV")
            v = rel.get("inV")
            if not u or not v:
                continue

            u_clust = node_to_cluster.get(u)
            v_clust = node_to_cluster.get(v)

            if u_clust is None and v_clust is None:
                cluster_id = f"c_{cluster_count}"
                cluster_count += 1
                node_to_cluster[u] = cluster_id
                node_to_cluster[v] = cluster_id
                clusters[cluster_id] = {u, v}
            elif u_clust is not None and v_clust is None:
                node_to_cluster[v] = u_clust
                clusters[u_clust].add(v)
            elif u_clust is None and v_clust is not None:
                node_to_cluster[u] = v_clust
                clusters[v_clust].add(u)
            elif u_clust != v_clust:
                # Merge clusters
                for node in clusters[v_clust]:
                    node_to_cluster[node] = u_clust
                    clusters[u_clust].add(node)
                del clusters[v_clust]

        return {k: list(v) for k, v in clusters.items()}

    async def find_shortest_path(self, source_id: str, target_id: str):
        """Finds the quickest road between two Lego houses."""
        # FIXED: Changed execute_query to the standard submit().all().result()
        query = f"g.V('{source_id}').repeat(out().simplePath()).until(hasId('{target_id}')).path().limit(1)"
        try:
            result = self.repo.client.submit(query).all().result()
            return result
        except Exception as e:
            logger.error(f"Shortest path failed: {e}")
            return []

graph_analytics = GraphAnalytics()