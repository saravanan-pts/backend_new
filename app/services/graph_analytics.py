import logging
import json
from typing import List, Dict, Any, Set
from datetime import datetime

from app.repositories.graph_repository import GraphRepository
# Ensure these imports match your existing OpenAI configuration
from app.services.openai_extractor import client as openai_client, AZURE_OPENAI_DEPLOYMENT

logger = logging.getLogger(__name__)

class GraphAnalytics:
    def __init__(self):
        self.repo = GraphRepository()

    # --- SAFE GREMLIN EXECUTION HELPER ---
    async def _execute_gremlin(self, query: str) -> Any:
        """Safely executes Gremlin queries without Threading/Future crashes."""
        try:
            client = getattr(self.repo, 'client', None)
            if not client: return None
            
            submit = getattr(client, 'submitAsync', getattr(client, 'submit_async', getattr(client, 'submit', None)))
            if not submit: return None
            
            future = submit(query)
            result_set = future.result() if hasattr(future, 'result') else future
            
            if hasattr(result_set, 'all'):
                results = result_set.all().result()
            else:
                results = result_set
                
            return results
        except Exception as e:
            logger.error(f"[Analytics] Gremlin Query Failed: {e}")
            return None

    async def detect_communities(self) -> Dict[str, Any]:
        """
        1. Simple Clustering: Finds connected groups of entities.
        2. AI Summary: Asks OpenAI to find the 'theme' of each group.
        3. Persistence: Saves 'Community' nodes back to the graph.
        """
        logger.info("[Analytics] Starting Community Detection...")
        
        # 1. Fetch all relationships to see the structure
        relationships = await self.repo.get_relationships()
        if not relationships:
            logger.warning("[Analytics] No relationships found to cluster.")
            return {"communities_detected": 0, "new_community_nodes": []}
        
        # 2. Simple Clustering (Heuristic: Connected components)
        clusters = self._simple_clustering(relationships)
        logger.info(f"[Analytics] Detected {len(clusters)} potential communities.")

        communities_created = []

        # 3. Generate Summaries for each Cluster
        for cluster_id, entity_ids in clusters.items():
            # Skip small clusters (less than 3 nodes) to save AI tokens and noise
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

    async def _generate_community_summary(self, cluster_id: str, entity_ids: List[str]) -> str:
        """Fetch group data, ask AI for a crisp business theme, and save as a Community node."""
        try:
            # 1. Fetch labels/content for entities using Gremlin safely
            id_list = [f"'{eid}'" for eid in entity_ids]
            query = f"g.V({','.join(id_list)}).project('id', 'label', 'props').by(id).by(label).by(valueMap())"
            
            entity_data = await self._execute_gremlin(query)
            
            if not entity_data:
                return None

            # 2. Format context for AI (Compress to save tokens)
            context_items = []
            for e in entity_data:
                label = e.get('label', 'Unknown')
                props = e.get('props', {})
                name = props.get('name', [e.get('id')])[0] if isinstance(props.get('name'), list) else props.get('name', e.get('id'))
                context_items.append(f"[{label}] {name}")
            
            context_text = "\n".join(context_items)

            # 3. UPGRADED AI PROMPT: Strictly "Up to the point" Executive Analysis
            prompt = f"""
            You are a Senior Process Mining Analyst reviewing a cluster of connected entities in an enterprise Knowledge Graph.
            
            ENTITIES IN CLUSTER:
            {context_text[:5000]}

            TASK:
            Provide a hyper-concise, executive-level summary of what this cluster represents. Get straight to the point. No fluff.

            REQUIREMENTS:
            1. 'label': A strict, 2-to-3 word category (e.g., "Fraud Ring", "Auto Claim Lifecycle", "Blue-Collar Sales").
            2. 'theme': A 1-sentence description of the core business function or anomaly.
            3. 'summary': Maximum 2 sentences. State exactly what operational process this is, and if there is an obvious bottleneck or risk.

            Return ONLY valid JSON: {{ "theme": "...", "summary": "...", "label": "..." }}
            """

            response = await openai_client.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1, # Extremely low temp for factual, concise output
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # 4. Save the "Community" as a Node in the Graph
            community_id = f"community_{cluster_id}"
            community_props = {
                "name": result.get("label", "Operational Cluster"),
                "normType": "Community",
                "theme": result.get("theme", ""),
                "summary": result.get("summary", ""),
                "member_count": len(entity_ids),
                "generated_at": datetime.now().isoformat(),
                "pk": "Community"
            }
            
            await self.repo.create_entity(community_id, "Community", community_props)

            # 5. Link members to the Community
            for member_id in entity_ids:
                await self.repo.create_relationship(member_id, community_id, "BELONGS_TO", {"confidence": 1.0})

            return community_id

        except Exception as e:
            logger.error(f"Failed to summarize community {cluster_id}: {e}")
            return None

    def _simple_clustering(self, relationships: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Connected Components algorithm to group IDs. (Remains sync as it is pure CPU logic)"""
        node_to_cluster = {}
        clusters = {}
        cluster_count = 0

        for rel in relationships:
            # FIX: Robustly grab Source/Target IDs regardless of how the DB formatted the dictionary
            u = rel.get("outV") or rel.get("source") or rel.get("from")
            v = rel.get("inV") or rel.get("target") or rel.get("to")
            
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
        """Finds the quickest road between two entities."""
        query = f"g.V('{source_id}').repeat(out().simplePath()).until(hasId('{target_id}')).path().limit(1)"
        try:
            # FIX: Used the safe execution wrapper
            result = await self._execute_gremlin(query)
            return result if result else []
        except Exception as e:
            logger.error(f"Shortest path failed: {e}")
            return []

graph_analytics = GraphAnalytics()