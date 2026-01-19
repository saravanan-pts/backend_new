import logging
import asyncio
import random
from typing import List, Dict, Any, Optional

from gremlin_python.driver.protocol import GremlinServerError
from app.db.cosmos_client import get_gremlin_client

logger = logging.getLogger(__name__)

class GraphRepository:
    def __init__(self):
        self.client = get_gremlin_client()

    def _parse_filename(self, filename: str):
        if '.' in filename: base = filename.rsplit('.', 1)[0]
        else: base = filename
        if "_" in base: parts = base.split('_', 1); return parts[0], parts[1]
        return "general", base

    async def _execute_query(self, query: str, bindings: Dict[str, Any] = None) -> Any:
        retries = 0
        MAX_RETRIES = 10
        while True:
            try:
                result_set = self.client.submit_async(query, bindings=bindings).result()
                return result_set.all().result()
            except Exception as exc:
                error_msg = str(exc)
                if "429" in error_msg or "RequestRateTooLarge" in error_msg:
                    retries += 1
                    if retries > MAX_RETRIES:
                        logger.error(f"Max retries exceeded for query: {query}")
                        raise exc
                    wait_time = (2 ** retries) + (random.randint(0, 1000) / 1000.0)
                    await asyncio.sleep(wait_time)
                else:
                    raise exc

    # --- 1. FIXED CREATE ENTITY (Proper Binding Usage) ---
    async def create_entity(self, entity_id: str, label: str, properties: Dict[str, Any]) -> None:
        """
        Creates OR Updates a vertex safely using Bindings.
        """
        prop_assignments = []
        # BINDINGS: We define the variables here
        bindings = {
            "entity_id": entity_id,
            "label": label,
        }
        
        for key, value in properties.items():
            if key in ["id", "pk"]: continue
            if value is not None:
                prop_key = f"prop_{key}"
                prop_assignments.append(f".property('{key}', {prop_key})")
                bindings[prop_key] = value
        
        props_str = "".join(prop_assignments)
        
        # QUERY STRING: We do NOT use f-string for the variable names.
        # We concatenate strings to ensure "entity_id" is sent literally.
        # This tells Cosmos DB to look up "entity_id" in the bindings.
        query = (
            "g.V(entity_id).fold().coalesce("
            "unfold(), "
            "addV(label).property('id', entity_id).property('pk', entity_id)"
            f"){props_str}" 
        )

        await self._execute_query(query, bindings)

    # --- 2. FIXED CREATE RELATIONSHIP (Proper Binding Usage) ---
    async def create_relationship(self, from_id: str, to_id: str, label: str, properties: Dict[str, Any] = None) -> None:
        prop_assignments = []
        bindings = {
            "from_id": from_id,
            "to_id": to_id,
            "label": label,
        }

        if properties:
            for key, value in properties.items():
                if value is not None:
                    prop_key = f"prop_{key}"
                    prop_assignments.append(f".property('{key}', {prop_key})")
                    bindings[prop_key] = value

        props_str = "".join(prop_assignments)

        # QUERY STRING: Same fix. Send literal "from_id" etc.
        query = (
            "g.V(from_id).coalesce("
            "outE(label).where(inV().hasId(to_id)), "
            "addE(label).to(g.V(to_id))"
            f"){props_str}"
        )
        
        await self._execute_query(query, bindings)

    # --- 3. FETCH & DELETE (Unchanged, Logic was correct) ---
    async def fetch_combined_graph(self, limit: int = 500, types: List[str] = None, document_id: str = None) -> Dict[str, Any]:
        try:
            bindings = {"limit": limit}
            node_query = "g.V()"
            edge_query_base = "g.E()"

            if document_id:
                domain, docId = self._parse_filename(document_id)
                node_query += ".has('domain', domain).has('documentId', docId)"
                edge_query_base += ".where(outV().has('domain', domain).has('documentId', docId)).where(inV().has('domain', domain).has('documentId', docId))"
                bindings["domain"] = domain
                bindings["docId"] = docId

            if types:
                bindings["types_list"] = types
                node_query += ".hasLabel(within(types_list))"

            node_query += ".limit(limit).valueMap(true)"
            edge_query = f"{edge_query_base}.limit(limit_val).project('id','label','source','target','properties').by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"
            
            bindings_edges = bindings.copy()
            bindings_edges["limit_val"] = limit * 2

            raw_nodes = await self._execute_query(node_query, bindings=bindings)
            raw_edges = await self._execute_query(edge_query, bindings=bindings_edges)

            return {"nodes": raw_nodes, "edges": raw_edges, "meta": {"count": {"nodes": len(raw_nodes), "edges": len(raw_edges)}}}
        except Exception as exc:
            logger.error("Failed to fetch combined graph: %s", exc)
            raise exc

    async def delete_data_by_filename(self, filename: str) -> None:
        BATCH_SIZE = 20
        try:
            domain, docId = self._parse_filename(filename)
            logger.info(f"Deleting data for domain='{domain}', documentId='{docId}'")
            query = f"g.V().has('documentId', '{docId}').has('domain', '{domain}').limit({BATCH_SIZE}).drop()"
            count_query = f"g.V().has('documentId', '{docId}').has('domain', '{domain}').count()"
            while True:
                res = await self._execute_query(count_query)
                if not res or res[0] == 0: break
                await self._execute_query(query)
                await asyncio.sleep(0.2)
            await self.delete_entity(filename)
            logger.info("Cleared graph data for document: %s", filename)
        except Exception as exc:
            logger.error("Failed to clear document data for %s: %s", filename, exc)
            raise exc

    # --- STANDARD OPERATIONS ---
    async def clear_graph(self, scope: str = "all") -> bool:
        try:
            BATCH_SIZE = 500
            if scope == "relationships": query = f"g.E().limit({BATCH_SIZE}).drop()"; check = "g.E().count()"
            else: query = f"g.V().limit({BATCH_SIZE}).drop()"; check = "g.V().count()"
            while True:
                res = await self._execute_query(check)
                if not res or res[0] == 0: break
                await self._execute_query(query)
                await asyncio.sleep(0.2)
            return True
        except: return False

    async def get_entities(self, label: Optional[str] = None) -> List[Dict[str, Any]]:
        q = f"g.V().hasLabel('{label}').valueMap(true)" if label else "g.V().valueMap(true)"
        return await self._execute_query(q)

    async def get_relationships(self) -> List[Dict[str, Any]]:
        return await self._execute_query("g.E().elementMap()")

    async def update_entity(self, entity_id: str, properties: Dict[str, Any]) -> None:
        query = f"g.V('{entity_id}')"
        for k, v in properties.items():
            safe_v = str(v).replace("'", "\\'")
            query += f".property('{k}', '{safe_v}')"
        await self._execute_query(query)

    async def delete_entity(self, entity_id: str) -> None:
        await self._execute_query(f"g.V('{entity_id}').drop()")

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]) -> None:
        query = f"g.E('{rel_id}')"
        for k, v in properties.items():
            safe_v = str(v).replace("'", "\\'")
            query += f".property('{k}', '{safe_v}')"
        await self._execute_query(query)

    async def delete_relationship(self, rel_id: str) -> None:
        await self._execute_query(f"g.E('{rel_id}').drop()")

    async def search_nodes(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        return await self._execute_query(f"g.V().hasLabel(TextP.containing('{keyword}')).limit({limit}).valueMap(true)")

    async def get_stats(self) -> Dict[str, Any]:
        nodes = (await self._execute_query("g.V().count()"))[0]
        edges = (await self._execute_query("g.E().count()"))[0]
        return {"nodes": nodes, "edges": edges}

    async def get_graph(self) -> Dict[str, Any]:
        return {"nodes": await self.get_entities(), "edges": await self.get_relationships()}

    async def get_relationships_for_entity(self, entity_id: str) -> List[Dict[str, Any]]:
        return await self._execute_query(f"g.V('{entity_id}').bothE().elementMap()")

graph_repository = GraphRepository()