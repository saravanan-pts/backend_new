import { useCallback } from "react";
import { graphOps } from "@/services/graph-operations";
import { useGraphStore } from "@/lib/store";
import { surrealDB } from "@/lib/surrealdb-client";
import type { Entity, Relationship } from "@/types";

export function useGraph() {
  const {
    entities,
    relationships,
    selectedEntity,
    selectedRelationship,
    setEntities,
    addEntity,
    updateEntity,
    deleteEntity,
    setRelationships,
    addRelationship,
    updateRelationship: updateRelationshipInStore,
    deleteRelationship,
    setSelectedEntity,
    setSelectedRelationship,
    setLoading,
  } = useGraphStore();

  const loadGraph = useCallback(async (documentId?: string | null) => {
    // Check if connected before loading
    const client = surrealDB.getClient();
    if (!client) {
      throw new Error("SurrealDB client not connected. Call connect() first.");
    }
    
    setLoading(true);
    try {
      let allEntities: Entity[];
      let allRelationships: Relationship[];

      if (documentId) {
        // Load entities and relationships for specific document
        [allEntities, allRelationships] = await Promise.all([
          graphOps.getEntitiesByDocument(documentId),
          graphOps.getAllRelationships(documentId),
        ]);
      } else {
        // Load all entities and relationships
        [allEntities, allRelationships] = await Promise.all([
          graphOps.getAllEntities(),
          graphOps.getAllRelationships(),
        ]);
      }

      setEntities(allEntities);
      setRelationships(allRelationships);
    } catch (error: any) {
      // Handle connection and permission errors gracefully
      if (error?.message?.includes("not connected")) {
        console.warn("Graph loading skipped - connection not ready");
        setEntities([]);
        setRelationships([]);
      } else if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        // Permission errors are already handled in graph-operations, but just in case
        console.warn("Permission error loading graph. App will work with limited functionality.");
        setEntities([]);
        setRelationships([]);
      } else {
        console.error("Error loading graph:", error);
        // Don't throw - just set empty arrays so app can still function
        setEntities([]);
        setRelationships([]);
      }
    } finally {
      setLoading(false);
    }
  }, [setEntities, setRelationships, setLoading]);

  const createEntity = useCallback(
    async (entity: Omit<Entity, "id" | "createdAt" | "updatedAt">) => {
      setLoading(true);
      try {
        const created = await graphOps.createEntity(entity);
        addEntity(created);
        return created;
      } catch (error) {
        console.error("Error creating entity:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [addEntity, setLoading]
  );

  const updateEntityById = useCallback(
    async (id: string, updates: Partial<Entity>) => {
      setLoading(true);
      try {
        const updated = await graphOps.updateEntity(id, updates);
        updateEntity(id, updated);
        return updated;
      } catch (error) {
        console.error("Error updating entity:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [updateEntity, setLoading]
  );

  const removeEntity = useCallback(
    async (id: string) => {
      setLoading(true);
      try {
        await graphOps.deleteEntity(id);
        deleteEntity(id);
      } catch (error) {
        console.error("Error deleting entity:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [deleteEntity, setLoading]
  );

  const createRelationship = useCallback(
    async (
      from: string,
      to: string,
      type: Relationship["type"],
      properties?: Relationship["properties"],
      confidence?: number,
      source?: string
    ) => {
      setLoading(true);
      try {
        const created = await graphOps.createRelationship(
          from,
          to,
          type,
          properties,
          confidence,
          source
        );
        addRelationship(created);
        return created;
      } catch (error) {
        console.error("Error creating relationship:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [addRelationship, setLoading]
  );

  const updateRelationshipById = useCallback(
    async (id: string, updates: Partial<Relationship>) => {
      setLoading(true);
      try {
        const updated = await graphOps.updateRelationship(id, updates);
        // Update relationship in store
        updateRelationshipInStore(id, updated);
        return updated;
      } catch (error) {
        console.error("Error updating relationship:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [updateRelationshipInStore, setLoading]
  );

  const getRelationshipById = useCallback(
    async (id: string): Promise<Relationship | null> => {
      setLoading(true);
      try {
        const relationship = await graphOps.getRelationship(id);
        return relationship;
      } catch (error) {
        console.error("Error getting relationship:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [setLoading]
  );

  const removeRelationship = useCallback(
    async (id: string) => {
      setLoading(true);
      try {
        await graphOps.deleteRelationship(id);
        deleteRelationship(id);
      } catch (error) {
        console.error("Error deleting relationship:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [deleteRelationship, setLoading]
  );

  const searchEntities = useCallback(
    async (query: string) => {
      setLoading(true);
      try {
        const results = await graphOps.searchEntities(query);
        return results;
      } catch (error) {
        console.error("Error searching entities:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [setLoading]
  );

  const getNeighbors = useCallback(
    async (entityId: string, depth: number = 1) => {
      setLoading(true);
      try {
        const result = await graphOps.getNeighbors(entityId, depth);
        return result;
      } catch (error) {
        console.error("Error getting neighbors:", error);
        throw error;
      } finally {
        setLoading(false);
      }
    },
    [setLoading]
  );

  return {
    // State
    entities: Array.from(entities.values()),
    relationships,
    selectedEntity,
    selectedRelationship,
    // Actions
    loadGraph,
    createEntity,
    updateEntity: updateEntityById,
    deleteEntity: removeEntity,
    createRelationship,
    updateRelationship: updateRelationshipById,
    deleteRelationship: removeRelationship,
    getRelationship: getRelationshipById,
    searchEntities,
    getNeighbors,
    selectEntity: setSelectedEntity,
    selectRelationship: setSelectedRelationship,
  };
}

