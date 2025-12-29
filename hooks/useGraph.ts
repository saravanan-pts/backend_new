import { useCallback } from "react";
import { useGraphStore } from "@/lib/store";
import type { Entity, Relationship } from "@/types";

/**
 * Client-side graph hook
 * Talks ONLY to /api/*
 * No DB drivers, no gremlin, no surreal imports
 */
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

  /* ===============================
     LOAD GRAPH
     =============================== */

  const loadGraph = useCallback(
    async (documentId?: string | null) => {
      setLoading(true);
      try {
        const url = documentId
          ? `/api/graph?documentId=${documentId}`
          : `/api/graph`;

        const res = await fetch(url);
        if (!res.ok) throw new Error("Failed to load graph");

        const data = await res.json();
        setEntities(data.entities ?? []);
        setRelationships(data.relationships ?? []);
      } catch (error) {
        console.error("Error loading graph:", error);
        setEntities([]);
        setRelationships([]);
      } finally {
        setLoading(false);
      }
    },
    [setEntities, setRelationships, setLoading]
  );

  /* ===============================
     ENTITY OPERATIONS
     =============================== */

  const createEntity = useCallback(
    async (entity: Omit<Entity, "id" | "createdAt" | "updatedAt">) => {
      setLoading(true);
      try {
        const res = await fetch("/api/entities", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(entity),
        });

        if (!res.ok) throw new Error("Failed to create entity");

        const created = await res.json();
        addEntity(created);
        return created;
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
        const res = await fetch(`/api/entities/${id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(updates),
        });

        if (!res.ok) throw new Error("Failed to update entity");

        const updated = await res.json();
        updateEntity(id, updated);
        return updated;
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
        const res = await fetch(`/api/entities/${id}`, { method: "DELETE" });
        if (!res.ok) throw new Error("Failed to delete entity");
        deleteEntity(id);
      } finally {
        setLoading(false);
      }
    },
    [deleteEntity, setLoading]
  );

  /* ===============================
     RELATIONSHIP OPERATIONS
     =============================== */

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
        const res = await fetch("/api/relationships", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            from,
            to,
            type,
            properties,
            confidence,
            source,
          }),
        });

        if (!res.ok) throw new Error("Failed to create relationship");

        const created = await res.json();
        addRelationship(created);
        return created;
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
        const res = await fetch(`/api/relationships/${id}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(updates),
        });

        if (!res.ok) throw new Error("Failed to update relationship");

        const updated = await res.json();
        updateRelationshipInStore(id, updated);
        return updated;
      } finally {
        setLoading(false);
      }
    },
    [updateRelationshipInStore, setLoading]
  );

  const removeRelationship = useCallback(
    async (id: string) => {
      setLoading(true);
      try {
        const res = await fetch(`/api/relationships/${id}`, {
          method: "DELETE",
        });

        if (!res.ok) throw new Error("Failed to delete relationship");
        deleteRelationship(id);
      } finally {
        setLoading(false);
      }
    },
    [deleteRelationship, setLoading]
  );

  /* ===============================
     QUERY HELPERS
     =============================== */

  const searchEntities = useCallback(
    async (query: string) => {
      setLoading(true);
      try {
        const res = await fetch(`/api/entities/search?q=${query}`);
        if (!res.ok) throw new Error("Search failed");
        return await res.json();
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
        const res = await fetch(
          `/api/entities/${entityId}/neighbors?depth=${depth}`
        );
        if (!res.ok) throw new Error("Failed to get neighbors");
        return await res.json();
      } finally {
        setLoading(false);
      }
    },
    [setLoading]
  );

  /* ===============================
     PUBLIC API
     =============================== */

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

    searchEntities,
    getNeighbors,

    selectEntity: setSelectedEntity,
    selectRelationship: setSelectedRelationship,
  };
}
