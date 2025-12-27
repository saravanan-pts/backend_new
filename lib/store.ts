import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { Entity, Relationship, EntityType } from "@/types";

interface GraphState {
  // Graph data
  entities: Map<string, Entity>;
  relationships: Relationship[];
  selectedEntity: Entity | null;
  selectedRelationship: Relationship | null;

  // UI state
  isLoading: boolean;
  activeTab: "upload" | "input" | "details" | "settings";
  showSettings: boolean;
  filterTypes: EntityType[];

  // Actions
  setEntities: (entities: Entity[]) => void;
  addEntity: (entity: Entity) => void;
  updateEntity: (id: string, updates: Partial<Entity>) => void;
  deleteEntity: (id: string) => void;
  setRelationships: (relationships: Relationship[]) => void;
  addRelationship: (relationship: Relationship) => void;
  updateRelationship: (id: string, relationship: Relationship) => void;
  deleteRelationship: (id: string) => void;
  setSelectedEntity: (entity: Entity | null) => void;
  setSelectedRelationship: (relationship: Relationship | null) => void;
  setLoading: (loading: boolean) => void;
  setActiveTab: (tab: "upload" | "input" | "details" | "settings") => void;
  setShowSettings: (show: boolean) => void;
  toggleFilterType: (type: EntityType) => void;
  clearFilters: () => void;
  clearGraph: () => void;
}

export const useGraphStore = create<GraphState>()(
  persist(
    (set) => ({
      // Initial state
      entities: new Map(),
      relationships: [],
      selectedEntity: null,
      selectedRelationship: null,
      isLoading: false,
      activeTab: "upload",
      showSettings: false,
      filterTypes: [],

      // Actions
      setEntities: (entities) =>
        set({
          entities: new Map(entities.map((e) => [e.id, e])),
        }),

      addEntity: (entity) =>
        set((state) => {
          const newEntities = new Map(state.entities);
          newEntities.set(entity.id, entity);
          return { entities: newEntities };
        }),

      updateEntity: (id, updates) =>
        set((state) => {
          const entity = state.entities.get(id);
          if (!entity) return state;

          const updatedEntity = { ...entity, ...updates };
          const newEntities = new Map(state.entities);
          newEntities.set(id, updatedEntity);

          // Update selected entity if it's the one being updated
          const selectedEntity =
            state.selectedEntity?.id === id ? updatedEntity : state.selectedEntity;

          return { entities: newEntities, selectedEntity };
        }),

      deleteEntity: (id) =>
        set((state) => {
          const newEntities = new Map(state.entities);
          newEntities.delete(id);

          // Remove relationships connected to this entity
          const newRelationships = state.relationships.filter(
            (r) => r.from !== id && r.to !== id
          );

          // Clear selection if deleted entity was selected
          const selectedEntity =
            state.selectedEntity?.id === id ? null : state.selectedEntity;

          return {
            entities: newEntities,
            relationships: newRelationships,
            selectedEntity,
          };
        }),

      setRelationships: (relationships) => set({ relationships }),

      addRelationship: (relationship) =>
        set((state) => ({
          relationships: [...state.relationships, relationship],
        })),

      updateRelationship: (id, relationship) =>
        set((state) => {
          const newRelationships = state.relationships.map((r) =>
            r.id === id ? relationship : r
          );
          
          // Update selected relationship if it's the one being updated
          const selectedRelationship =
            state.selectedRelationship?.id === id ? relationship : state.selectedRelationship;

          return {
            relationships: newRelationships,
            selectedRelationship,
          };
        }),

      deleteRelationship: (id) =>
        set((state) => {
          const newRelationships = state.relationships.filter((r) => r.id !== id);
          
          // Clear selection if deleted relationship was selected
          const selectedRelationship =
            state.selectedRelationship?.id === id ? null : state.selectedRelationship;

          return {
            relationships: newRelationships,
            selectedRelationship,
          };
        }),

      setSelectedEntity: (entity) => set({ selectedEntity: entity }),
      
      setSelectedRelationship: (relationship) => set({ selectedRelationship: relationship }),

      setLoading: (loading) => set({ isLoading: loading }),

      setActiveTab: (tab) => set({ activeTab: tab }),

      setShowSettings: (show) => set({ showSettings: show }),

      toggleFilterType: (type) =>
        set((state) => {
          const filterTypes = state.filterTypes.includes(type)
            ? state.filterTypes.filter((t) => t !== type)
            : [...state.filterTypes, type];
          return { filterTypes };
        }),

      clearFilters: () => set({ filterTypes: [] }),

      clearGraph: () =>
        set({
          entities: new Map(),
          relationships: [],
          selectedEntity: null,
          selectedRelationship: null,
        }),
    }),
    {
      name: "graph-store",
      partialize: (state) => ({
        // Only persist UI preferences, not graph data
        activeTab: state.activeTab,
        filterTypes: state.filterTypes,
      }),
    }
  )
);

