import type { Entity, Relationship, Document } from "@/types";

export interface GraphDriver {
  /* ===============================
     ENTITY (VERTEX) OPERATIONS
     =============================== */

  createEntity(
    entity: Omit<Entity, "id" | "createdAt" | "updatedAt">
  ): Promise<Entity>;

  updateEntity(
    id: string,
    updates: Partial<Omit<Entity, "id" | "createdAt">>
  ): Promise<Entity>;

  deleteEntity(id: string): Promise<void>;

  getEntity(id: string): Promise<Entity | null>;

  getAllEntities(): Promise<Entity[]>;

  searchEntities(query: string): Promise<Entity[]>;

  /* ===============================
     RELATIONSHIP (EDGE) OPERATIONS
     =============================== */

  createRelationship(
    from: string,
    to: string,
    type: Relationship["type"],
    properties?: Relationship["properties"],
    confidence?: number,
    source?: string
  ): Promise<Relationship>;

  updateRelationship(
    id: string,
    updates: Partial<Omit<Relationship, "id" | "createdAt">>
  ): Promise<Relationship>;

  deleteRelationship(id: string): Promise<void>;

  getRelationship(id: string): Promise<Relationship | null>;

  getAllRelationships(documentId?: string): Promise<Relationship[]>;

  /* ===============================
     GRAPH TRAVERSAL
     =============================== */

  getNeighbors(
    entityId: string,
    depth?: number
  ): Promise<{
    entities: Entity[];
    relationships: Relationship[];
  }>;

  getSubgraph(
    entityIds: string[]
  ): Promise<{
    entities: Entity[];
    relationships: Relationship[];
  }>;

  /* ===============================
     DOCUMENT OPERATIONS
     =============================== */

  createDocument(
    document: Omit<Document, "id" | "uploadedAt">
  ): Promise<Document>;

  updateDocument(
    id: string,
    updates: Partial<Document>
  ): Promise<Document>;

  getAllDocuments(): Promise<Document[]>;

  getEntitiesByDocument(
    documentId: string
  ): Promise<Entity[]>;

  /* ===============================
     MAINTENANCE / UTILS
     =============================== */

  clearAllData(): Promise<{
    entitiesDeleted: number;
    relationshipsDeleted: number;
    documentsDeleted: number;
  }>;
}
