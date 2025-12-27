// Entity Types
export type EntityType =
  | "Person"
  | "Organization"
  | "Location"
  | "Concept"
  | "Event"
  | "Technology";

// Relationship Types
export type RelationshipType =
  | "RELATED_TO"
  | "PART_OF"
  | "WORKS_AT"
  | "LOCATED_IN"
  | "MENTIONS"
  | "CREATED_BY";

// Entity Interface
export interface Entity {
  id: string;
  type: EntityType;
  label: string;
  properties: Record<string, any>;
  metadata?: {
    source?: string;
    confidence?: number;
  };
  createdAt?: string;
  updatedAt?: string;
}

// Relationship Interface
export interface Relationship {
  id: string;
  from: string; // Entity ID
  to: string; // Entity ID
  type: RelationshipType;
  properties?: Record<string, any>;
  confidence?: number;
  source?: string;
  createdAt?: string;
}

// Document Interface
export interface Document {
  id: string;
  filename: string;
  content: string;
  fileType: "text" | "pdf" | "csv" | "docx";
  uploadedAt: string;
  processedAt?: string;
  entityCount?: number;
  relationshipCount?: number;
}

// Extracted Entity (from AI)
export interface ExtractedEntity {
  id?: string;
  type: EntityType;
  label: string;
  properties?: Record<string, any>;
  confidence?: number;
}

// Extracted Relationship (from AI)
export interface ExtractedRelationship {
  from: string; // Entity label or ID
  to: string; // Entity label or ID
  type: RelationshipType;
  confidence?: number;
  properties?: Record<string, any>;
}

// Entity Extraction Result
export interface EntityExtractionResult {
  entities: ExtractedEntity[];
  relationships: ExtractedRelationship[];
}

// Graph Data for Visualization
export interface GraphData {
  nodes: Entity[];
  edges: Relationship[];
}

