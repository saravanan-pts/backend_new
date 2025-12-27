import type { EntityType, RelationshipType } from "@/types";

// SurrealDB Schema Definitions
export const ENTITY_TYPES: EntityType[] = [
  "Person",
  "Organization",
  "Location",
  "Concept",
  "Event",
  "Technology",
];

export const RELATIONSHIP_TYPES: RelationshipType[] = [
  "RELATED_TO",
  "PART_OF",
  "WORKS_AT",
  "LOCATED_IN",
  "MENTIONS",
  "CREATED_BY",
];

// SurrealDB Table Definitions
export const TABLES = {
  ENTITY: "entity",
  RELATIONSHIP: "relationship",
  DOCUMENT: "document",
} as const;

// Schema validation functions
export function isValidEntityType(type: string): type is EntityType {
  return ENTITY_TYPES.includes(type as EntityType);
}

export function isValidRelationshipType(
  type: string
): type is RelationshipType {
  return RELATIONSHIP_TYPES.includes(type as RelationshipType);
}

// SurrealQL Schema Creation Queries
export const SCHEMA_QUERIES = {
  defineEntityTable: `
    DEFINE TABLE ${TABLES.ENTITY} SCHEMAFULL;
    DEFINE FIELD type ON ${TABLES.ENTITY} TYPE string ASSERT $value IN ['Person', 'Organization', 'Location', 'Concept', 'Event', 'Technology'];
    DEFINE FIELD label ON ${TABLES.ENTITY} TYPE string;
    DEFINE FIELD properties ON ${TABLES.ENTITY} TYPE object;
    DEFINE FIELD metadata ON ${TABLES.ENTITY} TYPE object;
    DEFINE FIELD createdAt ON ${TABLES.ENTITY} TYPE datetime DEFAULT time::now();
    DEFINE FIELD updatedAt ON ${TABLES.ENTITY} TYPE datetime DEFAULT time::now();
    DEFINE INDEX idx_label ON ${TABLES.ENTITY} FIELDS label;
    DEFINE INDEX idx_type ON ${TABLES.ENTITY} FIELDS type;
  `,
  defineRelationshipTable: `
    DEFINE TABLE ${TABLES.RELATIONSHIP} SCHEMAFULL;
    DEFINE FIELD from ON ${TABLES.RELATIONSHIP} TYPE record<${TABLES.ENTITY}>;
    DEFINE FIELD to ON ${TABLES.RELATIONSHIP} TYPE record<${TABLES.ENTITY}>;
    DEFINE FIELD type ON ${TABLES.RELATIONSHIP} TYPE string ASSERT $value IN ['RELATED_TO', 'PART_OF', 'WORKS_AT', 'LOCATED_IN', 'MENTIONS', 'CREATED_BY'];
    DEFINE FIELD properties ON ${TABLES.RELATIONSHIP} TYPE object;
    DEFINE FIELD confidence ON ${TABLES.RELATIONSHIP} TYPE float ASSERT $value >= 0.0 AND $value <= 1.0;
    DEFINE FIELD source ON ${TABLES.RELATIONSHIP} TYPE string;
    DEFINE FIELD createdAt ON ${TABLES.RELATIONSHIP} TYPE datetime DEFAULT time::now();
    DEFINE INDEX idx_from ON ${TABLES.RELATIONSHIP} FIELDS from;
    DEFINE INDEX idx_to ON ${TABLES.RELATIONSHIP} FIELDS to;
    DEFINE INDEX idx_type ON ${TABLES.RELATIONSHIP} FIELDS type;
  `,
  defineDocumentTable: `
    DEFINE TABLE ${TABLES.DOCUMENT} SCHEMAFULL;
    DEFINE FIELD filename ON ${TABLES.DOCUMENT} TYPE string;
    DEFINE FIELD content ON ${TABLES.DOCUMENT} TYPE string;
    DEFINE FIELD fileType ON ${TABLES.DOCUMENT} TYPE string ASSERT $value IN ['text', 'pdf', 'csv', 'docx'];
    DEFINE FIELD uploadedAt ON ${TABLES.DOCUMENT} TYPE datetime DEFAULT time::now();
    DEFINE FIELD processedAt ON ${TABLES.DOCUMENT} TYPE datetime;
    DEFINE FIELD entityCount ON ${TABLES.DOCUMENT} TYPE number;
    DEFINE FIELD relationshipCount ON ${TABLES.DOCUMENT} TYPE number;
    DEFINE INDEX idx_filename ON ${TABLES.DOCUMENT} FIELDS filename;
  `,
};

