import { surrealDB } from "@/lib/surrealdb-client";
import { TABLES } from "@/lib/schema";
import type {
  Entity,
  Relationship,
  Document,
} from "@/types";
import { GraphDriver } from "./graph-driver.interface";

export class SurrealGraphDriver implements GraphDriver {
  // Entity Operations
  async createEntity(entity: Omit<Entity, "id" | "createdAt" | "updatedAt">): Promise<Entity> {
    try {
      const db = surrealDB.getClient();

      // Use query with type::datetime() for proper datetime handling
      const now = new Date().toISOString();
      
      const query = `
        CREATE ${TABLES.ENTITY} CONTENT {
          type: $type,
          label: $label,
          properties: $properties,
          metadata: $metadata,
          createdAt: type::datetime($createdAt),
          updatedAt: type::datetime($updatedAt)
        }
      `;
      
      const result = await db.query(query, {
        type: entity.type,
        label: entity.label,
        properties: entity.properties || {},
        metadata: entity.metadata || {},
        createdAt: now,
        updatedAt: now,
      });
      
      const records = Array.isArray(result) && result[0] && 'result' in result[0] 
        ? (result[0].result as any[])
        : [];
      const record = records[0];
      if (!record) {
        throw new Error("Failed to create entity record");
      }

      return this.mapRecordToEntity(record);
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        throw new Error("Insufficient permissions to create entities. Please check your JWT token permissions.");
      }
      throw error;
    }
  }

  async updateEntity(
    id: string,
    updates: Partial<Omit<Entity, "id" | "createdAt">>
  ): Promise<Entity> {
    try {
      const db = surrealDB.getClient();

      // Build update query with proper datetime handling
      const updateFields: string[] = [];
      const params: any = { id };

      Object.keys(updates).forEach(key => {
        if (key !== 'createdAt' && key !== 'updatedAt' && updates[key as keyof typeof updates] !== undefined) {
          updateFields.push(`${key}: $${key}`);
          params[key] = updates[key as keyof typeof updates];
        }
      });

      updateFields.push('updatedAt: type::datetime()');

      const query = `
        UPDATE $id CONTENT {
          ${updateFields.join(',\n          ')}
        }
      `;

      const result = await db.query(query, params);
      const records = Array.isArray(result) && result[0] && 'result' in result[0] 
        ? (result[0].result as any[])
        : [];
      const record = records[0];
      
      if (!record) {
        throw new Error("Failed to update entity record");
      }

      return this.mapRecordToEntity(record);
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        throw new Error("Insufficient permissions to update entities. Please check your JWT token permissions.");
      }
      throw error;
    }
  }

  async deleteEntity(id: string): Promise<void> {
    const db = surrealDB.getClient();

    // Delete all relationships connected to this entity
    await db.query(
      `DELETE FROM ${TABLES.RELATIONSHIP} WHERE from = $id OR to = $id`,
      { id }
    );

    // Delete the entity
    await db.delete(id);
  }

  async getEntity(id: string): Promise<Entity | null> {
    const db = surrealDB.getClient();
    const record = await db.select(id);

    if (!record) return null;
    return this.mapRecordToEntity(record);
  }

  async getAllEntities(): Promise<Entity[]> {
    try {
      const db = surrealDB.getClient();
      const records = await db.select(TABLES.ENTITY);

      return Array.isArray(records)
        ? records.map((r) => this.mapRecordToEntity(r))
        : [];
    } catch (error: any) {
      // Handle permission errors gracefully
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        console.warn("Insufficient permissions to read entities. Returning empty array.");
        return [];
      }
      throw error;
    }
  }

  async searchEntities(query: string): Promise<Entity[]> {
    const db = surrealDB.getClient();
    const results = await db.query(
      `SELECT * FROM ${TABLES.ENTITY} WHERE label ~ $query OR properties.* ~ $query`,
      { query }
    );

    const records = results[0]?.result || [];
    return Array.isArray(records)
      ? records.map((r) => this.mapRecordToEntity(r))
      : [];
  }

  async getNeighbors(entityId: string, depth: number = 1): Promise<{
    entities: Entity[];
    relationships: Relationship[];
  }> {
    const db = surrealDB.getClient();

    // Build query for neighbors up to N hops
    let query = `SELECT * FROM ${TABLES.ENTITY} WHERE id IN (`;
    const neighborQueries: string[] = [];

    for (let i = 1; i <= depth; i++) {
      if (i === 1) {
        neighborQueries.push(
          `SELECT from FROM ${TABLES.RELATIONSHIP} WHERE to = $id`
        );
        neighborQueries.push(
          `SELECT to FROM ${TABLES.RELATIONSHIP} WHERE from = $id`
        );
      } else {
        // For deeper levels, we'd need recursive queries
        // Simplified for now - can be enhanced
        neighborQueries.push(
          `SELECT from FROM ${TABLES.RELATIONSHIP} WHERE to IN (SELECT from FROM ${TABLES.RELATIONSHIP} WHERE to = $id)`
        );
      }
    }

    query += neighborQueries.join(" UNION ") + ")";

    const entityResults = await db.query(query, { id: entityId });
    const resultData = entityResults[0]?.result;
    const entities = Array.isArray(resultData)
      ? resultData.map((r: any) => this.mapRecordToEntity(r))
      : [];

    // Get relationships
    const relationshipResults = await db.query(
      `SELECT * FROM ${TABLES.RELATIONSHIP} WHERE from = $id OR to = $id`,
      { id: entityId }
    );
    const relResultData = relationshipResults[0]?.result;
    const relationships = Array.isArray(relResultData)
      ? relResultData.map((r: any) => this.mapRecordToRelationship(r))
      : [];

    return { entities, relationships };
  }

  async getSubgraph(entityIds: string[]): Promise<{
    entities: Entity[];
    relationships: Relationship[];
  }> {
    const db = surrealDB.getClient();

    const ids = entityIds.map((id) => `'${id}'`).join(", ");

    const entityResults = await db.query(
      `SELECT * FROM ${TABLES.ENTITY} WHERE id IN [${ids}]`
    );
    const entityResultData = entityResults[0]?.result;
    const entities = Array.isArray(entityResultData)
      ? entityResultData.map((r: any) => this.mapRecordToEntity(r))
      : [];

    const relationshipResults = await db.query(
      `SELECT * FROM ${TABLES.RELATIONSHIP} WHERE from IN [${ids}] AND to IN [${ids}]`
    );
    const relResultData = relationshipResults[0]?.result;
    const relationships = Array.isArray(relResultData)
      ? relResultData.map((r: any) => this.mapRecordToRelationship(r))
      : [];

    return { entities, relationships };
  }

  // Relationship Operations
  async createRelationship(
    from: string,
    to: string,
    type: Relationship["type"],
    properties?: Relationship["properties"],
    confidence?: number,
    source?: string
  ): Promise<Relationship> {
    try {
      const db = surrealDB.getClient();

      // Use query to ensure source is treated as a string, not a record reference
      // SurrealDB auto-converts strings that look like record IDs, so we need to be explicit
      const now = new Date().toISOString();
      
      const query = `
        CREATE ${TABLES.RELATIONSHIP} CONTENT {
          from: $from,
          to: $to,
          type: $type,
          properties: $properties,
          confidence: $confidence,
          source: type::string($source),
          createdAt: type::datetime($createdAt)
        }
      `;
      
      const result = await db.query(query, {
        from,
        to,
        type,
        properties: properties || {},
        confidence: confidence ?? 1.0,
        source: source || "manual",
        createdAt: now,
      });
      
      const records = result[0]?.result;
      if (!records || !Array.isArray(records) || records.length === 0) {
        throw new Error("Failed to create relationship record");
      }
      
      const record = records[0];

      return this.mapRecordToRelationship(record);
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        throw new Error("Insufficient permissions to create relationships. Please check your JWT token permissions.");
      }
      throw error;
    }
  }

  async updateRelationship(
    id: string,
    updates: Partial<Omit<Relationship, "id" | "createdAt">>
  ): Promise<Relationship> {
    try {
      const db = surrealDB.getClient();

      // Build update query with proper handling
      const updateFields: string[] = [];
      const params: any = { id };

      Object.keys(updates).forEach(key => {
        if (key !== 'createdAt' && updates[key as keyof typeof updates] !== undefined) {
          if (key === 'source') {
            // Ensure source is treated as string
            updateFields.push(`${key}: type::string($${key})`);
          } else {
            updateFields.push(`${key}: $${key}`);
          }
          params[key] = updates[key as keyof typeof updates];
        }
      });

      if (updateFields.length === 0) {
        throw new Error("No valid fields to update");
      }

      const query = `
        UPDATE $id CONTENT {
          ${updateFields.join(',\n          ')}
        }
      `;

      const result = await db.query(query, params);
      const records = Array.isArray(result) && result[0] && 'result' in result[0] 
        ? (result[0].result as any[])
        : [];
      const record = records[0];
      
      if (!record) {
        throw new Error("Failed to update relationship record");
      }

      return this.mapRecordToRelationship(record);
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        throw new Error("Insufficient permissions to update relationships. Please check your JWT token permissions.");
      }
      throw error;
    }
  }

  async deleteRelationship(id: string): Promise<void> {
    const db = surrealDB.getClient();
    await db.delete(id);
  }

  async getRelationship(id: string): Promise<Relationship | null> {
    try {
      const db = surrealDB.getClient();
      const record = await db.select(id);

      if (!record) return null;
      return this.mapRecordToRelationship(record);
    } catch (error: any) {
      // Handle permission errors gracefully
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        console.warn("Insufficient permissions to read relationship. Returning null.");
        return null;
      }
      throw error;
    }
  }

  async getAllRelationships(documentId?: string): Promise<Relationship[]> {
    try {
      const db = surrealDB.getClient();
      
      if (documentId) {
        // Filter relationships by source document
        const query = `SELECT * FROM ${TABLES.RELATIONSHIP} WHERE source = $documentId`;
        const result = await db.query(query, { documentId });
        const records = Array.isArray(result) && result[0] && 'result' in result[0] 
          ? (result[0].result as any[])
          : [];
        return records.map((r) => this.mapRecordToRelationship(r));
      } else {
        // Get all relationships
        const records = await db.select(TABLES.RELATIONSHIP);
        return Array.isArray(records)
          ? records.map((r) => this.mapRecordToRelationship(r))
          : [];
      }
    } catch (error: any) {
      // Handle permission errors gracefully
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        console.warn("Insufficient permissions to read relationships. Returning empty array.");
        return [];
      }
      throw error;
    }
  }

  async getAllDocuments(): Promise<Document[]> {
    try {
      const db = surrealDB.getClient();
      const records = await db.select(TABLES.DOCUMENT);

      return Array.isArray(records)
        ? records.map((r) => this.mapRecordToDocument(r))
        : [];
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        console.warn("Insufficient permissions to read documents. Returning empty array.");
        return [];
      }
      throw error;
    }
  }

  async getEntitiesByDocument(documentId: string): Promise<Entity[]> {
    try {
      const db = surrealDB.getClient();
      // Entities have metadata.source pointing to document ID
      const query = `SELECT * FROM ${TABLES.ENTITY} WHERE metadata.source = $documentId`;
      const result = await db.query(query, { documentId });
      const records = Array.isArray(result) && result[0] && 'result' in result[0] 
        ? (result[0].result as any[])
        : [];
      return records.map((r) => this.mapRecordToEntity(r));
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        console.warn("Insufficient permissions to read entities. Returning empty array.");
        return [];
      }
      throw error;
    }
  }

  // Document Operations
  async createDocument(
    document: Omit<Document, "id" | "uploadedAt">
  ): Promise<Document> {
    try {
      const db = surrealDB.getClient();

      // Build the document data
      // Use query with type::datetime() for proper datetime conversion
      const processedAtValue = document.processedAt || new Date().toISOString();
      
      const query = `
        CREATE ${TABLES.DOCUMENT} CONTENT {
          filename: $filename,
          content: $content,
          fileType: $fileType,
          processedAt: type::datetime($processedAt),
          entityCount: $entityCount,
          relationshipCount: $relationshipCount
        }
      `;
      
      const result = await db.query(query, {
        filename: document.filename,
        content: document.content,
        fileType: document.fileType,
        processedAt: processedAtValue,
        entityCount: document.entityCount ?? 0,
        relationshipCount: document.relationshipCount ?? 0,
      });
      
      const records = Array.isArray(result) && result[0] && 'result' in result[0] 
        ? (result[0].result as any[])
        : [];
      const record = records[0];
      if (!record) {
        throw new Error("Failed to create document record");
      }

      return this.mapRecordToDocument(record);
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        throw new Error("Insufficient permissions to create documents. Please check your JWT token permissions.");
      }
      if (error?.message?.includes("field `id`")) {
        throw new Error("Schema error: The document table has an invalid id field definition. Please remove it from the schema or restart the connection to auto-fix.");
      }
      throw error;
    }
  }

  async updateDocument(
    id: string,
    updates: Partial<Document>
  ): Promise<Document> {
    const db = surrealDB.getClient();
    const record = await db.merge(id, updates);
    return this.mapRecordToDocument(record);
  }

  /**
   * Clear all data from the database (entities, relationships, and documents)
   * WARNING: This will permanently delete all data!
   */
  async clearAllData(): Promise<{ entitiesDeleted: number; relationshipsDeleted: number; documentsDeleted: number }> {
    try {
      const db = surrealDB.getClient();
      
      // Get counts before deletion for reporting
      const entities = await this.getAllEntities();
      const relationships = await this.getAllRelationships();
      const documents = await this.getAllDocuments();
      
      const entityCount = entities.length;
      const relationshipCount = relationships.length;
      const documentCount = documents.length;

      // Delete in order: relationships first (they reference entities), then entities, then documents
      // Use DELETE FROM table to delete all records efficiently
      if (relationshipCount > 0) {
        await db.query(`DELETE FROM ${TABLES.RELATIONSHIP}`);
      }
      
      if (entityCount > 0) {
        await db.query(`DELETE FROM ${TABLES.ENTITY}`);
      }
      
      if (documentCount > 0) {
        await db.query(`DELETE FROM ${TABLES.DOCUMENT}`);
      }

      return {
        entitiesDeleted: entityCount,
        relationshipsDeleted: relationshipCount,
        documentsDeleted: documentCount,
      };
    } catch (error: any) {
      if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
        throw new Error("Insufficient permissions to clear data. Please check your JWT token permissions.");
      }
      throw error;
    }
  }

  // Helper Methods
  private mapRecordToEntity(record: any): Entity {
    // Handle SurrealDB query result format
    // Query results come as: { result: [{ id: "...", ... }] }
    // Direct select/create results come as: { id: "...", ... }
    const entity = Array.isArray(record) ? record[0] : record;
    
    if (!entity || typeof entity !== 'object') {
      throw new Error(`Invalid entity record format: ${JSON.stringify(record)}`);
    }
    
    return {
      id: entity.id || String(entity),
      type: entity.type,
      label: entity.label,
      properties: entity.properties || {},
      metadata: entity.metadata || {},
      createdAt: entity.createdAt,
      updatedAt: entity.updatedAt,
    };
  }

  private mapRecordToRelationship(record: any): Relationship {
    // Handle SurrealDB query result format
    const rel = Array.isArray(record) ? record[0] : record;
    
    if (!rel || typeof rel !== 'object') {
      throw new Error(`Invalid relationship record format: ${JSON.stringify(record)}`);
    }
    
    return {
      id: rel.id || String(rel),
      from: rel.from,
      to: rel.to,
      type: rel.type,
      properties: rel.properties || {},
      confidence: rel.confidence,
      source: rel.source,
      createdAt: rel.createdAt,
    };
  }

  private mapRecordToDocument(record: any): Document {
    return {
      id: record.id || record,
      filename: record.filename,
      content: record.content,
      fileType: record.fileType,
      uploadedAt: record.uploadedAt,
      processedAt: record.processedAt,
      entityCount: record.entityCount,
      relationshipCount: record.relationshipCount,
    };
  }
}
