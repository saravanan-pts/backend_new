import type { GraphDriver } from "./graph-driver.interface";
import type { Entity, Relationship, Document } from "@/types";
import { getGremlinClient } from "@/lib/cosmos-gremlin-client";
import { randomUUID } from "crypto";

/**
 * Cosmos DB Gremlin Driver
 * Uses ONLY client.submit()
 * Fully Cosmos-supported
 */
export class CosmosGremlinGraphDriver implements GraphDriver {

  /* =========================================================
     ENTITY OPERATIONS
     ========================================================= */

  async createEntity(
    entity: Omit<Entity, "id" | "createdAt" | "updatedAt">
  ): Promise<Entity> {
    const client = getGremlinClient();

    const id = randomUUID();
    const now = new Date().toISOString();
    const pk = entity.type;

    const query = `
      g.addV(label)
        .property('id', id)
        .property('pk', pk)
        .property('label', labelValue)
        .property('properties', props)
        .property('metadata', meta)
        .property('createdAt', createdAt)
        .property('updatedAt', updatedAt)
    `;

    await client.submit(query, {
      label: entity.type,
      labelValue: entity.label,
      id,
      pk,
      props: JSON.stringify(entity.properties ?? {}),
      meta: JSON.stringify(entity.metadata ?? {}),
      createdAt: now,
      updatedAt: now,
    });

    return {
      id,
      type: entity.type,
      label: entity.label,
      properties: entity.properties ?? {},
      metadata: entity.metadata ?? {},
      createdAt: now,
      updatedAt: now,
    };
  }

  async getAllEntities(): Promise<Entity[]> {
    const client = getGremlinClient();

    const res = await client.submit("g.V().valueMap(true)");

    return res._items.map((v: any) => ({
      id: v.id,
      type: v.label,
      label: v.label,
      properties: JSON.parse(v.properties?.[0] ?? "{}"),
      metadata: JSON.parse(v.metadata?.[0] ?? "{}"),
      createdAt: v.createdAt?.[0],
      updatedAt: v.updatedAt?.[0],
    }));
  }

  async getEntity(id: string): Promise<Entity | null> {
    const client = getGremlinClient();

    const res = await client.submit(
      "g.V([pk, id]).valueMap(true)",
      { id, pk: undefined } // pk optional if id is globally unique
    );

    if (!res._items.length) return null;

    const v = res._items[0];
    return {
      id: v.id,
      type: v.label,
      label: v.label,
      properties: JSON.parse(v.properties?.[0] ?? "{}"),
      metadata: JSON.parse(v.metadata?.[0] ?? "{}"),
      createdAt: v.createdAt?.[0],
      updatedAt: v.updatedAt?.[0],
    };
  }

  async deleteEntity(id: string): Promise<void> {
    const client = getGremlinClient();
    await client.submit("g.V(id).drop()", { id });
  }

  async updateEntity(): Promise<Entity> {
    throw new Error("CosmosGremlin: updateEntity not supported");
  }

  async searchEntities(): Promise<Entity[]> {
    throw new Error("CosmosGremlin: searchEntities not supported");
  }

  async getNeighbors(): Promise<{ entities: Entity[]; relationships: Relationship[] }> {
    throw new Error("CosmosGremlin: getNeighbors not implemented");
  }

  async getSubgraph(): Promise<{ entities: Entity[]; relationships: Relationship[] }> {
    throw new Error("CosmosGremlin: getSubgraph not implemented");
  }

  /* =========================================================
     RELATIONSHIP OPERATIONS
     ========================================================= */

  async createRelationship(
    from: string,
    to: string,
    type: Relationship["type"],
    properties: Relationship["properties"] = {},
    confidence = 1.0,
    source = "manual"
  ): Promise<Relationship> {
    const client = getGremlinClient();

    const id = randomUUID();
    const now = new Date().toISOString();

    const query = `
      g.V(from)
        .addE(edgeLabel)
        .to(g.V(to))
        .property('id', id)
        .property('properties', props)
        .property('confidence', confidence)
        .property('source', source)
        .property('createdAt', createdAt)
    `;

    await client.submit(query, {
      from,
      to,
      edgeLabel: type,
      id,
      props: JSON.stringify(properties),
      confidence,
      source,
      createdAt: now,
    });

    return {
      id,
      from,
      to,
      type,
      properties,
      confidence,
      source,
      createdAt: now,
    };
  }

  async getAllRelationships(): Promise<Relationship[]> {
    const client = getGremlinClient();

    const res = await client.submit(`
      g.E()
        .project('id','from','to','type','properties','confidence','source','createdAt')
        .by('id')
        .by(outV().id())
        .by(inV().id())
        .by(label())
        .by(values('properties'))
        .by(values('confidence'))
        .by(values('source'))
        .by(values('createdAt'))
    `);

    return res._items.map((e: any) => ({
      id: e.id,
      from: e.from,
      to: e.to,
      type: e.type,
      properties: JSON.parse(e.properties ?? "{}"),
      confidence: e.confidence,
      source: e.source,
      createdAt: e.createdAt,
    }));
  }

  async getRelationship(): Promise<Relationship | null> {
    throw new Error("CosmosGremlin: getRelationship not supported");
  }

  async updateRelationship(): Promise<Relationship> {
    throw new Error("CosmosGremlin: updateRelationship not supported");
  }

  async deleteRelationship(id: string): Promise<void> {
    const client = getGremlinClient();
    await client.submit("g.E(id).drop()", { id });
  }

  /* =========================================================
     DOCUMENT OPERATIONS (NOT SUPPORTED)
     ========================================================= */

  async createDocument(): Promise<Document> {
    throw new Error("CosmosGremlin: Documents not supported");
  }

  async updateDocument(): Promise<Document> {
    throw new Error("CosmosGremlin: Documents not supported");
  }

  async getAllDocuments(): Promise<Document[]> {
    throw new Error("CosmosGremlin: Documents not supported");
  }

  async getEntitiesByDocument(): Promise<Entity[]> {
    throw new Error("CosmosGremlin: Documents not supported");
  }

  async clearAllData() {
    const client = getGremlinClient();

    const vCount = (await client.submit("g.V().count()"))._items[0];
    const eCount = (await client.submit("g.E().count()"))._items[0];

    await client.submit("g.E().drop()");
    await client.submit("g.V().drop()");

    return {
      entitiesDeleted: vCount ?? 0,
      relationshipsDeleted: eCount ?? 0,
      documentsDeleted: 0,
    };
  }
}
