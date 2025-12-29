import { graph } from "@/services/graph";
import type { Entity, Relationship } from "@/types";

export const sampleEntities: Omit<Entity, "id" | "createdAt" | "updatedAt">[] = [
  {
    type: "Person",
    label: "John Doe",
    properties: {
      email: "john.doe@example.com",
      role: "Software Engineer",
      department: "Engineering",
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Person",
    label: "Jane Smith",
    properties: {
      email: "jane.smith@example.com",
      role: "Product Manager",
      department: "Product",
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Organization",
    label: "Acme Corporation",
    properties: {
      industry: "Technology",
      founded: "2020",
      employees: 500,
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Organization",
    label: "Tech Startup Inc",
    properties: {
      industry: "Software",
      founded: "2022",
      employees: 50,
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Location",
    label: "San Francisco",
    properties: {
      country: "USA",
      state: "California",
      population: 873965,
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Location",
    label: "New York",
    properties: {
      country: "USA",
      state: "New York",
      population: 8336817,
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Technology",
    label: "React",
    properties: {
      type: "Frontend Framework",
      language: "JavaScript",
      version: "18.0",
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Technology",
    label: "Node.js",
    properties: {
      type: "Runtime",
      language: "JavaScript",
      version: "20.0",
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Concept",
    label: "Machine Learning",
    properties: {
      category: "AI",
      description: "Subset of artificial intelligence",
    },
    metadata: {
      confidence: 1.0,
    },
  },
  {
    type: "Event",
    label: "Tech Conference 2024",
    properties: {
      date: "2024-06-15",
      location: "San Francisco",
      attendees: 1000,
    },
    metadata: {
      confidence: 1.0,
    },
  },
];

export const sampleRelationships: Array<{
  fromLabel: string;
  toLabel: string;
  type: Relationship["type"];
  properties?: Record<string, any>;
  confidence?: number;
}> = [
  {
    fromLabel: "John Doe",
    toLabel: "Acme Corporation",
    type: "WORKS_AT",
    properties: {
      startDate: "2021-01-15",
      position: "Senior Engineer",
    },
    confidence: 1.0,
  },
  {
    fromLabel: "Jane Smith",
    toLabel: "Acme Corporation",
    type: "WORKS_AT",
    properties: {
      startDate: "2020-06-01",
      position: "Product Manager",
    },
    confidence: 1.0,
  },
  {
    fromLabel: "Acme Corporation",
    toLabel: "San Francisco",
    type: "LOCATED_IN",
    confidence: 1.0,
  },
  {
    fromLabel: "Tech Startup Inc",
    toLabel: "New York",
    type: "LOCATED_IN",
    confidence: 1.0,
  },
  {
    fromLabel: "John Doe",
    toLabel: "React",
    type: "CREATED_BY",
    properties: {
      expertise: "Expert",
    },
    confidence: 0.9,
  },
  {
    fromLabel: "Acme Corporation",
    toLabel: "React",
    type: "RELATED_TO",
    properties: {
      usage: "Primary Framework",
    },
    confidence: 0.8,
  },
  {
    fromLabel: "Machine Learning",
    toLabel: "Tech Conference 2024",
    type: "MENTIONS",
    confidence: 0.7,
  },
  {
    fromLabel: "Tech Startup Inc",
    toLabel: "Acme Corporation",
    type: "RELATED_TO",
    properties: {
      relationship: "Competitor",
    },
    confidence: 0.6,
  },
];

export async function populateTestData(): Promise<{
  entities: Entity[];
  relationships: Relationship[];
}> {
  console.log("Populating test data...");

  // Create entities
  const createdEntities: Entity[] = [];
  for (const entityData of sampleEntities) {
    try {
      const entity = await graph.createEntity(entityData);
      createdEntities.push(entity);
    } catch (error) {
      console.error(`Error creating entity ${entityData.label}:`, error);
    }
  }

  // Create label to ID map
  const labelToId = new Map<string, string>();
  for (const entity of createdEntities) {
    labelToId.set(entity.label, entity.id);
  }

  // Create relationships
  const createdRelationships: Relationship[] = [];
  for (const relData of sampleRelationships) {
    const fromId = labelToId.get(relData.fromLabel);
    const toId = labelToId.get(relData.toLabel);

    if (!fromId || !toId) {
      console.warn(
        `Skipping relationship: entities not found (${relData.fromLabel} -> ${relData.toLabel})`
      );
      continue;
    }

    try {
      const relationship = await graph.createRelationship(
        fromId,
        toId,
        relData.type,
        relData.properties,
        relData.confidence,
        "test-data"
      );
      createdRelationships.push(relationship);
    } catch (error) {
      console.error(
        `Error creating relationship (${relData.fromLabel} -> ${relData.toLabel}):`,
        error
      );
    }
  }

  console.log(
    `Test data populated: ${createdEntities.length} entities, ${createdRelationships.length} relationships`
  );

  return {
    entities: createdEntities,
    relationships: createdRelationships,
  };
}

export async function clearTestData(): Promise<void> {
  console.log("Clearing test data...");

  try {
    const allEntities = await graph.getAllEntities();
    const allRelationships = await graph.getAllRelationships();

    // Delete all relationships first
    for (const rel of allRelationships) {
      try {
        await graph.deleteRelationship(rel.id);
      } catch (error) {
        console.error(`Error deleting relationship ${rel.id}:`, error);
      }
    }

    // Delete all entities
    for (const entity of allEntities) {
      try {
        await graph.deleteEntity(entity.id);
      } catch (error) {
        console.error(`Error deleting entity ${entity.id}:`, error);
      }
    }

    console.log("Test data cleared");
  } catch (error) {
    console.error("Error clearing test data:", error);
    throw error;
  }
}

