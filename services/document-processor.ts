import { azureOpenAI } from "./azure-openai";
import { graphOps } from "./graph-operations";
import type {
  EntityExtractionResult,
  ExtractedEntity,
  ExtractedRelationship,
  Document,
  Entity,
} from "@/types";
import pdfParse from "pdf-parse";
import Papa from "papaparse";
import mammoth from "mammoth";

const MAX_CHUNK_SIZE_TOKENS = 8000;
const APPROX_CHARS_PER_TOKEN = 4; // Rough estimate

export class DocumentProcessor {
  async processText(text: string, filename: string = "text.txt"): Promise<{
    document: Document;
    entities: Entity[];
    relationships: any[];
  }> {
    const chunks = this.chunkText(text);
    const extractionResults: EntityExtractionResult[] = [];

    // Process each chunk
    for (const chunk of chunks) {
      try {
        const result = await azureOpenAI.extractEntitiesAndRelationships(chunk);
        extractionResults.push(result);
      } catch (error) {
        console.error(`Error processing chunk:`, error);
      }
    }

    // Merge results
    const merged = this.mergeExtractionResults(extractionResults);

    // Post-process relationships to map IDs/labels to actual entity labels
    const processedRelationships = this.postProcessRelationships(
      merged.relationships,
      merged.entities
    );

    // Create document record
    const document = await graphOps.createDocument({
      filename,
      content: text,
      fileType: "text",
      processedAt: new Date().toISOString(),
      entityCount: merged.entities.length,
      relationshipCount: processedRelationships.length,
    });

    // Create entities and relationships in database
    const createdEntities = await this.createEntitiesFromExtraction(
      merged.entities,
      document.id
    );
    const createdRelationships = await this.createRelationshipsFromExtraction(
      processedRelationships,
      createdEntities,
      document.id
    );

    return {
      document,
      entities: createdEntities,
      relationships: createdRelationships,
    };
  }

  async processPDF(file: File): Promise<{
    document: Document;
    entities: Entity[];
    relationships: any[];
  }> {
    const arrayBuffer = await file.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    const pdfData = await pdfParse(buffer);
    const text = pdfData.text;

    return this.processText(text, file.name);
  }

  async processCSV(file: File): Promise<{
    document: Document;
    entities: Entity[];
    relationships: any[];
  }> {
    const text = await file.text();

    return new Promise((resolve, reject) => {
      Papa.parse(text, {
        header: true,
        skipEmptyLines: true,
        complete: async (results) => {
          try {
            // Convert CSV rows to entities
            const entities: ExtractedEntity[] = [];
            const rows = results.data as any[];

            for (const row of rows) {
              // Create entity from each row
              // Assume first column is name/label, others are properties
              const keys = Object.keys(row);
              if (keys.length > 0) {
                const label = row[keys[0]] || "Unnamed";
                const properties: Record<string, any> = {};

                for (let i = 1; i < keys.length; i++) {
                  properties[keys[i]] = row[keys[i]];
                }

                entities.push({
                  type: "Organization", // Default, can be enhanced
                  label: String(label),
                  properties,
                  confidence: 1.0,
                });
              }
            }

            // Process entities through AI for relationship extraction
            const textForAI = rows
              .map((r) => Object.values(r).join(" "))
              .join("\n");

            const chunks = this.chunkText(textForAI);
            const extractionResults: EntityExtractionResult[] = [];

            for (const chunk of chunks) {
              try {
                const result =
                  await azureOpenAI.extractEntitiesAndRelationships(chunk);
                extractionResults.push(result);
              } catch (error) {
                console.error(`Error processing CSV chunk:`, error);
              }
            }

            const merged = this.mergeExtractionResults(extractionResults);
            merged.entities = [...merged.entities, ...entities];

            // Create document
            const document = await graphOps.createDocument({
              filename: file.name,
              content: text,
              fileType: "csv",
              processedAt: new Date().toISOString(),
              entityCount: merged.entities.length,
              relationshipCount: merged.relationships.length,
            });

            // Create entities and relationships
            const createdEntities = await this.createEntitiesFromExtraction(
              merged.entities,
              document.id
            );
            const createdRelationships =
              await this.createRelationshipsFromExtraction(
                merged.relationships,
                createdEntities,
                document.id
              );

            resolve({
              document,
              entities: createdEntities,
              relationships: createdRelationships,
            });
          } catch (error) {
            reject(error);
          }
        },
        error: (error: Error) => {
          reject(error);
        },
      });
    });
  }

  async processDOCX(file: File): Promise<{
    document: Document;
    entities: Entity[];
    relationships: any[];
  }> {
    const arrayBuffer = await file.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    const result = await mammoth.extractRawText({ buffer });
    const text = result.value;

    return this.processText(text, file.name);
  }

  private chunkText(text: string): string[] {
    const maxChars = MAX_CHUNK_SIZE_TOKENS * APPROX_CHARS_PER_TOKEN;
    const chunks: string[] = [];

    if (text.length <= maxChars) {
      return [text];
    }

    // Simple chunking by character count
    // Could be enhanced with sentence/paragraph boundaries
    for (let i = 0; i < text.length; i += maxChars) {
      chunks.push(text.substring(i, i + maxChars));
    }

    return chunks;
  }

  private mergeExtractionResults(
    results: EntityExtractionResult[]
  ): EntityExtractionResult {
    const entityMap = new Map<string, ExtractedEntity>();
    const relationships: ExtractedRelationship[] = [];

    // Merge entities (deduplicate by label)
    for (const result of results) {
      for (const entity of result.entities) {
        // Validate entity has required fields
        if (!entity.label) {
          console.warn("Skipping entity without label:", entity);
          continue;
        }
        
        const key = String(entity.label).toLowerCase().trim();
        if (!key) {
          console.warn("Skipping entity with empty label:", entity);
          continue;
        }
        
        const existing = entityMap.get(key);

        if (!existing || (entity.confidence || 0) > (existing.confidence || 0)) {
          entityMap.set(key, entity);
        }
      }

      // Validate relationships before adding
      for (const rel of result.relationships) {
        if (rel.from && rel.to && rel.type) {
          relationships.push(rel);
        } else {
          console.warn("Skipping invalid relationship:", rel);
        }
      }
    }

    return {
      entities: Array.from(entityMap.values()),
      relationships,
    };
  }

  private postProcessRelationships(
    relationships: ExtractedRelationship[],
    entities: ExtractedEntity[]
  ): ExtractedRelationship[] {
    // Create maps for entity lookup
    const entityLabelMap = new Map<string, string>(); // id -> label
    const entityLabelLowerMap = new Map<string, string>(); // lowercase label -> actual label
    
    entities.forEach(entity => {
      if (entity.id) {
        entityLabelMap.set(entity.id, entity.label);
      }
      if (entity.label) {
        const lowerLabel = entity.label.toLowerCase().trim();
        entityLabelLowerMap.set(lowerLabel, entity.label);
      }
    });

    // Process relationships to map IDs/variations to actual labels
    const processed: ExtractedRelationship[] = [];
    
    for (const rel of relationships) {
      if (!rel.from || !rel.to || !rel.type) {
        continue;
      }

      // Try to find matching entity label
      let fromLabel = rel.from;
      let toLabel = rel.to;

      // Check if it's an ID (starts with entity type prefix)
      if (entityLabelMap.has(rel.from)) {
        fromLabel = entityLabelMap.get(rel.from)!;
      } else {
        // Try lowercase match
        const fromLower = rel.from.toLowerCase().trim();
        if (entityLabelLowerMap.has(fromLower)) {
          fromLabel = entityLabelLowerMap.get(fromLower)!;
        }
      }

      if (entityLabelMap.has(rel.to)) {
        toLabel = entityLabelMap.get(rel.to)!;
      } else {
        // Try lowercase match
        const toLower = rel.to.toLowerCase().trim();
        if (entityLabelLowerMap.has(toLower)) {
          toLabel = entityLabelLowerMap.get(toLower)!;
        }
      }

      // Only include if both labels match actual entities
      if (entityLabelLowerMap.has(fromLabel.toLowerCase().trim()) && 
          entityLabelLowerMap.has(toLabel.toLowerCase().trim())) {
        processed.push({
          ...rel,
          from: fromLabel,
          to: toLabel,
        });
      } else {
        console.warn(
          `Skipping relationship after post-processing: ${rel.from} -> ${rel.to} (labels not found)`
        );
      }
    }

    return processed;
  }

  private async createEntitiesFromExtraction(
    extractedEntities: ExtractedEntity[],
    sourceDocumentId: string
  ): Promise<Entity[]> {
    const createdEntities: Entity[] = [];

    for (const extracted of extractedEntities) {
      try {
        // Check if entity already exists by label
        const existing = await graphOps.searchEntities(extracted.label);
        let entity: Entity;

        if (existing.length > 0) {
          // Use existing entity
          entity = existing[0];
        } else {
          // Create new entity
          entity = await graphOps.createEntity({
            type: extracted.type,
            label: extracted.label,
            properties: extracted.properties || {},
            metadata: {
              source: sourceDocumentId,
              confidence: extracted.confidence,
            },
          });
        }

        createdEntities.push(entity);
      } catch (error) {
        console.error(`Error creating entity ${extracted.label}:`, error);
      }
    }

    return createdEntities;
  }

  private async createRelationshipsFromExtraction(
    extractedRelationships: ExtractedRelationship[],
    entities: Entity[],
    sourceDocumentId: string
  ): Promise<any[]> {
    const createdRelationships: any[] = [];

    // Create label to entity map
    const entityMap = new Map<string, Entity>();
    for (const entity of entities) {
      if (entity.label) {
        entityMap.set(String(entity.label).toLowerCase().trim(), entity);
      }
    }

    for (const extracted of extractedRelationships) {
      try {
        // Validate relationship data
        if (!extracted.from || !extracted.to || !extracted.type) {
          console.warn(
            `Skipping invalid relationship: missing from, to, or type`,
            extracted
          );
          continue;
        }

        const fromKey = String(extracted.from).toLowerCase().trim();
        const toKey = String(extracted.to).toLowerCase().trim();
        
        const fromEntity = entityMap.get(fromKey);
        const toEntity = entityMap.get(toKey);

        if (!fromEntity || !toEntity) {
          console.warn(
            `Skipping relationship: entities not found (${extracted.from} -> ${extracted.to})`
          );
          continue;
        }

        const relationship = await graphOps.createRelationship(
          fromEntity.id,
          toEntity.id,
          extracted.type,
          extracted.properties,
          extracted.confidence,
          String(sourceDocumentId) // Ensure source is a string, not a record
        );

        createdRelationships.push(relationship);
      } catch (error) {
        console.error(
          `Error creating relationship (${extracted.from} -> ${extracted.to}):`,
          error
        );
      }
    }

    return createdRelationships;
  }
}

export const documentProcessor = new DocumentProcessor();

