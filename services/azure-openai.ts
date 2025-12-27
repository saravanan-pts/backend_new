import { OpenAIClient, AzureKeyCredential } from "@azure/openai";
import type {
  EntityExtractionResult,
  ExtractedEntity,
  ExtractedRelationship,
  EntityType,
  RelationshipType,
} from "@/types";

export class AzureOpenAIService {
  private client: OpenAIClient | null = null;
  private maxRetries: number = 3;
  private baseDelay: number = 1000;

  constructor() {
    this.initializeClient();
  }

  private initializeClient(): void {
    const endpoint = process.env.AZURE_OPENAI_ENDPOINT;
    const apiKey = process.env.AZURE_OPENAI_API_KEY;

    if (!endpoint || !apiKey) {
      console.warn(
        "Azure OpenAI credentials not found. Entity extraction will not work."
      );
      return;
    }

    try {
      this.client = new OpenAIClient(
        endpoint,
        new AzureKeyCredential(apiKey)
      );
    } catch (error) {
      console.error("Failed to initialize Azure OpenAI client:", error);
    }
  }

  async extractEntitiesAndRelationships(
    text: string
  ): Promise<EntityExtractionResult> {
    if (!this.client) {
      throw new Error("Azure OpenAI client not initialized");
    }

    const deploymentName = process.env.AZURE_OPENAI_DEPLOYMENT_NAME;
    if (!deploymentName) {
      throw new Error("AZURE_OPENAI_DEPLOYMENT_NAME not configured");
    }

    const prompt = this.buildExtractionPrompt(text);

    let attempt = 0;
    while (attempt < this.maxRetries) {
      try {
        const response = await this.client.getChatCompletions(
          deploymentName,
          [
            {
              role: "system",
              content:
                "You are an expert at extracting entities and relationships from text. Always respond with valid JSON only, no additional text.",
            },
            {
              role: "user",
              content: prompt,
            },
          ],
          {
            temperature: 0.3,
            maxTokens: 4000,
            responseFormat: { type: "json_object" },
          }
        );

        const content = response.choices[0]?.message?.content;
        if (!content) {
          throw new Error("No content in OpenAI response");
        }

        const result = this.parseExtractionResult(content);
        return result;
      } catch (error: any) {
        attempt++;
        if (attempt >= this.maxRetries) {
          console.error("Failed to extract entities after retries:", error);
          throw error;
        }

        // Exponential backoff
        const delay = this.baseDelay * Math.pow(2, attempt - 1);
        console.log(`Retrying entity extraction in ${delay}ms...`);
        await this.sleep(delay);
      }
    }

    throw new Error("Failed to extract entities after all retries");
  }

  private buildExtractionPrompt(text: string): string {
    return `Analyze the following text and extract entities and relationships. Return a JSON object with this exact structure:

{
  "entities": [
    {
      "id": "optional-unique-id",
      "type": "Person|Organization|Location|Concept|Event|Technology",
      "label": "Entity name",
      "properties": {},
      "confidence": 0.0-1.0
    }
  ],
  "relationships": [
    {
      "from": "EXACT_ENTITY_LABEL",
      "to": "EXACT_ENTITY_LABEL",
      "type": "RELATED_TO|PART_OF|WORKS_AT|LOCATED_IN|MENTIONS|CREATED_BY",
      "confidence": 0.0-1.0,
      "properties": {}
    }
  ]
}

CRITICAL INSTRUCTIONS:
1. For relationships, ALWAYS use the EXACT "label" value from the entities array
2. DO NOT use IDs, generic names, or variations - use the exact label as it appears in entities
3. Match relationship "from" and "to" fields exactly to entity labels
4. Example: If an entity has label "Orion Logistics", use "Orion Logistics" (not "Orion", "orion_logistics", or "org1")

Entity Types:
- Person: Individual people
- Organization: Companies, institutions, groups
- Location: Places, cities, countries
- Concept: Abstract ideas, topics
- Event: Occurrences, happenings
- Technology: Technologies, tools, systems

Relationship Types:
- RELATED_TO: General relationship
- PART_OF: Entity is part of another
- WORKS_AT: Person works at organization
- LOCATED_IN: Entity is located in a place
- MENTIONS: Entity mentions or references another
- CREATED_BY: Entity was created by another

Text to analyze:
${text.substring(0, 8000)}`;
  }

  private parseExtractionResult(
    content: string
  ): EntityExtractionResult {
    try {
      // Remove markdown code blocks if present
      const cleanedContent = content
        .replace(/```json\n?/g, "")
        .replace(/```\n?/g, "")
        .trim();

      const parsed = JSON.parse(cleanedContent);

      // Validate and normalize structure
      const entities: ExtractedEntity[] = Array.isArray(parsed.entities)
        ? parsed.entities.map((e: any) => ({
            id: e.id,
            type: this.normalizeEntityType(e.type),
            label: e.label || String(e.name || e.title || ""),
            properties: e.properties || {},
            confidence: this.normalizeConfidence(e.confidence),
          }))
        : [];

      const relationships: ExtractedRelationship[] = Array.isArray(
        parsed.relationships
      )
        ? parsed.relationships.map((r: any) => ({
            from: String(r.from || ""),
            to: String(r.to || ""),
            type: this.normalizeRelationshipType(r.type),
            confidence: this.normalizeConfidence(r.confidence),
            properties: r.properties || {},
          }))
        : [];

      return { entities, relationships };
    } catch (error) {
      console.error("Failed to parse extraction result:", error);
      console.error("Content:", content);
      throw new Error("Invalid JSON response from OpenAI");
    }
  }

  private normalizeEntityType(type: string): EntityType {
    const normalized = type.trim();
    const validTypes: EntityType[] = [
      "Person",
      "Organization",
      "Location",
      "Concept",
      "Event",
      "Technology",
    ];

    if (validTypes.includes(normalized as EntityType)) {
      return normalized as EntityType;
    }

    // Fallback mapping
    const lower = normalized.toLowerCase();
    if (lower.includes("person") || lower.includes("people")) return "Person";
    if (lower.includes("org") || lower.includes("company")) return "Organization";
    if (lower.includes("location") || lower.includes("place")) return "Location";
    if (lower.includes("event")) return "Event";
    if (lower.includes("tech")) return "Technology";
    return "Concept";
  }

  private normalizeRelationshipType(type: string | undefined | null): RelationshipType {
    if (!type) {
      return "RELATED_TO"; // Default fallback
    }
    
    const normalized = String(type).trim().toUpperCase();
    const validTypes: RelationshipType[] = [
      "RELATED_TO",
      "PART_OF",
      "WORKS_AT",
      "LOCATED_IN",
      "MENTIONS",
      "CREATED_BY",
    ];

    if (validTypes.includes(normalized as RelationshipType)) {
      return normalized as RelationshipType;
    }

    return "RELATED_TO"; // Default fallback
  }

  private normalizeConfidence(confidence: any): number {
    const conf = typeof confidence === "number" ? confidence : parseFloat(String(confidence || "0.5"));
    return Math.max(0.0, Math.min(1.0, conf || 0.5));
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

export const azureOpenAI = new AzureOpenAIService();

