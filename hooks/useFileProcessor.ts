import { useState, useCallback } from "react";
import { useGraphStore } from "@/lib/store";
import type { Entity, Relationship, Document } from "@/types";

interface ProcessingResult {
  document: Document;
  entities: Entity[];
  relationships: Relationship[];
}

export function useFileProcessor() {
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const { addEntity, addRelationship } = useGraphStore();

  const processFile = useCallback(
    async (file: File): Promise<ProcessingResult> => {
      setIsProcessing(true);
      setProgress(0);
      setError(null);

      try {
        setProgress(25);

        // Use API route for file processing (server-side only)
        const formData = new FormData();
        formData.append("file", file);

        const response = await fetch("/api/process", {
          method: "POST",
          body: formData,
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.error || "Failed to process file");
        }

        setProgress(75);
        const data = await response.json();

        if (!data.success) {
          throw new Error(data.error || "Processing failed");
        }

        const result: ProcessingResult = {
          document: data.document,
          entities: data.entities,
          relationships: data.relationships,
        };

        setProgress(100);

        // Add entities and relationships to store
        result.entities.forEach((entity) => addEntity(entity));
        result.relationships.forEach((rel) => addRelationship(rel));

        return result;
      } catch (err: any) {
        const errorMessage = err.message || "Failed to process file";
        setError(errorMessage);
        throw err;
      } finally {
        setIsProcessing(false);
        setProgress(0);
      }
    },
    [addEntity, addRelationship]
  );

  const processText = useCallback(
    async (text: string, filename: string = "text.txt"): Promise<ProcessingResult> => {
      setIsProcessing(true);
      setProgress(0);
      setError(null);

      try {
        setProgress(25);

        // Use API route for text processing (server-side only)
        const formData = new FormData();
        formData.append("text", text);

        const response = await fetch("/api/process", {
          method: "POST",
          body: formData,
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.error || "Failed to process text");
        }

        setProgress(75);
        const data = await response.json();

        if (!data.success) {
          throw new Error(data.error || "Processing failed");
        }

        const result: ProcessingResult = {
          document: data.document,
          entities: data.entities,
          relationships: data.relationships,
        };

        setProgress(100);

        // Add entities and relationships to store
        result.entities.forEach((entity) => addEntity(entity));
        result.relationships.forEach((rel) => addRelationship(rel));

        return result;
      } catch (err: any) {
        const errorMessage = err.message || "Failed to process text";
        setError(errorMessage);
        throw err;
      } finally {
        setIsProcessing(false);
        setProgress(0);
      }
    },
    [addEntity, addRelationship]
  );

  return {
    isProcessing,
    progress,
    error,
    processFile,
    processText,
  };
}

