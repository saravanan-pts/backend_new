import { useState, useCallback } from "react";
import { azureOpenAI } from "@/services/azure-openai";
import type { EntityExtractionResult } from "@/types";

export function useEntityExtraction() {
  const [isExtracting, setIsExtracting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const extractEntities = useCallback(
    async (text: string): Promise<EntityExtractionResult> => {
      setIsExtracting(true);
      setError(null);

      try {
        const result = await azureOpenAI.extractEntitiesAndRelationships(text);
        return result;
      } catch (err: any) {
        const errorMessage = err.message || "Failed to extract entities";
        setError(errorMessage);
        throw err;
      } finally {
        setIsExtracting(false);
      }
    },
    []
  );

  return {
    isExtracting,
    error,
    extractEntities,
  };
}

