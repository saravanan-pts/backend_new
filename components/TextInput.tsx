"use client";

import { useState } from "react";
import { Send, Loader2 } from "lucide-react";
import { useFileProcessor } from "@/hooks/useFileProcessor";
import toast from "react-hot-toast";
import type { ExtractedEntity, ExtractedRelationship } from "@/types";

export default function TextInput() {
  const [text, setText] = useState("");
  const [extractedData, setExtractedData] = useState<{
    entities: ExtractedEntity[];
    relationships: ExtractedRelationship[];
  } | null>(null);
  const { processText, isProcessing } = useFileProcessor();

  const handleProcess = async () => {
    if (!text.trim()) {
      toast.error("Please enter some text");
      return;
    }

    try {
      const result = await processText(text, "manual-input.txt");
      setExtractedData({
        entities: result.entities,
        relationships: result.relationships,
      });
      toast.success(
        `Extracted ${result.entities.length} entities and ${result.relationships.length} relationships`
      );
    } catch (error: any) {
      toast.error(`Failed to process text: ${error.message}`);
    }
  };

  const handleClear = () => {
    setText("");
    setExtractedData(null);
  };

  const characterCount = text.length;
  const wordCount = text.trim().split(/\s+/).filter((w) => w).length;

  return (
    <div className="space-y-4 h-full flex flex-col">
      {/* Text Area */}
      <div className="flex-1 flex flex-col">
        <div className="flex items-center justify-between mb-2">
          <label className="text-sm font-medium text-gray-700">
            Enter text to analyze
          </label>
          <div className="text-xs text-gray-500">
            {characterCount} chars, {wordCount} words
          </div>
        </div>
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="Paste or type text here to extract entities and relationships..."
          className="flex-1 w-full px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
          disabled={isProcessing}
        />
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2">
        <button
          onClick={handleProcess}
          disabled={isProcessing || !text.trim()}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
        >
          {isProcessing ? (
            <>
              <Loader2 className="w-4 h-4 animate-spin" />
              Processing...
            </>
          ) : (
            <>
              <Send className="w-4 h-4" />
              Process Text
            </>
          )}
        </button>
        <button
          onClick={handleClear}
          disabled={isProcessing}
          className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50"
        >
          Clear
        </button>
      </div>

      {/* Extracted Data Preview */}
      {extractedData && (
        <div className="mt-4 p-4 bg-gray-50 rounded-lg space-y-4">
          <h3 className="font-semibold text-sm">Extraction Results</h3>

          {/* Entities */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">
              Entities ({extractedData.entities.length})
            </h4>
            <div className="max-h-40 overflow-y-auto space-y-1">
              {extractedData.entities.length === 0 ? (
                <p className="text-xs text-gray-500">No entities found</p>
              ) : (
                extractedData.entities.map((entity, index) => (
                  <div
                    key={index}
                    className="text-xs p-2 bg-white rounded border border-gray-200"
                  >
                    <span className="font-medium">{entity.label}</span>
                    <span className="text-gray-500 ml-2">({entity.type})</span>
                    {entity.confidence && (
                      <span className="text-gray-400 ml-2">
                        {Math.round(entity.confidence * 100)}%
                      </span>
                    )}
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Relationships */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">
              Relationships ({extractedData.relationships.length})
            </h4>
            <div className="max-h-40 overflow-y-auto space-y-1">
              {extractedData.relationships.length === 0 ? (
                <p className="text-xs text-gray-500">No relationships found</p>
              ) : (
                extractedData.relationships.map((rel, index) => (
                  <div
                    key={index}
                    className="text-xs p-2 bg-white rounded border border-gray-200"
                  >
                    <span className="font-medium">{rel.from}</span>
                    <span className="text-gray-500 mx-1">→</span>
                    <span className="font-medium">{rel.type}</span>
                    <span className="text-gray-500 mx-1">→</span>
                    <span className="font-medium">{rel.to}</span>
                    {rel.confidence && (
                      <span className="text-gray-400 ml-2">
                        {Math.round(rel.confidence * 100)}%
                      </span>
                    )}
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

