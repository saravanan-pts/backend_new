"use client";

import { useState, useEffect, useCallback } from "react";
import { RefreshCw, Database, FileText } from "lucide-react";
import type { Document } from "@/types";

interface GraphSelectorProps {
  selectedDocumentId: string | null;
  onSelectDocument: (documentId: string | null) => void;
  onRefresh: () => void;
}

export default function GraphSelector({
  selectedDocumentId,
  onSelectDocument,
  onRefresh,
}: GraphSelectorProps) {
  const [documents, setDocuments] = useState<Document[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [showDocuments, setShowDocuments] = useState(false);

  /**
   * Load documents via API (server decides Surreal vs Cosmos)
   */
  const loadDocuments = useCallback(async () => {
    setIsLoading(true);
    try {
      const res = await fetch("/api/documents");

      if (!res.ok) {
        throw new Error(`Failed to load documents (${res.status})`);
      }

      const docs: Document[] = await res.json();

      setDocuments(
        docs.sort(
          (a, b) =>
            new Date(b.uploadedAt).getTime() -
            new Date(a.uploadedAt).getTime()
        )
      );
    } catch (error) {
      console.error("Error loading documents:", error);
      setDocuments([]);
    } finally {
      setIsLoading(false);
    }
  }, []);

  /**
   * Initial load
   */
  useEffect(() => {
    loadDocuments();
  }, [loadDocuments]);

  const handleSelectAll = () => {
    onSelectDocument(null);
    setShowDocuments(false);
  };

  const handleSelectDocument = (docId: string) => {
    onSelectDocument(docId);
    setShowDocuments(false);
  };

  return (
    <div className="relative">
      <div className="flex items-center gap-2">
        <button
          onClick={onRefresh}
          className="p-2 hover:bg-gray-100 rounded"
          title="Refresh graph"
          disabled={isLoading}
        >
          <RefreshCw className={`w-4 h-4 ${isLoading ? "animate-spin" : ""}`} />
        </button>

        <button
          onClick={() => setShowDocuments((v) => !v)}
          className="flex items-center gap-2 px-3 py-2 bg-white border border-gray-300 rounded hover:bg-gray-50 text-sm"
        >
          <Database className="w-4 h-4" />
          <span>
            {selectedDocumentId
              ? documents.find((d) => d.id === selectedDocumentId)?.filename ??
                "Selected"
              : "All Graphs"}
          </span>
        </button>
      </div>

      {showDocuments && (
        <>
          <div
            className="fixed inset-0 z-40"
            onClick={() => setShowDocuments(false)}
          />

          <div className="absolute right-0 mt-2 w-64 bg-white border border-gray-200 rounded-lg shadow-lg z-50 max-h-96 overflow-y-auto">
            <div className="p-2 border-b border-gray-200">
              <h3 className="text-sm font-semibold text-gray-700">
                Select Graph
              </h3>
            </div>

            <div className="p-2">
              <button
                onClick={handleSelectAll}
                className={`w-full text-left px-3 py-2 rounded text-sm hover:bg-gray-100 ${
                  selectedDocumentId === null
                    ? "bg-blue-50 text-blue-600"
                    : "text-gray-700"
                }`}
              >
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4" />
                  <div>
                    <div className="font-medium">All Graphs</div>
                    <div className="text-xs text-gray-500">
                      Show all entities and relationships
                    </div>
                  </div>
                </div>
              </button>
            </div>

            {documents.length === 0 ? (
              <div className="p-4 text-center">
                <p className="text-sm text-gray-500 mb-2">
                  No documents found
                </p>
              </div>
            ) : (
              <div className="divide-y divide-gray-200">
                {documents.map((doc) => (
                  <button
                    key={doc.id}
                    onClick={() => handleSelectDocument(doc.id)}
                    className={`w-full text-left px-3 py-2 hover:bg-gray-100 ${
                      selectedDocumentId === doc.id ? "bg-blue-50" : ""
                    }`}
                  >
                    <div className="flex items-start gap-2">
                      <FileText className="w-4 h-4 mt-0.5 text-gray-400" />
                      <div className="flex-1 min-w-0">
                        <div
                          className={`text-sm font-medium truncate ${
                            selectedDocumentId === doc.id
                              ? "text-blue-600"
                              : "text-gray-700"
                          }`}
                        >
                          {doc.filename}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">
                          {doc.entityCount} entities,{" "}
                          {doc.relationshipCount} relationships
                        </div>
                        <div className="text-xs text-gray-400 mt-1">
                          {new Date(doc.uploadedAt).toLocaleDateString()}
                        </div>
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            )}

            <div className="p-2 border-t border-gray-200">
              <button
                onClick={loadDocuments}
                className="w-full text-left px-3 py-2 text-sm text-blue-600 hover:bg-blue-50 rounded"
                disabled={isLoading}
              >
                <div className="flex items-center gap-2">
                  <RefreshCw
                    className={`w-4 h-4 ${
                      isLoading ? "animate-spin" : ""
                    }`}
                  />
                  Refresh List
                </div>
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
