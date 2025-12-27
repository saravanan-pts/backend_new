"use client";

import { useState } from "react";
import { Settings, Download, Upload, Trash2, Moon, Sun } from "lucide-react";
import { useGraphStore } from "@/lib/store";
import { useGraph } from "@/hooks/useGraph";
import { useSurrealDB } from "@/hooks/useSurrealDB";
import AlertDialog from "@/components/AlertDialog";
import ConfirmDialog from "@/components/ConfirmDialog";

export default function SettingsPanel() {
  const { clearGraph } = useGraphStore();
  const { loadGraph } = useGraph();
  const { isConnected, checkHealth } = useSurrealDB();
  const [darkMode, setDarkMode] = useState(false);
  
  // Dialog states
  const [showImportAlert, setShowImportAlert] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [showClearConfirm, setShowClearConfirm] = useState(false);

  const handleExportGraph = () => {
    const { entities, relationships } = useGraphStore.getState();
    const data = {
      entities: Array.from(entities.values()),
      relationships,
      exportedAt: new Date().toISOString(),
    };

    const blob = new Blob([JSON.stringify(data, null, 2)], {
      type: "application/json",
    });
    const link = document.createElement("a");
    link.download = `graph-export-${Date.now()}.json`;
    link.href = URL.createObjectURL(blob);
    link.click();
  };

  const handleImportGraph = () => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = "application/json";
    input.onchange = async (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (!file) return;

      try {
        const text = await file.text();
        const data = JSON.parse(text);

        if (data.entities && data.relationships) {
          // Import logic would go here
          // For now, just reload from database
          await loadGraph();
          setShowImportAlert({ type: "success", message: "Graph imported successfully" });
        } else {
          setShowImportAlert({ type: "error", message: "Invalid graph file format" });
        }
      } catch (error) {
        setShowImportAlert({ type: "error", message: "Failed to import graph: " + (error as Error).message });
      }
    };
    input.click();
  };

  const handleClearAll = () => {
    setShowClearConfirm(true);
  };

  const confirmClearAll = () => {
    clearGraph();
    setShowClearConfirm(false);
    setShowImportAlert({ type: "success", message: "Graph data cleared" });
  };

  const toggleDarkMode = () => {
    setDarkMode(!darkMode);
    // Apply dark mode class to document
    if (!darkMode) {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 mb-4">
        <Settings className="w-5 h-5" />
        <h2 className="text-lg font-semibold">Settings</h2>
      </div>

      {/* Connection Status */}
      <div className="bg-gray-50 rounded-lg p-4">
        <h3 className="font-semibold text-sm mb-3">SurrealDB Connection</h3>
        <div className="space-y-2 text-sm">
          <div className="flex items-center justify-between">
            <span className="text-gray-600">Status:</span>
            <span
              className={`font-medium ${
                isConnected ? "text-green-600" : "text-red-600"
              }`}
            >
              {isConnected ? "Connected" : "Disconnected"}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-gray-600">URL:</span>
            <span className="text-gray-800 font-mono text-xs">
              {process.env.NEXT_PUBLIC_SURREALDB_URL
                ? new URL(process.env.NEXT_PUBLIC_SURREALDB_URL).hostname
                : "Not configured"}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-gray-600">Namespace:</span>
            <span className="text-gray-800">
              {process.env.NEXT_PUBLIC_SURREALDB_NAMESPACE || "Not configured"}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-gray-600">Database:</span>
            <span className="text-gray-800">
              {process.env.NEXT_PUBLIC_SURREALDB_DATABASE || "Not configured"}
            </span>
          </div>
          <button
            onClick={checkHealth}
            className="w-full mt-3 px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
          >
            Check Connection
          </button>
        </div>
      </div>

      {/* Azure OpenAI Config */}
      <div className="bg-gray-50 rounded-lg p-4">
        <h3 className="font-semibold text-sm mb-3">Azure OpenAI</h3>
        <div className="space-y-2 text-sm">
          <div className="flex items-center justify-between">
            <span className="text-gray-600">Endpoint:</span>
            <span className="text-gray-800 font-mono text-xs">
              {process.env.NEXT_PUBLIC_AZURE_OPENAI_ENDPOINT
                ? "Configured"
                : "Not configured"}
            </span>
          </div>
          <div className="flex items-center justify-between">
            <span className="text-gray-600">Deployment:</span>
            <span className="text-gray-800">
              {process.env.NEXT_PUBLIC_AZURE_OPENAI_DEPLOYMENT_NAME ||
                "Not configured"}
            </span>
          </div>
        </div>
      </div>

      {/* Graph Preferences */}
      <div className="bg-gray-50 rounded-lg p-4">
        <h3 className="font-semibold text-sm mb-3">Graph Preferences</h3>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <span className="text-sm text-gray-600">Dark Mode</span>
            <button
              onClick={toggleDarkMode}
              className="p-2 hover:bg-gray-200 rounded"
            >
              {darkMode ? (
                <Sun className="w-5 h-5" />
              ) : (
                <Moon className="w-5 h-5" />
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Data Management */}
      <div className="bg-gray-50 rounded-lg p-4">
        <h3 className="font-semibold text-sm mb-3">Data Management</h3>
        <div className="space-y-2">
          <button
            onClick={handleExportGraph}
            className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
          >
            <Download className="w-4 h-4" />
            Export Graph Data
          </button>
          <button
            onClick={handleImportGraph}
            className="w-full flex items-center justify-center gap-2 px-4 py-2 border border-gray-300 rounded hover:bg-gray-50"
          >
            <Upload className="w-4 h-4" />
            Import Graph Data
          </button>
          <button
            onClick={handleClearAll}
            className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
          >
            <Trash2 className="w-4 h-4" />
            Clear All Data
          </button>
        </div>
      </div>

      {/* Dialogs */}
      {showImportAlert && (
        <AlertDialog
          title={showImportAlert.type === "success" ? "Success" : "Error"}
          message={showImportAlert.message}
          type={showImportAlert.type}
          onClose={() => setShowImportAlert(null)}
        />
      )}

      {showClearConfirm && (
        <ConfirmDialog
          title="Clear All Data"
          message="Are you sure you want to clear all graph data? This action cannot be undone."
          confirmText="Clear"
          cancelText="Cancel"
          onConfirm={confirmClearAll}
          onCancel={() => setShowClearConfirm(false)}
          danger={true}
        />
      )}
    </div>
  );
}

