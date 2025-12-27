"use client";

import { useState } from "react";
import { ZoomIn, ZoomOut, Maximize2, Search, Download, X, Plus, Link2 } from "lucide-react";
import type { EntityType } from "@/types";
import { ENTITY_TYPES } from "@/lib/schema";
import { useGraphStore } from "@/lib/store";

interface GraphControlsProps {
  graphRef: React.RefObject<{
    fit: () => void;
    resetZoom: () => void;
    exportGraph: (format: "png" | "json") => void;
    filterByType: (types: string[]) => void;
  }>;
  onCreateNode?: () => void;
  onCreateRelationship?: () => void;
}

export default function GraphControls({ graphRef, onCreateNode, onCreateRelationship }: GraphControlsProps) {
  const { filterTypes, toggleFilterType, clearFilters } = useGraphStore();
  const [searchQuery, setSearchQuery] = useState("");
  const [showFilters, setShowFilters] = useState(false);

  const handleZoomIn = () => {
    // Cytoscape zoom is handled internally via pan/zoom
    // This would need to be exposed through the ref
  };

  const handleZoomOut = () => {
    // Same as above
  };

  const handleFit = () => {
    graphRef.current?.fit();
  };

  const handleResetZoom = () => {
    graphRef.current?.resetZoom();
  };

  const handleExportPNG = () => {
    graphRef.current?.exportGraph("png");
  };

  const handleExportJSON = () => {
    graphRef.current?.exportGraph("json");
  };

  const handleFilterToggle = (type: EntityType) => {
    toggleFilterType(type);
    const newFilters = filterTypes.includes(type)
      ? filterTypes.filter((t) => t !== type)
      : [...filterTypes, type];
    graphRef.current?.filterByType(newFilters);
  };

  const handleClearFilters = () => {
    clearFilters();
    graphRef.current?.filterByType([]);
  };

  return (
    <div className="bg-white border-b border-gray-200 p-2 lg:p-4 shadow-sm">
      <div className="flex flex-wrap items-center gap-2 lg:gap-4">
        {/* Add Node and Relationship Buttons */}
        {(onCreateNode || onCreateRelationship) && (
          <div className="flex items-center gap-1 lg:gap-2 border-r border-gray-200 pr-2 lg:pr-4">
            {onCreateNode && (
              <button
                onClick={onCreateNode}
                className="flex items-center gap-1 lg:gap-2 px-3 py-1.5 lg:px-4 lg:py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
                title="Add Node"
              >
                <Plus className="w-4 h-4" />
                <span className="hidden sm:inline">Add Node</span>
              </button>
            )}
            {onCreateRelationship && (
              <button
                onClick={onCreateRelationship}
                className="flex items-center gap-1 lg:gap-2 px-3 py-1.5 lg:px-4 lg:py-2 bg-green-600 text-white rounded text-sm hover:bg-green-700"
                title="Add Relationship"
              >
                <Link2 className="w-4 h-4" />
                <span className="hidden sm:inline">Add Edge</span>
              </button>
            )}
          </div>
        )}
        
        {/* Zoom Controls */}
        <div className="flex items-center gap-1 lg:gap-2 border-r border-gray-200 pr-2 lg:pr-4">
          <button
            onClick={handleZoomIn}
            className="p-2 hover:bg-gray-100 rounded"
            title="Zoom In"
          >
            <ZoomIn className="w-4 h-4" />
          </button>
          <button
            onClick={handleZoomOut}
            className="p-2 hover:bg-gray-100 rounded"
            title="Zoom Out"
          >
            <ZoomOut className="w-4 h-4" />
          </button>
          <button
            onClick={handleFit}
            className="p-2 hover:bg-gray-100 rounded"
            title="Fit to Screen"
          >
            <Maximize2 className="w-4 h-4" />
          </button>
          <button
            onClick={handleResetZoom}
            className="p-2 hover:bg-gray-100 rounded text-sm"
            title="Reset Zoom"
          >
            1:1
          </button>
        </div>

        {/* Search */}
        <div className="flex-1 min-w-[120px] lg:min-w-[200px]">
          <div className="relative">
            <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-8 pr-4 py-1 lg:py-2 text-sm border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        {/* Filters */}
        <div className="relative">
          <button
            onClick={() => setShowFilters(!showFilters)}
            className="px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded text-sm"
          >
            Filters {filterTypes.length > 0 && `(${filterTypes.length})`}
          </button>

          {showFilters && (
            <div className="absolute right-0 mt-2 w-64 bg-white border border-gray-200 rounded-lg shadow-lg z-10 p-4">
              <div className="flex justify-between items-center mb-3">
                <h3 className="font-semibold text-sm">Filter by Type</h3>
                {filterTypes.length > 0 && (
                  <button
                    onClick={handleClearFilters}
                    className="text-xs text-blue-600 hover:text-blue-800"
                  >
                    Clear
                  </button>
                )}
              </div>
              <div className="space-y-2">
                {ENTITY_TYPES.map((type) => (
                  <label
                    key={type}
                    className="flex items-center space-x-2 cursor-pointer"
                  >
                    <input
                      type="checkbox"
                      checked={filterTypes.includes(type)}
                      onChange={() => handleFilterToggle(type)}
                      className="rounded"
                    />
                    <span className="text-sm">{type}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Export */}
        <div className="flex items-center gap-1 lg:gap-2 border-l border-gray-200 pl-2 lg:pl-4">
          <button
            onClick={handleExportPNG}
            className="p-2 hover:bg-gray-100 rounded"
            title="Export as PNG"
          >
            <Download className="w-4 h-4" />
          </button>
          <button
            onClick={handleExportJSON}
            className="px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
            title="Export as JSON"
          >
            JSON
          </button>
        </div>
      </div>
    </div>
  );
}

