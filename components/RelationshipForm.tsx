"use client";

import { useState, useEffect } from "react";
import { X, Plus, Trash2 } from "lucide-react";
import { RELATIONSHIP_TYPES } from "@/lib/schema";
import type { Relationship, RelationshipType, Entity } from "@/types";

interface RelationshipFormProps {
  fromEntityId?: string; // Pre-fill from entity
  toEntityId?: string; // Pre-fill to entity
  relationship?: Relationship; // If provided, form is in edit mode
  entities: Entity[]; // List of entities for dropdowns
  onSubmit: (
    from: string,
    to: string,
    type: RelationshipType,
    properties?: Record<string, any>,
    confidence?: number
  ) => Promise<void>;
  onCancel: () => void;
}

export default function RelationshipForm({
  fromEntityId,
  toEntityId,
  relationship,
  entities,
  onSubmit,
  onCancel,
}: RelationshipFormProps) {
  const [from, setFrom] = useState(
    relationship?.from || fromEntityId || ""
  );
  const [to, setTo] = useState(relationship?.to || toEntityId || "");
  const [type, setType] = useState<RelationshipType>(
    relationship?.type || "RELATED_TO"
  );
  const [properties, setProperties] = useState<Record<string, any>>(
    relationship?.properties || {}
  );
  const [confidence, setConfidence] = useState<number>(
    relationship?.confidence ?? 1.0
  );
  const [newPropertyKey, setNewPropertyKey] = useState("");
  const [newPropertyValue, setNewPropertyValue] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Filter out the selected "from" entity from "to" dropdown
  const availableToEntities = entities.filter((e) => e.id !== from);
  // Filter out the selected "to" entity from "from" dropdown
  const availableFromEntities = entities.filter((e) => e.id !== to);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!from || !to) {
      setError("Both source and target entities are required");
      return;
    }

    if (from === to) {
      setError("Source and target entities must be different");
      return;
    }

    setIsSubmitting(true);
    try {
      await onSubmit(from, to, type, properties, confidence);
    } catch (err: any) {
      setError(err.message || "Failed to save relationship");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleAddProperty = () => {
    if (!newPropertyKey.trim()) return;

    setProperties((prev) => ({
      ...prev,
      [newPropertyKey.trim()]: newPropertyValue,
    }));
    setNewPropertyKey("");
    setNewPropertyValue("");
  };

  const handleRemoveProperty = (key: string) => {
    setProperties((prev) => {
      const newProps = { ...prev };
      delete newProps[key];
      return newProps;
    });
  };

  const handlePropertyChange = (key: string, value: any) => {
    setProperties((prev) => ({
      ...prev,
      [key]: value,
    }));
  };

  const getEntityLabel = (id: string) => {
    const entity = entities.find((e) => e.id === id);
    return entity ? `${entity.label} (${entity.type})` : id;
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-xl font-semibold">
            {relationship ? "Edit Relationship" : "Create Relationship"}
          </h2>
          <button
            onClick={onCancel}
            className="p-1 hover:bg-gray-100 rounded"
            disabled={isSubmitting}
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="flex-1 overflow-y-auto p-6 space-y-4">
          {error && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
              {error}
            </div>
          )}

          {/* From Entity */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              From Entity <span className="text-red-500">*</span>
            </label>
            <select
              value={from}
              onChange={(e) => setFrom(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
              disabled={isSubmitting || !!relationship}
            >
              <option value="">Select source entity</option>
              {availableFromEntities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.label} ({entity.type})
                </option>
              ))}
            </select>
            {from && (
              <p className="mt-1 text-xs text-gray-500">
                {getEntityLabel(from)}
              </p>
            )}
          </div>

          {/* To Entity */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              To Entity <span className="text-red-500">*</span>
            </label>
            <select
              value={to}
              onChange={(e) => setTo(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
              disabled={isSubmitting || !!relationship}
            >
              <option value="">Select target entity</option>
              {availableToEntities.map((entity) => (
                <option key={entity.id} value={entity.id}>
                  {entity.label} ({entity.type})
                </option>
              ))}
            </select>
            {to && (
              <p className="mt-1 text-xs text-gray-500">{getEntityLabel(to)}</p>
            )}
          </div>

          {/* Type */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Relationship Type <span className="text-red-500">*</span>
            </label>
            <select
              value={type}
              onChange={(e) => setType(e.target.value as RelationshipType)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
              disabled={isSubmitting}
            >
              {RELATIONSHIP_TYPES.map((t) => (
                <option key={t} value={t}>
                  {t.replace(/_/g, " ")}
                </option>
              ))}
            </select>
          </div>

          {/* Confidence */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Confidence: {Math.round(confidence * 100)}%
            </label>
            <input
              type="range"
              min="0"
              max="1"
              step="0.01"
              value={confidence}
              onChange={(e) => setConfidence(parseFloat(e.target.value))}
              className="w-full"
              disabled={isSubmitting}
            />
          </div>

          {/* Properties */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Properties
            </label>

            {/* Existing Properties */}
            <div className="space-y-2 mb-3">
              {Object.entries(properties).map(([key, value]) => (
                <div key={key} className="flex items-center gap-2">
                  <input
                    type="text"
                    value={key}
                    readOnly
                    className="flex-1 px-2 py-1 border border-gray-300 rounded bg-gray-50 text-sm"
                  />
                  <input
                    type="text"
                    value={String(value)}
                    onChange={(e) => handlePropertyChange(key, e.target.value)}
                    className="flex-1 px-2 py-1 border border-gray-300 rounded text-sm"
                    disabled={isSubmitting}
                  />
                  <button
                    type="button"
                    onClick={() => handleRemoveProperty(key)}
                    className="p-1 hover:bg-red-100 rounded text-red-600"
                    disabled={isSubmitting}
                  >
                    <Trash2 className="w-4 h-4" />
                  </button>
                </div>
              ))}
            </div>

            {/* Add New Property */}
            <div className="flex items-center gap-2">
              <input
                type="text"
                value={newPropertyKey}
                onChange={(e) => setNewPropertyKey(e.target.value)}
                placeholder="Key"
                className="flex-1 px-2 py-1 border border-gray-300 rounded text-sm"
                disabled={isSubmitting}
              />
              <input
                type="text"
                value={newPropertyValue}
                onChange={(e) => setNewPropertyValue(e.target.value)}
                placeholder="Value"
                className="flex-1 px-2 py-1 border border-gray-300 rounded text-sm"
                disabled={isSubmitting}
              />
              <button
                type="button"
                onClick={handleAddProperty}
                className="p-1 hover:bg-blue-100 rounded text-blue-600"
                disabled={isSubmitting || !newPropertyKey.trim()}
              >
                <Plus className="w-4 h-4" />
              </button>
            </div>
          </div>
        </form>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-200 flex items-center justify-end gap-3">
          <button
            type="button"
            onClick={onCancel}
            className="px-4 py-2 text-gray-700 bg-gray-100 rounded hover:bg-gray-200"
            disabled={isSubmitting}
          >
            Cancel
          </button>
          <button
            type="submit"
            onClick={handleSubmit}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            disabled={isSubmitting || !from || !to || from === to}
          >
            {isSubmitting ? "Saving..." : relationship ? "Update" : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}

