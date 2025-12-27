"use client";

import { useState, useEffect } from "react";
import { X, Plus, Trash2 } from "lucide-react";
import { ENTITY_TYPES } from "@/lib/schema";
import type { Entity, EntityType } from "@/types";

interface EntityFormProps {
  entity?: Entity; // If provided, form is in edit mode
  onSubmit: (data: Omit<Entity, "id" | "createdAt" | "updatedAt">) => Promise<void>;
  onCancel: () => void;
}

export default function EntityForm({ entity, onSubmit, onCancel }: EntityFormProps) {
  const [label, setLabel] = useState(entity?.label || "");
  const [type, setType] = useState<EntityType>(entity?.type || "Concept");
  const [properties, setProperties] = useState<Record<string, any>>(
    entity?.properties || {}
  );
  const [newPropertyKey, setNewPropertyKey] = useState("");
  const [newPropertyValue, setNewPropertyValue] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!label.trim()) {
      setError("Label is required");
      return;
    }

    setIsSubmitting(true);
    try {
      await onSubmit({
        label: label.trim(),
        type,
        properties,
        metadata: entity?.metadata,
      });
    } catch (err: any) {
      setError(err.message || "Failed to save entity");
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

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-xl font-semibold">
            {entity ? "Edit Entity" : "Create Entity"}
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

          {/* Label */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Label <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="Enter entity label"
              required
              disabled={isSubmitting}
            />
          </div>

          {/* Type */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Type <span className="text-red-500">*</span>
            </label>
            <select
              value={type}
              onChange={(e) => setType(e.target.value as EntityType)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              required
              disabled={isSubmitting}
            >
              {ENTITY_TYPES.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
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
            disabled={isSubmitting || !label.trim()}
          >
            {isSubmitting ? "Saving..." : entity ? "Update" : "Create"}
          </button>
        </div>
      </div>
    </div>
  );
}

