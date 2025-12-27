"use client";

import { useState, useEffect, memo } from "react";
import { X, Edit2, Trash2, Save, Plus, Link2 } from "lucide-react";
import { useGraphStore } from "@/lib/store";
import { useGraph } from "@/hooks/useGraph";
import { ENTITY_TYPES } from "@/lib/schema";
import type { Entity, Relationship, EntityType } from "@/types";
import ConfirmDialog from "@/components/ConfirmDialog";
import InputDialog from "@/components/InputDialog";

interface NodeDetailPanelProps {
  onClose: () => void;
  onCreateRelationship?: (fromEntityId: string) => void;
  onEditRelationship?: (relationship: Relationship) => void;
  onDeleteRelationship?: (relationshipId: string) => void;
}

function NodeDetailPanel({ 
  onClose, 
  onCreateRelationship,
  onEditRelationship,
  onDeleteRelationship,
}: NodeDetailPanelProps) {
  const { selectedEntity, relationships, entities } = useGraphStore();
  const { updateEntity, deleteEntity, updateRelationship, deleteRelationship } = useGraph();
  const [isEditing, setIsEditing] = useState(false);
  const [isEditingLabel, setIsEditingLabel] = useState(false);
  const [editedLabel, setEditedLabel] = useState("");
  const [editedType, setEditedType] = useState<EntityType>("Concept");
  const [editedProperties, setEditedProperties] = useState<Record<string, any>>({});
  
  // Dialog states
  const [showDeleteRelationshipConfirm, setShowDeleteRelationshipConfirm] = useState(false);
  const [relationshipToDelete, setRelationshipToDelete] = useState<string | null>(null);
  const [showDeleteEntityConfirm, setShowDeleteEntityConfirm] = useState(false);
  const [showAddPropertyDialog, setShowAddPropertyDialog] = useState(false);

  useEffect(() => {
    if (selectedEntity) {
      setEditedLabel(selectedEntity.label);
      setEditedType(selectedEntity.type);
      setEditedProperties(selectedEntity.properties || {});
      setIsEditing(false);
      setIsEditingLabel(false);
    }
  }, [selectedEntity]);

  if (!selectedEntity) {
    return (
      <div className="w-full h-full bg-gray-50 flex items-center justify-center text-gray-500">
        <p>Select a node to view details</p>
      </div>
    );
  }

  const connectedRelationships = relationships.filter(
    (rel) => rel.from === selectedEntity.id || rel.to === selectedEntity.id
  );

  const handleSave = async () => {
    try {
      await updateEntity(selectedEntity.id, {
        label: editedLabel,
        type: editedType,
        properties: editedProperties,
      });
      setIsEditing(false);
      setIsEditingLabel(false);
    } catch (error) {
      console.error("Error updating entity:", error);
    }
  };

  const handleSaveLabel = async () => {
    try {
      await updateEntity(selectedEntity.id, {
        label: editedLabel,
        type: editedType,
      });
      setIsEditingLabel(false);
    } catch (error) {
      console.error("Error updating entity:", error);
    }
  };

  const handleRelationshipDelete = async (relId: string) => {
    setRelationshipToDelete(relId);
    setShowDeleteRelationshipConfirm(true);
  };

  const confirmDeleteRelationship = async () => {
    if (relationshipToDelete) {
      try {
        await deleteRelationship(relationshipToDelete);
        if (onDeleteRelationship) {
          onDeleteRelationship(relationshipToDelete);
        }
      } catch (error) {
        console.error("Error deleting relationship:", error);
      }
      setRelationshipToDelete(null);
    }
    setShowDeleteRelationshipConfirm(false);
  };

  const getEntityLabel = (id: string) => {
    const entity = Array.from(entities.values()).find((e) => e.id === id);
    return entity ? entity.label : id;
  };

  const handleDelete = () => {
    setShowDeleteEntityConfirm(true);
  };

  const confirmDeleteEntity = async () => {
    try {
      await deleteEntity(selectedEntity.id);
      onClose();
    } catch (error) {
      console.error("Error deleting entity:", error);
    }
    setShowDeleteEntityConfirm(false);
  };

  const handlePropertyChange = (key: string, value: any) => {
    setEditedProperties((prev) => ({
      ...prev,
      [key]: value,
    }));
  };

  const handleAddProperty = () => {
    setShowAddPropertyDialog(true);
  };

  const confirmAddProperty = (key: string) => {
    if (key.trim()) {
      setEditedProperties((prev) => ({
        ...prev,
        [key.trim()]: "",
      }));
    }
    setShowAddPropertyDialog(false);
  };

  return (
    <div className="w-full h-full bg-white border-l border-gray-200 flex flex-col">
      {/* Header */}
      <div className="p-4 border-b border-gray-200 flex items-center justify-between">
        <div className="flex-1">
          {isEditingLabel ? (
            <div className="space-y-2">
              <input
                type="text"
                value={editedLabel}
                onChange={(e) => setEditedLabel(e.target.value)}
                className="w-full px-2 py-1 border border-gray-300 rounded text-lg font-semibold"
                autoFocus
              />
              <select
                value={editedType}
                onChange={(e) => setEditedType(e.target.value as EntityType)}
                className="w-full px-2 py-1 border border-gray-300 rounded text-sm"
              >
                {ENTITY_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
              <div className="flex gap-2">
                <button
                  onClick={handleSaveLabel}
                  className="px-3 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
                >
                  Save
                </button>
                <button
                  onClick={() => {
                    setIsEditingLabel(false);
                    setEditedLabel(selectedEntity.label);
                    setEditedType(selectedEntity.type);
                  }}
                  className="px-3 py-1 bg-gray-100 text-gray-700 rounded text-sm hover:bg-gray-200"
                >
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            <div>
              <div className="flex items-center gap-2">
                <h2 className="text-lg font-semibold">{selectedEntity.label}</h2>
                <button
                  onClick={() => setIsEditingLabel(true)}
                  className="p-1 hover:bg-gray-100 rounded"
                  title="Edit label and type"
                >
                  <Edit2 className="w-4 h-4" />
                </button>
              </div>
              <p className="text-sm text-gray-500">{selectedEntity.type}</p>
            </div>
          )}
        </div>
        <button
          onClick={onClose}
          className="p-1 hover:bg-gray-100 rounded ml-2"
        >
          <X className="w-5 h-5" />
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 space-y-6">
        {/* Properties */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold">Properties</h3>
            {!isEditing ? (
              <button
                onClick={() => setIsEditing(true)}
                className="p-1 hover:bg-gray-100 rounded"
              >
                <Edit2 className="w-4 h-4" />
              </button>
            ) : (
              <div className="flex gap-2">
                <button
                  onClick={handleSave}
                  className="p-1 hover:bg-green-100 rounded text-green-600"
                >
                  <Save className="w-4 h-4" />
                </button>
                <button
                  onClick={() => {
                    setIsEditing(false);
                    setEditedProperties(selectedEntity.properties || {});
                  }}
                  className="p-1 hover:bg-gray-100 rounded"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
            )}
          </div>

          <div className="space-y-2">
            {Object.entries(editedProperties).map(([key, value]) => (
              <div key={key} className="flex items-start gap-2">
                <label className="text-sm font-medium text-gray-700 w-24 flex-shrink-0">
                  {key}:
                </label>
                {isEditing ? (
                  <input
                    type="text"
                    value={String(value)}
                    onChange={(e) => handlePropertyChange(key, e.target.value)}
                    className="flex-1 px-2 py-1 border border-gray-300 rounded text-sm"
                  />
                ) : (
                  <span className="text-sm text-gray-600 flex-1">
                    {String(value)}
                  </span>
                )}
              </div>
            ))}

            {isEditing && (
              <button
                onClick={handleAddProperty}
                className="flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 mt-2"
              >
                <Plus className="w-4 h-4" />
                Add Property
              </button>
            )}
          </div>
        </div>

        {/* Relationships */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold">Relationships ({connectedRelationships.length})</h3>
            {onCreateRelationship && (
              <button
                onClick={() => onCreateRelationship(selectedEntity.id)}
                className="flex items-center gap-1 px-2 py-1 text-sm bg-blue-600 text-white rounded hover:bg-blue-700"
                title="Create new relationship"
              >
                <Link2 className="w-3 h-3" />
                Add
              </button>
            )}
          </div>
          <div className="space-y-2">
            {connectedRelationships.length === 0 ? (
              <p className="text-sm text-gray-500">No relationships</p>
            ) : (
              connectedRelationships.map((rel) => (
                <div
                  key={rel.id}
                  className="p-2 bg-gray-50 rounded text-sm"
                >
                  <div className="flex items-center justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{rel.type}</span>
                        <span className="text-gray-500 text-xs">
                          {rel.confidence ? `${Math.round(rel.confidence * 100)}%` : ""}
                        </span>
                      </div>
                      <p className="text-xs text-gray-600 mt-1">
                        {rel.from === selectedEntity.id ? "→" : "←"} {getEntityLabel(rel.from === selectedEntity.id ? rel.to : rel.from)}
                      </p>
                    </div>
                    <div className="flex gap-1">
                      {onEditRelationship && (
                        <button
                          onClick={() => onEditRelationship(rel)}
                          className="p-1 hover:bg-blue-100 rounded text-blue-600"
                          title="Edit relationship"
                        >
                          <Edit2 className="w-3 h-3" />
                        </button>
                      )}
                      <button
                        onClick={() => handleRelationshipDelete(rel.id)}
                        className="p-1 hover:bg-red-100 rounded text-red-600"
                        title="Delete relationship"
                      >
                        <Trash2 className="w-3 h-3" />
                      </button>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Metadata */}
        <div>
          <h3 className="font-semibold mb-3">Metadata</h3>
          <div className="space-y-1 text-sm text-gray-600">
            {selectedEntity.metadata?.source && (
              <p>Source: {selectedEntity.metadata.source}</p>
            )}
            {selectedEntity.metadata?.confidence && (
              <p>Confidence: {Math.round(selectedEntity.metadata.confidence * 100)}%</p>
            )}
            {selectedEntity.createdAt && (
              <p>Created: {new Date(selectedEntity.createdAt).toLocaleDateString()}</p>
            )}
            {selectedEntity.updatedAt && (
              <p>Updated: {new Date(selectedEntity.updatedAt).toLocaleDateString()}</p>
            )}
          </div>
        </div>
      </div>

      {/* Footer Actions */}
      <div className="p-4 border-t border-gray-200">
        <button
          onClick={handleDelete}
          className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
        >
          <Trash2 className="w-4 h-4" />
          Delete Entity
        </button>
      </div>

      {/* Confirmation Dialogs */}
      {showDeleteRelationshipConfirm && (
        <ConfirmDialog
          title="Delete Relationship"
          message="Are you sure you want to delete this relationship?"
          confirmText="Delete"
          cancelText="Cancel"
          onConfirm={confirmDeleteRelationship}
          onCancel={() => {
            setShowDeleteRelationshipConfirm(false);
            setRelationshipToDelete(null);
          }}
          danger={true}
        />
      )}

      {showDeleteEntityConfirm && (
        <ConfirmDialog
          title="Delete Entity"
          message="Are you sure you want to delete this entity? All connected relationships will also be deleted."
          confirmText="Delete"
          cancelText="Cancel"
          onConfirm={confirmDeleteEntity}
          onCancel={() => setShowDeleteEntityConfirm(false)}
          danger={true}
        />
      )}

      {showAddPropertyDialog && (
        <InputDialog
          title="Add Property"
          message="Enter a property key:"
          placeholder="Property key"
          onSubmit={confirmAddProperty}
          onCancel={() => setShowAddPropertyDialog(false)}
          submitText="Add"
        />
      )}
    </div>
  );
}

export default memo(NodeDetailPanel);

