"use client";

import { useEffect, useRef, useState } from "react";
import { Toaster, toast } from "react-hot-toast";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import GraphVisualization, {
  type GraphVisualizationRef,
} from "@/components/GraphVisualization";
import GraphControls from "@/components/GraphControls";
import GraphSelector from "@/components/GraphSelector";
import NodeDetailPanel from "@/components/NodeDetailPanel";
import FileUpload from "@/components/FileUpload";
import TextInput from "@/components/TextInput";
import SettingsPanel from "@/components/SettingsPanel";
import EntityForm from "@/components/EntityForm";
import RelationshipForm from "@/components/RelationshipForm";
import ContextMenu, { type ContextMenuTarget } from "@/components/ContextMenu";
import { useGraphStore } from "@/lib/store";
import { useGraph } from "@/hooks/useGraph";
import { useSurrealDB } from "@/hooks/useSurrealDB";
import { Upload, FileText, Info, Settings } from "lucide-react";
import type { Entity, Relationship } from "@/types";

export default function Home() {
  const graphRef = useRef<GraphVisualizationRef>(null);
  const { activeTab, setActiveTab, selectedEntity, setSelectedEntity, entities: storeEntities } =
    useGraphStore();
  const { entities, relationships, selectedRelationship, loadGraph, createEntity, updateEntity, deleteEntity, createRelationship, updateRelationship, deleteRelationship, getRelationship, selectRelationship } = useGraph();
  const { isConnected } = useSurrealDB();
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const [selectedDocumentId, setSelectedDocumentId] = useState<string | null>(null);
  
  // Modal states
  const [showEntityForm, setShowEntityForm] = useState(false);
  const [editingEntity, setEditingEntity] = useState<Entity | null>(null);
  const [showRelationshipForm, setShowRelationshipForm] = useState(false);
  const [editingRelationship, setEditingRelationship] = useState<Relationship | null>(null);
  const [relationshipFromId, setRelationshipFromId] = useState<string | undefined>();
  const [relationshipToId, setRelationshipToId] = useState<string | undefined>();
  
  // Context menu state
  const [contextMenu, setContextMenu] = useState<{
    x: number;
    y: number;
    target: ContextMenuTarget;
    nodeId?: string;
    edgeId?: string;
  } | null>(null);

  // Load graph data on mount - wait for connection first
  useEffect(() => {
    if (!isConnected) {
      // Wait for connection before loading
      return;
    }

    const loadGraphData = async () => {
      try {
        await loadGraph(selectedDocumentId);
        setIsInitialLoad(false);
      } catch (error: any) {
        // Handle all errors gracefully - app should still work
        if (error?.message?.includes("not connected")) {
          console.warn("Graph loading skipped - waiting for connection");
        } else if (error?.message?.includes("permissions") || error?.message?.includes("IAM error")) {
          console.warn("Permission error - app will work with limited functionality");
        } else {
          console.error("Failed to load graph:", error);
        }
        setIsInitialLoad(false);
      }
    };

    if (isInitialLoad) {
      loadGraphData();
    }
  }, [loadGraph, isConnected, isInitialLoad]);

  // Reload graph when document selection changes
  useEffect(() => {
    if (isConnected && !isInitialLoad) {
      loadGraph(selectedDocumentId).catch((error) => {
        console.error("Failed to reload graph:", error);
      });
    }
  }, [selectedDocumentId]); // Only depend on selectedDocumentId

  // Update graph visualization when data changes
  useEffect(() => {
    if (graphRef.current && entities.length > 0 || relationships.length > 0) {
      // Small delay to ensure Cytoscape is initialized
      const timeoutId = setTimeout(() => {
        if (graphRef.current) {
          graphRef.current.loadGraphData(entities, relationships);
        }
      }, 200);
      
      return () => clearTimeout(timeoutId);
    } else if (graphRef.current) {
      // Even if empty, try to load (for empty state)
      graphRef.current.loadGraphData(entities, relationships);
    }
  }, [entities, relationships]);

  // Handle entity form submission
  const handleEntitySubmit = async (data: Omit<Entity, "id" | "createdAt" | "updatedAt">) => {
    try {
      if (editingEntity) {
        const updated = await updateEntity(editingEntity.id, data);
        graphRef.current?.updateNode(updated);
        toast.success("Entity updated successfully");
      } else {
        const created = await createEntity(data);
        graphRef.current?.addNode(created);
        toast.success("Entity created successfully");
      }
      setShowEntityForm(false);
      setEditingEntity(null);
    } catch (error: any) {
      toast.error(error.message || "Failed to save entity");
      throw error;
    }
  };

  // Handle relationship form submission
  const handleRelationshipSubmit = async (
    from: string,
    to: string,
    type: Relationship["type"],
    properties?: Record<string, any>,
    confidence?: number
  ) => {
    try {
      if (editingRelationship) {
        const updated = await updateRelationship(editingRelationship.id, {
          from,
          to,
          type,
          properties,
          confidence,
        });
        graphRef.current?.updateEdge(updated);
        toast.success("Relationship updated successfully");
      } else {
        const created = await createRelationship(from, to, type, properties, confidence);
        graphRef.current?.addEdge(created);
        toast.success("Relationship created successfully");
      }
      setShowRelationshipForm(false);
      setEditingRelationship(null);
      setRelationshipFromId(undefined);
      setRelationshipToId(undefined);
      selectRelationship(null);
    } catch (error: any) {
      toast.error(error.message || "Failed to save relationship");
      throw error;
    }
  };

  // Context menu handlers
  const handleContextMenu = (x: number, y: number, target: ContextMenuTarget, nodeId?: string, edgeId?: string) => {
    setContextMenu({ x, y, target, nodeId, edgeId });
  };

  const handleCreateNode = () => {
    setEditingEntity(null);
    setShowEntityForm(true);
  };

  const handleEditNode = (nodeId?: string) => {
    const entityId = nodeId || contextMenu?.nodeId;
    if (entityId) {
      const entity = entities.find((e) => e.id === entityId);
      if (entity) {
        setEditingEntity(entity);
        setShowEntityForm(true);
      }
    }
  };

  const handleDeleteNode = async (nodeId?: string) => {
    const entityId = nodeId || contextMenu?.nodeId;
    if (entityId) {
      try {
        await deleteEntity(entityId);
        graphRef.current?.removeNode(entityId);
        toast.success("Entity deleted successfully");
        if (selectedEntity?.id === entityId) {
          setSelectedEntity(null);
        }
      } catch (error: any) {
        toast.error(error.message || "Failed to delete entity");
      }
    }
  };

  const handleCreateRelationship = (fromEntityId?: string) => {
    setEditingRelationship(null);
    setRelationshipFromId(fromEntityId || contextMenu?.nodeId);
    setRelationshipToId(undefined);
    setShowRelationshipForm(true);
  };

  const handleEditEdge = (relationshipOrId?: Relationship | string) => {
    let relationship: Relationship | undefined;
    
    if (typeof relationshipOrId === 'string') {
      relationship = relationships.find((r) => r.id === relationshipOrId);
    } else if (relationshipOrId) {
      relationship = relationshipOrId;
    } else {
      const relId = contextMenu?.edgeId;
      if (relId) {
        relationship = relationships.find((r) => r.id === relId);
      }
    }
    
    if (relationship) {
      setEditingRelationship(relationship);
      setRelationshipFromId(relationship.from);
      setRelationshipToId(relationship.to);
      setShowRelationshipForm(true);
    }
  };

  const handleDeleteEdge = async (edgeId?: string) => {
    const relId = edgeId || contextMenu?.edgeId;
    if (relId) {
      try {
        await deleteRelationship(relId);
        graphRef.current?.removeEdge(relId);
        toast.success("Relationship deleted successfully");
      } catch (error: any) {
        toast.error(error.message || "Failed to delete relationship");
      }
    }
  };

  // Handle node selection
  const handleNodeSelect = (entityId: string) => {
    const entity = entities.find((e) => e.id === entityId);
    if (entity) {
      setSelectedEntity(entity);
      selectRelationship(null); // Deselect relationship when node is selected
      setActiveTab("details");
      graphRef.current?.highlightNode(entityId);
    }
  };

  const handleNodeDeselect = () => {
    setSelectedEntity(null);
  };

  // Handle edge selection
  const handleEdgeSelect = async (edgeId: string) => {
    try {
      // Try to get relationship from store first
      let relationship: Relationship | null | undefined = relationships.find((r) => r.id === edgeId);
      
      // If not found, fetch from database
      if (!relationship) {
        relationship = await getRelationship(edgeId);
      }
      
      if (relationship) {
        selectRelationship(relationship);
        setEditingRelationship(relationship);
        setRelationshipFromId(relationship.from);
        setRelationshipToId(relationship.to);
        setShowRelationshipForm(true);
        setSelectedEntity(null); // Deselect entity when edge is selected
        graphRef.current?.highlightEdge(edgeId);
      }
    } catch (error: any) {
      console.error("Error selecting edge:", error);
      toast.error(error.message || "Failed to select edge");
    }
  };

  const handleEdgeDeselect = () => {
    selectRelationship(null);
  };

  const tabs = [
    { id: "upload" as const, label: "Upload", icon: Upload },
    { id: "input" as const, label: "Input", icon: FileText },
    { id: "details" as const, label: "Details", icon: Info },
    { id: "settings" as const, label: "Settings", icon: Settings },
  ];

  return (
    <ErrorBoundary>
      <div className="min-h-screen bg-gray-50 flex flex-col">
        {/* Header */}
        <header className="bg-white border-b border-gray-200 shadow-sm">
          <div className="px-6 py-4 flex items-center justify-between">
            <div className="flex items-center gap-3">
              <h1 className="text-2xl font-bold text-gray-900">
                Knowledge Graph POC
              </h1>
              <div
                className={`w-3 h-3 rounded-full ${
                  isConnected ? "bg-green-500" : "bg-red-500"
                }`}
                title={isConnected ? "Connected" : "Disconnected"}
              />
            </div>
            <div className="flex items-center gap-4">
              <div className="text-sm text-gray-600">
                {entities.length} entities, {relationships.length} relationships
              </div>
              {isConnected && entities.length === 0 && relationships.length === 0 && (
                <button
                  onClick={() => {
                    loadGraph(selectedDocumentId).catch((error) => {
                      console.error("Failed to reload graph:", error);
                      toast.error("Failed to load graph. Check permissions.");
                    });
                  }}
                  className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded hover:bg-blue-200"
                  title="Reload graph data"
                >
                  Reload
                </button>
              )}
            </div>
          </div>
        </header>

        {/* Main Content */}
        <div className="flex-1 flex flex-col lg:flex-row overflow-hidden">
          {/* Graph Visualization (70%) */}
          <div className="flex-1 flex flex-col lg:w-[70%] w-full">
            <div className="bg-white border-b border-gray-200 px-4 py-2 flex items-center justify-end">
              <GraphSelector
                selectedDocumentId={selectedDocumentId}
                onSelectDocument={setSelectedDocumentId}
                onRefresh={() => {
                  loadGraph(selectedDocumentId).catch((error) => {
                    console.error("Failed to refresh graph:", error);
                  });
                }}
              />
            </div>
              <GraphControls 
                graphRef={graphRef}
                onCreateNode={() => {
                  setEditingEntity(null);
                  setShowEntityForm(true);
                }}
                onCreateRelationship={() => {
                  setEditingRelationship(null);
                  setRelationshipFromId(undefined);
                  setRelationshipToId(undefined);
                  setShowRelationshipForm(true);
                }}
              />
            <div className="flex-1 relative">
              {isInitialLoad ? (
                <div className="absolute inset-0 flex items-center justify-center">
                  <div className="text-center">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4" />
                    <p className="text-gray-600">Loading graph...</p>
                  </div>
                </div>
              ) : (
                <>
                  <GraphVisualization
                    ref={graphRef}
                    onNodeSelect={handleNodeSelect}
                    onNodeDeselect={handleNodeDeselect}
                    onEdgeSelect={handleEdgeSelect}
                    onEdgeDeselect={handleEdgeDeselect}
                    onContextMenu={handleContextMenu}
                    onCreateNode={(x, y) => {
                      // Convert canvas coordinates - for now just show form
                      handleCreateNode();
                    }}
                    onCreateRelationship={(fromId, toId) => {
                      setRelationshipFromId(fromId);
                      setRelationshipToId(toId);
                      setEditingRelationship(null);
                      setShowRelationshipForm(true);
                    }}
                  />
                  {entities.length === 0 && relationships.length === 0 && !isInitialLoad && (
                    <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
                      <div className="text-center bg-white/95 px-6 py-4 rounded-lg shadow-lg border border-gray-200 max-w-md">
                        <p className="text-gray-700 font-medium text-lg mb-2">No graph data available</p>
                        <p className="text-sm text-gray-500 mb-4">
                          {isConnected 
                            ? "The database may be empty or you may not have read permissions. Try uploading a document or creating entities manually."
                            : "Waiting for database connection..."}
                        </p>
                        {isConnected && (
                          <div className="flex gap-2 justify-center">
                            <button
                              onClick={() => {
                                loadGraph(selectedDocumentId).catch((error) => {
                                  console.error("Failed to reload:", error);
                                  toast.error("Failed to load graph data");
                                });
                              }}
                              className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 pointer-events-auto"
                            >
                              Reload Graph
                            </button>
                            <button
                              onClick={() => {
                                setActiveTab("upload");
                                toast.success("Switch to Upload tab to add data");
                              }}
                              className="px-4 py-2 bg-gray-200 text-gray-700 rounded text-sm hover:bg-gray-300 pointer-events-auto"
                            >
                              Upload Document
                            </button>
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>

          {/* Control Panel (30%) */}
          <div className="lg:w-[30%] w-full bg-white border-t lg:border-t-0 lg:border-l border-gray-200 flex flex-col">
            {/* Tabs */}
            <div className="flex border-b border-gray-200 overflow-x-auto">
              {tabs.map((tab) => {
                const Icon = tab.icon;
                return (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={`flex-1 min-w-[80px] flex items-center justify-center gap-1 lg:gap-2 px-2 lg:px-4 py-3 text-xs lg:text-sm font-medium transition-colors ${
                      activeTab === tab.id
                        ? "bg-blue-50 text-blue-600 border-b-2 border-blue-600"
                        : "text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    <Icon className="w-4 h-4" />
                    <span className="hidden sm:inline">{tab.label}</span>
                  </button>
                );
              })}
            </div>

            {/* Tab Content */}
            <div className="flex-1 overflow-y-auto p-4">
              {activeTab === "upload" && <FileUpload />}
              {activeTab === "input" && <TextInput />}
              {activeTab === "details" && (
                <NodeDetailPanel 
                  onClose={() => setSelectedEntity(null)}
                  onCreateRelationship={handleCreateRelationship}
                  onEditRelationship={handleEditEdge}
                  onDeleteRelationship={handleDeleteEdge}
                />
              )}
              {activeTab === "settings" && <SettingsPanel />}
            </div>
          </div>
        </div>

        {/* Modals */}
        {showEntityForm && (
          <EntityForm
            entity={editingEntity || undefined}
            onSubmit={handleEntitySubmit}
            onCancel={() => {
              setShowEntityForm(false);
              setEditingEntity(null);
            }}
          />
        )}

        {showRelationshipForm && (
          <RelationshipForm
            fromEntityId={relationshipFromId}
            toEntityId={relationshipToId}
            relationship={editingRelationship || undefined}
            entities={entities}
            onSubmit={handleRelationshipSubmit}
            onCancel={() => {
              setShowRelationshipForm(false);
              setEditingRelationship(null);
              setRelationshipFromId(undefined);
              setRelationshipToId(undefined);
              selectRelationship(null);
            }}
          />
        )}

        {contextMenu && (
          <ContextMenu
            x={contextMenu.x}
            y={contextMenu.y}
            target={contextMenu.target}
            onCreateNode={handleCreateNode}
            onEditNode={() => handleEditNode()}
            onDeleteNode={() => handleDeleteNode()}
            onCreateRelationship={() => handleCreateRelationship()}
            onEditEdge={() => handleEditEdge()}
            onDeleteEdge={() => handleDeleteEdge()}
            onClose={() => setContextMenu(null)}
          />
        )}

        {/* Toast Notifications */}
        <Toaster
          position="top-right"
          toastOptions={{
            duration: 4000,
            style: {
              background: "#363636",
              color: "#fff",
            },
            success: {
              duration: 3000,
              iconTheme: {
                primary: "#10b981",
                secondary: "#fff",
              },
            },
            error: {
              duration: 5000,
              iconTheme: {
                primary: "#ef4444",
                secondary: "#fff",
              },
            },
          }}
        />
      </div>
    </ErrorBoundary>
  );
}
