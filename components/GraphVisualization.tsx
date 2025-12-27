"use client";

import { useEffect, useRef, useImperativeHandle, forwardRef, memo, useMemo } from "react";
import cytoscape, { Core } from "cytoscape";
import cola from "cytoscape-cola";
import dagre from "cytoscape-dagre";
import type { Entity, Relationship } from "@/types";

// Register layout extensions
cytoscape.use(cola);
cytoscape.use(dagre);

export interface GraphVisualizationRef {
  loadGraphData: (entities: Entity[], relationships: Relationship[]) => void;
  addNode: (entity: Entity) => void;
  addEdge: (relationship: Relationship) => void;
  updateNode: (entity: Entity) => void;
  updateEdge: (relationship: Relationship) => void;
  removeNode: (id: string) => void;
  removeEdge: (id: string) => void;
  highlightNode: (id: string) => void;
  highlightEdge: (id: string) => void;
  filterByType: (types: string[]) => void;
  exportGraph: (format: "png" | "json") => void;
  fit: () => void;
  resetZoom: () => void;
}

interface GraphVisualizationProps {
  onNodeSelect?: (entityId: string) => void;
  onNodeDeselect?: () => void;
  onEdgeSelect?: (edgeId: string) => void;
  onEdgeDeselect?: () => void;
  onContextMenu?: (x: number, y: number, target: "canvas" | "node" | "edge", nodeId?: string, edgeId?: string) => void;
  onCreateNode?: (x: number, y: number) => void;
  onCreateRelationship?: (fromId: string, toId: string) => void;
}

const GraphVisualization = memo(forwardRef<GraphVisualizationRef, GraphVisualizationProps>(
  ({ onNodeSelect, onNodeDeselect, onEdgeSelect, onEdgeDeselect, onContextMenu, onCreateNode, onCreateRelationship }, ref) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const cyRef = useRef<Core | null>(null);
    const dragSourceRef = useRef<string | null>(null);
    const pendingDataRef = useRef<{ entities: Entity[]; relationships: Relationship[] } | null>(null);


      useEffect(() => {
      // Ensure container is mounted and has dimensions before initializing
      if (!containerRef.current) return;
      
      // Handle window resize - defined outside so we can clean it up
      const handleResize = () => {
        if (cyRef.current && !cyRef.current.destroyed()) {
          cyRef.current.resize();
        }
      };
      
      // Wait for next tick to ensure DOM is fully ready
      const initCytoscape = () => {
        if (!containerRef.current) return;
        
        // Check if container has dimensions (is visible)
        const rect = containerRef.current.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) {
          // Container not ready yet, retry after a short delay
          setTimeout(initCytoscape, 100);
          return;
        }

        // Entity type colors - constant, no need for dependency
        const entityColors: Record<string, string> = {
          Person: "#3b82f6", // Blue
          Organization: "#10b981", // Green
          Location: "#f59e0b", // Amber
          Concept: "#8b5cf6", // Purple
          Event: "#ef4444", // Red
          Technology: "#06b6d4", // Cyan
        };

        // Initialize Cytoscape only when container is ready
        try {
          cyRef.current = cytoscape({
            container: containerRef.current,
        style: [
          {
            selector: "node",
            style: {
              "background-color": (ele) => {
                const type = ele.data("type") || "Concept";
                return entityColors[type] || "#6b7280";
              },
              "label": "data(label)",
              "width": 40,
              "height": 40,
              "shape": "ellipse",
              "text-valign": "center",
              "text-halign": "center",
              "color": "#ffffff",
              "font-size": "12px",
              "font-weight": "bold",
              "text-wrap": "wrap",
              "text-max-width": "100px",
              "border-width": 2,
              "border-color": "#ffffff",
              "transition-duration": 0, // Disable transitions for instant feedback
            },
          },
          {
            selector: "edge",
            style: {
              "width": 2,
              "line-color": "#9ca3af",
              "target-arrow-color": "#9ca3af",
              "target-arrow-shape": "triangle",
              "curve-style": "bezier",
              "label": "data(type)",
              "font-size": "10px",
              "text-rotation": "autorotate",
              "text-margin-y": -10,
              "transition-duration": 0, // Disable transitions
            },
          },
          {
            selector: "node:selected",
            style: {
              "border-width": 6,
              "border-color": "#f59e0b",
              "background-color": (ele) => {
                const type = ele.data("type") || "Concept";
                const baseColor = entityColors[type] || "#6b7280";
                return baseColor;
              },
              "width": 50, // Make selected nodes slightly larger
              "height": 50,
            },
          },
          {
            selector: "node.dragging",
            style: {
              "border-width": 4,
              "border-color": "#3b82f6",
              "opacity": 0.7,
            },
          },
          {
            selector: "edge:selected",
            style: {
              "width": 4,
              "line-color": "#f59e0b",
              "target-arrow-color": "#f59e0b",
              "opacity": 1,
            },
          },
        ],
        layout: {
          name: "cola",
          padding: 50,
          animate: false, // Disable animations for better UX
        } as any,
        minZoom: 0.1,
        maxZoom: 2,
      });
        } catch (error) {
          console.error("Error initializing Cytoscape:", error);
          return;
        }

      // Event handlers
      cyRef.current.on("tap", "node", (evt) => {
        const node = evt.target;
        const entityId = node.data("id");
        if (onNodeSelect) {
          onNodeSelect(entityId);
        }
        // Deselect edge when node is selected
        if (onEdgeDeselect) {
          onEdgeDeselect();
        }
      });

      cyRef.current.on("tap", "edge", (evt) => {
        const edge = evt.target;
        const edgeId = edge.data("id");
        if (onEdgeSelect) {
          onEdgeSelect(edgeId);
        }
        // Deselect node when edge is selected
        if (onNodeDeselect) {
          onNodeDeselect();
        }
      });

      cyRef.current.on("tap", (evt) => {
        if (evt.target === cyRef.current) {
          if (onNodeDeselect) {
            onNodeDeselect();
          }
          if (onEdgeDeselect) {
            onEdgeDeselect();
          }
        }
      });

      // Double-click on canvas to create node
      cyRef.current.on("dbltap", (evt) => {
        if (evt.target === cyRef.current && onCreateNode) {
          const pos = evt.position || evt.renderedPosition;
          onCreateNode(pos.x, pos.y);
        }
      });

      // Drag and drop for relationship creation
      cyRef.current.on("mousedown", "node", (evt) => {
        const node = evt.target;
        dragSourceRef.current = node.data("id");
        node.addClass("dragging");
      });

      cyRef.current.on("mouseup", "node", (evt) => {
        const node = evt.target;
        const targetId = node.data("id");
        
        if (dragSourceRef.current && dragSourceRef.current !== targetId && onCreateRelationship) {
          // Create relationship from source to target
          onCreateRelationship(dragSourceRef.current, targetId);
        }
        
        // Clean up
        cyRef.current?.elements().removeClass("dragging");
        dragSourceRef.current = null;
      });

      cyRef.current.on("mouseout", "node", () => {
        // Reset if mouse leaves node
        cyRef.current?.elements().removeClass("dragging");
        dragSourceRef.current = null;
      });

      // Right-click context menu
      cyRef.current.on("cxttap", "node", (evt) => {
        evt.preventDefault();
        const node = evt.target;
        const entityId = node.data("id");
        const pos = evt.position || evt.renderedPosition;
        const containerPos = containerRef.current?.getBoundingClientRect();
        if (onContextMenu && containerPos) {
          onContextMenu(
            containerPos.left + pos.x,
            containerPos.top + pos.y,
            "node",
            entityId
          );
        }
      });

      cyRef.current.on("cxttap", "edge", (evt) => {
        evt.preventDefault();
        const edge = evt.target;
        const edgeId = edge.data("id");
        const pos = evt.position || evt.renderedPosition;
        const containerPos = containerRef.current?.getBoundingClientRect();
        if (onContextMenu && containerPos) {
          onContextMenu(
            containerPos.left + pos.x,
            containerPos.top + pos.y,
            "edge",
            undefined,
            edgeId
          );
        }
      });

      cyRef.current.on("cxttap", (evt) => {
        if (evt.target === cyRef.current) {
          evt.preventDefault();
          const pos = evt.position || evt.renderedPosition;
          const containerPos = containerRef.current?.getBoundingClientRect();
          if (onContextMenu && containerPos) {
            onContextMenu(
              containerPos.left + pos.x,
              containerPos.top + pos.y,
              "canvas"
            );
          }
        }
      });

        // Add resize listener
        window.addEventListener("resize", handleResize);
        
        // If there's pending data, load it now that Cytoscape is ready
        if (pendingDataRef.current) {
          const { entities, relationships } = pendingDataRef.current;
          const nodes = entities.map((entity) => ({
            data: {
              id: entity.id,
              label: entity.label,
              type: entity.type,
              ...entity.properties,
            },
          }));

          const edges = relationships.map((rel) => ({
            data: {
              id: rel.id,
              source: rel.from,
              target: rel.to,
              type: rel.type,
              ...rel.properties,
            },
          }));

          try {
            cyRef.current.elements().remove();
            cyRef.current.json({ elements: [...nodes, ...edges] });
            
            if (nodes.length > 0 || edges.length > 0) {
              cyRef.current.layout({ name: "cola", animate: false } as any).run();
            } else {
              cyRef.current.fit(undefined, 50);
            }
            pendingDataRef.current = null; // Clear pending data
          } catch (error) {
            console.error("Error loading pending graph data:", error);
          }
        }
      };

      // Initialize with a small delay to ensure DOM is ready
      const timeoutId = setTimeout(initCytoscape, 0);

      return () => {
        clearTimeout(timeoutId);
        window.removeEventListener("resize", handleResize);
        if (cyRef.current) {
          try {
            cyRef.current.destroy();
          } catch (error) {
            console.error("Error destroying Cytoscape:", error);
          }
          cyRef.current = null;
        }
      };
    }, [onNodeSelect, onNodeDeselect, onContextMenu, onCreateNode, onCreateRelationship]);

    useImperativeHandle(ref, () => ({
      loadGraphData: (entities: Entity[], relationships: Relationship[]) => {
        // If Cytoscape isn't ready yet, store the data and it will load when ready
        if (!cyRef.current || !containerRef.current || (cyRef.current && cyRef.current.destroyed())) {
          console.log("Cytoscape not initialized yet, storing data for later loading", { 
            entityCount: entities.length, 
            relationshipCount: relationships.length 
          });
          // Store data to load when Cytoscape is ready
          pendingDataRef.current = { entities, relationships };
          return;
        }

        try {
          const nodes = entities.map((entity) => ({
            data: {
              id: entity.id,
              label: entity.label,
              type: entity.type,
              ...entity.properties,
            },
          }));

          const edges = relationships.map((rel) => ({
            data: {
              id: rel.id,
              source: rel.from,
              target: rel.to,
              type: rel.type,
              ...rel.properties,
            },
          }));

          // Check if Cytoscape is ready before operations
          if (!cyRef.current || cyRef.current.destroyed()) {
            console.warn("Cytoscape instance is destroyed, cannot load graph data");
            return;
          }

          // Always clear and reload to ensure graph updates
          cyRef.current.elements().remove();
          
          // Load elements (even if empty - this ensures graph is initialized)
          cyRef.current.json({ elements: [...nodes, ...edges] });
          
          if (nodes.length > 0 || edges.length > 0) {
            cyRef.current.layout({ name: "cola", animate: false } as any).run();
          } else {
            // Center view for empty graph
            cyRef.current.fit(undefined, 50);
          }
        } catch (error) {
          console.error("Error loading graph data:", error);
        }
      },

      addNode: (entity: Entity) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          cyRef.current.add({
            data: {
              id: entity.id,
              label: entity.label,
              type: entity.type,
              ...entity.properties,
            },
          });

          cyRef.current.layout({ name: "cola", animate: false } as any).run();
        } catch (error) {
          console.error("Error adding node:", error);
        }
      },

      addEdge: (relationship: Relationship) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          cyRef.current.add({
            data: {
              id: relationship.id,
              source: relationship.from,
              target: relationship.to,
              type: relationship.type,
              ...relationship.properties,
            },
          });

          cyRef.current.layout({ name: "cola", animate: false } as any).run();
        } catch (error) {
          console.error("Error adding edge:", error);
        }
      },

      updateNode: (entity: Entity) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          const node = cyRef.current.getElementById(entity.id);
          if (node.length > 0) {
            node.data({
              label: entity.label,
              type: entity.type,
              ...entity.properties,
            });
            // Trigger style update
            node.trigger("style");
          }
        } catch (error) {
          console.error("Error updating node:", error);
        }
      },

      updateEdge: (relationship: Relationship) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          const edge = cyRef.current.getElementById(relationship.id);
          if (edge.length > 0) {
            edge.data({
              source: relationship.from,
              target: relationship.to,
              type: relationship.type,
              ...relationship.properties,
            });
            // Trigger style update
            edge.trigger("style");
          }
        } catch (error) {
          console.error("Error updating edge:", error);
        }
      },

      removeNode: (id: string) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          const node = cyRef.current.getElementById(id);
          node.remove();
          cyRef.current.layout({ name: "cola", animate: false } as any).run();
        } catch (error) {
          console.error("Error removing node:", error);
        }
      },

      removeEdge: (id: string) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          const edge = cyRef.current.getElementById(id);
          edge.remove();
          cyRef.current.layout({ name: "cola", animate: false } as any).run();
        } catch (error) {
          console.error("Error removing edge:", error);
        }
      },

      highlightNode: (id: string) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          cyRef.current.elements().removeClass("highlighted");
          cyRef.current.elements("edge").removeClass("selected");
          const node = cyRef.current.getElementById(id);
          node.addClass("highlighted");
          cyRef.current.center(node);
          cyRef.current.zoom({
            level: 1.5,
            renderedPosition: { x: node.position().x, y: node.position().y },
          });
        } catch (error) {
          console.error("Error highlighting node:", error);
        }
      },

      highlightEdge: (id: string) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          cyRef.current.elements("node").removeClass("highlighted");
          cyRef.current.elements("edge").removeClass("selected");
          const edge = cyRef.current.getElementById(id);
          edge.addClass("selected");
          // Center view on the edge (midpoint between source and target)
          const sourceNode = edge.source();
          const targetNode = edge.target();
          if (sourceNode.length > 0 && targetNode.length > 0) {
            const sourcePos = sourceNode.position();
            const targetPos = targetNode.position();
            const midX = (sourcePos.x + targetPos.x) / 2;
            const midY = (sourcePos.y + targetPos.y) / 2;
            // Get current viewport center and pan to the midpoint
            const extent = cyRef.current.extent();
            const currentCenterX = (extent.x1 + extent.x2) / 2;
            const currentCenterY = (extent.y1 + extent.y2) / 2;
            cyRef.current.pan({
              x: midX - currentCenterX,
              y: midY - currentCenterY,
            });
          }
        } catch (error) {
          console.error("Error highlighting edge:", error);
        }
      },

      filterByType: (types: string[]) => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          if (types.length === 0) {
            cyRef.current.elements().forEach((ele) => {
              ele.style("display", "element");
            });
          } else {
            cyRef.current.elements().forEach((ele) => {
              ele.style("display", "none");
            });
            cyRef.current
              .elements()
              .filter((ele) => {
                if (ele.isNode()) {
                  return types.includes(ele.data("type"));
                }
                // Show edge if both endpoints are visible
                const source = ele.source();
                const target = ele.target();
                return (
                  types.includes(source.data("type")) &&
                  types.includes(target.data("type"))
                );
              })
              .forEach((ele) => {
                ele.style("display", "element");
              });
          }

          cyRef.current.layout({ name: "cola", animate: false } as any).run();
        } catch (error) {
          console.error("Error filtering by type:", error);
        }
      },

      exportGraph: (format: "png" | "json") => {
        if (!cyRef.current || cyRef.current.destroyed()) return;

        try {
          if (format === "png") {
            const png = cyRef.current.png({ full: true, bg: "white" });
            const link = document.createElement("a");
            link.download = "graph.png";
            link.href = png;
            link.click();
          } else {
            const json = JSON.stringify(cyRef.current.json(), null, 2);
            const blob = new Blob([json], { type: "application/json" });
            const link = document.createElement("a");
            link.download = "graph.json";
            link.href = URL.createObjectURL(blob);
            link.click();
          }
        } catch (error) {
          console.error("Error exporting graph:", error);
        }
      },

      fit: () => {
        if (!cyRef.current || cyRef.current.destroyed()) return;
        try {
          cyRef.current.fit(undefined, 50);
        } catch (error) {
          console.error("Error fitting graph:", error);
        }
      },

      resetZoom: () => {
        if (!cyRef.current || cyRef.current.destroyed()) return;
        try {
          cyRef.current.zoom(1);
          cyRef.current.center();
        } catch (error) {
          console.error("Error resetting zoom:", error);
        }
      },
    }));

    return (
      <div
        ref={containerRef}
        className="w-full h-full border border-gray-300 rounded-lg"
        style={{ minHeight: "400px" }}
      />
    );
  }
));

GraphVisualization.displayName = "GraphVisualization";

export default GraphVisualization;

