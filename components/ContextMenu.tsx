"use client";

import { useEffect, useRef, useState } from "react";
import { Plus, Edit2, Trash2, Link2 } from "lucide-react";
import ConfirmDialog from "@/components/ConfirmDialog";

export type ContextMenuTarget = "canvas" | "node" | "edge";

interface ContextMenuProps {
  x: number;
  y: number;
  target: ContextMenuTarget;
  onClose: () => void;
  onCreateNode?: () => void;
  onEditNode?: () => void;
  onDeleteNode?: () => void;
  onCreateRelationship?: () => void;
  onEditEdge?: () => void;
  onDeleteEdge?: () => void;
}

export default function ContextMenu({
  x,
  y,
  target,
  onClose,
  onCreateNode,
  onEditNode,
  onDeleteNode,
  onCreateRelationship,
  onEditEdge,
  onDeleteEdge,
}: ContextMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);
  const [showDeleteNodeConfirm, setShowDeleteNodeConfirm] = useState(false);
  const [showDeleteEdgeConfirm, setShowDeleteEdgeConfirm] = useState(false);

  // Close menu when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        onClose();
      }
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    document.addEventListener("keydown", handleEscape);

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      document.removeEventListener("keydown", handleEscape);
    };
  }, [onClose]);

  // Adjust position to keep menu in viewport
  const adjustedX = Math.min(x, window.innerWidth - 200);
  const adjustedY = Math.min(y, window.innerHeight - 200);

  const menuItems: Array<{
    label: string;
    icon: React.ReactNode;
    onClick: () => void;
    danger?: boolean;
  }> = [];

  if (target === "canvas") {
    if (onCreateNode) {
      menuItems.push({
        label: "Create Node",
        icon: <Plus className="w-4 h-4" />,
        onClick: () => {
          onCreateNode();
          onClose();
        },
      });
    }
  } else if (target === "node") {
    if (onEditNode) {
      menuItems.push({
        label: "Edit Node",
        icon: <Edit2 className="w-4 h-4" />,
        onClick: () => {
          onEditNode();
          onClose();
        },
      });
    }
    if (onCreateRelationship) {
      menuItems.push({
        label: "Create Relationship",
        icon: <Link2 className="w-4 h-4" />,
        onClick: () => {
          onCreateRelationship();
          onClose();
        },
      });
    }
    if (onDeleteNode) {
      menuItems.push({
        label: "Delete Node",
        icon: <Trash2 className="w-4 h-4" />,
        onClick: () => {
          setShowDeleteNodeConfirm(true);
        },
        danger: true,
      });
    }
  } else if (target === "edge") {
    if (onEditEdge) {
      menuItems.push({
        label: "Edit Relationship",
        icon: <Edit2 className="w-4 h-4" />,
        onClick: () => {
          onEditEdge();
          onClose();
        },
      });
    }
    if (onDeleteEdge) {
      menuItems.push({
        label: "Delete Relationship",
        icon: <Trash2 className="w-4 h-4" />,
        onClick: () => {
          setShowDeleteEdgeConfirm(true);
        },
        danger: true,
      });
    }
  }

  if (menuItems.length === 0) {
    return null;
  }

  return (
    <div
      ref={menuRef}
      className="fixed bg-white border border-gray-200 rounded-lg shadow-lg py-1 z-50 min-w-[180px]"
      style={{
        left: `${adjustedX}px`,
        top: `${adjustedY}px`,
      }}
    >
      {menuItems.map((item, index) => (
        <button
          key={index}
          onClick={item.onClick}
          className={`w-full px-4 py-2 text-left text-sm flex items-center gap-2 hover:bg-gray-100 ${
            item.danger ? "text-red-600 hover:bg-red-50" : "text-gray-700"
          }`}
        >
          {item.icon}
          {item.label}
        </button>
      ))}
      
      {/* Confirmation Dialogs */}
      {showDeleteNodeConfirm && (
        <ConfirmDialog
          title="Delete Node"
          message="Are you sure you want to delete this node? All connected relationships will also be deleted."
          confirmText="Delete"
          cancelText="Cancel"
          onConfirm={() => {
            onDeleteNode?.();
            setShowDeleteNodeConfirm(false);
            onClose();
          }}
          onCancel={() => {
            setShowDeleteNodeConfirm(false);
            onClose();
          }}
          danger={true}
        />
      )}

      {showDeleteEdgeConfirm && (
        <ConfirmDialog
          title="Delete Relationship"
          message="Are you sure you want to delete this relationship?"
          confirmText="Delete"
          cancelText="Cancel"
          onConfirm={() => {
            onDeleteEdge?.();
            setShowDeleteEdgeConfirm(false);
            onClose();
          }}
          onCancel={() => {
            setShowDeleteEdgeConfirm(false);
            onClose();
          }}
          danger={true}
        />
      )}
    </div>
  );
}

