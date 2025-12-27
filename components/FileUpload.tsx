"use client";

import { useCallback, useState, memo } from "react";
import { useDropzone } from "react-dropzone";
import { Upload, File, X, CheckCircle2, AlertCircle } from "lucide-react";
import { useFileProcessor } from "@/hooks/useFileProcessor";
import toast from "react-hot-toast";

const MAX_FILE_SIZE_MB = 10;
const MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024;
const ACCEPTED_TYPES = {
  "text/plain": [".txt"],
  "application/pdf": [".pdf"],
  "text/csv": [".csv"],
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document": [
    ".docx",
  ],
};

interface FileWithPreview extends File {
  preview?: string;
  status?: "pending" | "processing" | "success" | "error";
  result?: {
    entities: number;
    relationships: number;
  };
}

function FileUpload() {
  const { processFile, isProcessing, progress, error } = useFileProcessor();
  const [files, setFiles] = useState<FileWithPreview[]>([]);

  const onDrop = useCallback(
    async (acceptedFiles: File[]) => {
      const newFiles = acceptedFiles.map((file) => ({
        ...file,
        status: "pending" as const,
      }));

      setFiles((prev) => [...prev, ...newFiles]);

      // Process each file sequentially
      for (const file of acceptedFiles) {
        try {
          // Update status to processing
          setFiles((prev) => {
            const updated = [...prev];
            const fileIndex = updated.findIndex((f) => f.name === file.name);
            if (fileIndex >= 0) {
              updated[fileIndex] = { ...updated[fileIndex], status: "processing" };
            }
            return updated;
          });

          const result = await processFile(file);

          // Update status to success
          setFiles((prev) => {
            const updated = [...prev];
            const fileIndex = updated.findIndex((f) => f.name === file.name);
            if (fileIndex >= 0) {
              updated[fileIndex] = {
                ...updated[fileIndex],
                status: "success",
                result: {
                  entities: result.entities.length,
                  relationships: result.relationships.length,
                },
              };
            }
            return updated;
          });

          toast.success(
            `Processed ${file.name}: ${result.entities.length} entities, ${result.relationships.length} relationships`
          );
        } catch (err: any) {
          setFiles((prev) => {
            const updated = [...prev];
            const fileIndex = updated.findIndex((f) => f.name === file.name);
            if (fileIndex >= 0) {
              updated[fileIndex] = {
                ...updated[fileIndex],
                status: "error",
              };
            }
            return updated;
          });

          toast.error(`Failed to process ${file.name}: ${err.message}`);
        }
      }
    },
    [processFile]
  );

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: ACCEPTED_TYPES,
    maxSize: MAX_FILE_SIZE,
    multiple: true,
  });

  const removeFile = (index: number) => {
    setFiles((prev) => prev.filter((_, i) => i !== index));
  };

  const formatFileSize = (bytes: number): string => {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
    return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  };

  return (
    <div className="space-y-4">
      {/* Dropzone */}
      <div
        {...getRootProps()}
        className={`
          border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-colors
          ${
            isDragActive
              ? "border-blue-500 bg-blue-50"
              : "border-gray-300 hover:border-gray-400"
          }
        `}
      >
        <input {...getInputProps()} />
        <Upload className="w-12 h-12 mx-auto mb-4 text-gray-400" />
        {isDragActive ? (
          <p className="text-blue-600">Drop files here...</p>
        ) : (
          <div>
            <p className="text-gray-600 mb-2">
              Drag and drop files here, or click to select
            </p>
            <p className="text-sm text-gray-500">
              Supported: TXT, PDF, CSV, DOCX (max {MAX_FILE_SIZE_MB}MB)
            </p>
          </div>
        )}
      </div>

      {/* File List */}
      {files.length > 0 && (
        <div className="space-y-2">
          <h3 className="font-semibold text-sm">Uploaded Files</h3>
          {files.map((file, index) => (
            <div
              key={index}
              className="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
            >
              <div className="flex items-center gap-3 flex-1 min-w-0">
                <File className="w-5 h-5 text-gray-400 flex-shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium truncate">{file.name}</p>
                  <p className="text-xs text-gray-500">
                    {formatFileSize(file.size)}
                  </p>
                  {file.status === "processing" && (
                    <div className="mt-1">
                      <div className="w-full bg-gray-200 rounded-full h-1.5">
                        <div
                          className="bg-blue-600 h-1.5 rounded-full transition-all"
                          style={{ width: `${progress}%` }}
                        />
                      </div>
                    </div>
                  )}
                  {file.status === "success" && file.result && (
                    <p className="text-xs text-green-600 mt-1">
                      {file.result.entities} entities, {file.result.relationships}{" "}
                      relationships extracted
                    </p>
                  )}
                  {file.status === "error" && (
                    <p className="text-xs text-red-600 mt-1">Processing failed</p>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  {file.status === "success" && (
                    <CheckCircle2 className="w-5 h-5 text-green-600" />
                  )}
                  {file.status === "error" && (
                    <AlertCircle className="w-5 h-5 text-red-600" />
                  )}
                  <button
                    onClick={() => removeFile(index)}
                    className="p-1 hover:bg-gray-200 rounded"
                  >
                    <X className="w-4 h-4 text-gray-500" />
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Error Display */}
      {error && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
          <p className="text-sm text-red-600">{error}</p>
        </div>
      )}
    </div>
  );
}

export default memo(FileUpload);
