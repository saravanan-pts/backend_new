import { NextRequest, NextResponse } from "next/server";
import { documentProcessor } from "@/services/document-processor";

// Ensure this route only runs on the server
export const runtime = "nodejs";
export const maxDuration = 300; // 5 minutes for large file processing
export const dynamic = "force-dynamic";

/**
 * POST /api/process
 * Accepts text or file input and builds a knowledge graph
 * using the active graph driver (SurrealDB or Cosmos Gremlin)
 */
export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData();
    const file = formData.get("file") as File | null;
    const text = formData.get("text") as string | null;

    if (!file && !text) {
      return NextResponse.json(
        { error: "No file or text provided" },
        { status: 400 }
      );
    }

    let result;

    if (text) {
      // Process raw text
      result = await documentProcessor.processText(text, "api-input.txt");
    } else if (file) {
      const fileType = file.name.split(".").pop()?.toLowerCase();

      switch (fileType) {
        case "pdf":
          result = await documentProcessor.processPDF(file);
          break;

        case "csv":
          result = await documentProcessor.processCSV(file);
          break;

        case "doc":
        case "docx":
          result = await documentProcessor.processDOCX(file);
          break;

        case "txt":
        default: {
          const textContent = await file.text();
          result = await documentProcessor.processText(
            textContent,
            file.name
          );
          break;
        }
      }
    }

    return NextResponse.json({
      success: true,
      document: result!.document,
      entities: result!.entities,
      relationships: result!.relationships,
      stats: {
        entityCount: result!.entities.length,
        relationshipCount: result!.relationships.length,
      },
    });
  } catch (error: any) {
    console.error("Error processing input:", error);

    return NextResponse.json(
      {
        success: false,
        error: error?.message ?? "Failed to process input",
        details:
          process.env.NODE_ENV === "development"
            ? error?.stack
            : undefined,
      },
      { status: 500 }
    );
  }
}
