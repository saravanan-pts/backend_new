import { NextRequest, NextResponse } from "next/server";
import { documentProcessor } from "@/services/document-processor";
import { surrealDB } from "@/lib/surrealdb-client";

// Ensure this route only runs on the server
export const runtime = "nodejs";
export const maxDuration = 300; // 5 minutes for large file processing
export const dynamic = "force-dynamic";

export async function POST(request: NextRequest) {
  try {
    // Ensure database connection
    await surrealDB.connect();

    const formData = await request.formData();
    const file = formData.get("file") as File;
    const text = formData.get("text") as string | null;

    if (!file && !text) {
      return NextResponse.json(
        { error: "No file or text provided" },
        { status: 400 }
      );
    }

    let result;

    if (text) {
      // Process text directly
      result = await documentProcessor.processText(text, "api-input.txt");
    } else if (file) {
      // Process file
      const fileType = file.name.split(".").pop()?.toLowerCase();

      switch (fileType) {
        case "pdf":
          result = await documentProcessor.processPDF(file);
          break;
        case "csv":
          result = await documentProcessor.processCSV(file);
          break;
        case "docx":
        case "doc":
          result = await documentProcessor.processDOCX(file);
          break;
        case "txt":
        default:
          const textContent = await file.text();
          result = await documentProcessor.processText(textContent, file.name);
          break;
      }
    } else {
      return NextResponse.json(
        { error: "Invalid request" },
        { status: 400 }
      );
    }

    return NextResponse.json({
      success: true,
      document: result.document,
      entities: result.entities,
      relationships: result.relationships,
      stats: {
        entityCount: result.entities.length,
        relationshipCount: result.relationships.length,
      },
    });
  } catch (error: any) {
    console.error("Error processing file:", error);
    return NextResponse.json(
      {
        error: error.message || "Failed to process file",
        details: process.env.NODE_ENV === "development" ? error.stack : undefined,
      },
      { status: 500 }
    );
  }
}

