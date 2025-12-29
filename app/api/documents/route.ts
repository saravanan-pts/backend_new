import { NextResponse } from "next/server";
import { graph } from "@/services/graph";

/**
 * GET /api/documents
 */
export async function GET() {
  try {
    const documents = await graph.getAllDocuments();
    return NextResponse.json(documents);
  } catch (error: any) {
    console.error("GET /documents error:", error);
    return NextResponse.json(
      { error: error.message ?? "Failed to fetch documents" },
      { status: 500 }
    );
  }
}
