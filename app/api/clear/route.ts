import { NextRequest, NextResponse } from "next/server";
import { graph } from "@/services/graph";

// Ensure this route only runs on the server
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/**
 * DELETE /api/clear
 * Clears all data from the active graph database
 * (SurrealDB or Cosmos Gremlin)
 *
 * ⚠️ WARNING: This permanently deletes all data!
 */
export async function DELETE(_: NextRequest) {
  try {
    const result = await graph.clearAllData();

    return NextResponse.json({
      success: true,
      message: "All data cleared successfully",
      deleted: result,
    });
  } catch (error: any) {
    console.error("Error clearing graph data:", error);

    return NextResponse.json(
      {
        success: false,
        error: error?.message ?? "Failed to clear graph data",
      },
      { status: 500 }
    );
  }
}

/**
 * POST /api/clear
 * Alias for DELETE (useful for clients that can't send DELETE)
 */
export async function POST(request: NextRequest) {
  return DELETE(request);
}
