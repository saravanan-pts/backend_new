import { NextRequest, NextResponse } from "next/server";
import { graphOps } from "@/services/graph-operations";
import { surrealDB } from "@/lib/surrealdb-client";

// Ensure this route only runs on the server
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/**
 * DELETE /api/clear
 * Clears all data from the database (entities, relationships, and documents)
 * WARNING: This permanently deletes all data!
 */
export async function DELETE(request: NextRequest) {
  try {
    // Ensure database connection
    await surrealDB.connect();

    // Clear all data
    const result = await graphOps.clearAllData();

    return NextResponse.json({
      success: true,
      message: "All data cleared successfully",
      deleted: result,
    });
  } catch (error: any) {
    console.error("Error clearing database:", error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || "Failed to clear database",
      },
      { status: 500 }
    );
  }
}

/**
 * POST /api/clear (alternative method)
 * Same as DELETE but using POST method
 */
export async function POST(request: NextRequest) {
  return DELETE(request);
}

