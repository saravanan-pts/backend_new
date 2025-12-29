import { NextResponse } from "next/server";
import { graph } from "@/services/graph";

/**
 * GET /api/graph
 * Optional query param: ?documentId=
 */
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const documentId = searchParams.get("documentId");

    const [entities, relationships] = documentId
      ? await Promise.all([
          graph.getEntitiesByDocument(documentId),
          graph.getAllRelationships(documentId),
        ])
      : await Promise.all([
          graph.getAllEntities(),
          graph.getAllRelationships(),
        ]);

    return NextResponse.json({
      entities,
      relationships,
    });
  } catch (error: any) {
    console.error("GET /graph error:", error);
    return NextResponse.json(
      { error: error.message ?? "Failed to load graph" },
      { status: 500 }
    );
  }
}
