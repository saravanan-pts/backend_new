import { NextResponse } from "next/server";
import { graph } from "@/services/graph";
import type { Entity } from "@/types";

/**
 * GET /api/entities
 * Optional query param: ?documentId=
 */
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const documentId = searchParams.get("documentId");

    const entities = documentId
      ? await graph.getEntitiesByDocument(documentId)
      : await graph.getAllEntities();

    return NextResponse.json(entities);
  } catch (error: any) {
    console.error("GET /entities error:", error);
    return NextResponse.json(
      { error: error.message ?? "Failed to fetch entities" },
      { status: 500 }
    );
  }
}

/**
 * POST /api/entities
 */
export async function POST(req: Request) {
  try {
    const body = (await req.json()) as Omit<
      Entity,
      "id" | "createdAt" | "updatedAt"
    >;

    const entity = await graph.createEntity(body);
    return NextResponse.json(entity, { status: 201 });
  } catch (error: any) {
    console.error("POST /entities error:", error);
    return NextResponse.json(
      { error: error.message ?? "Failed to create entity" },
      { status: 500 }
    );
  }
}
