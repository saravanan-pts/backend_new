import { NextResponse } from "next/server";
import { graph } from "@/services/graph";
import type { Relationship } from "@/types";

/**
 * GET /api/relationships
 * Optional query param: ?documentId=
 */
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const documentId = searchParams.get("documentId");

    const relationships = await graph.getAllRelationships(
      documentId ?? undefined
    );

    return NextResponse.json(relationships);
  } catch (error: any) {
    console.error("GET /relationships error:", error);
    return NextResponse.json(
      { error: error.message ?? "Failed to fetch relationships" },
      { status: 500 }
    );
  }
}

/**
 * POST /api/relationships
 */
export async function POST(req: Request) {
  try {
    const body = (await req.json()) as {
      from: string;
      to: string;
      type: Relationship["type"];
      properties?: Relationship["properties"];
      confidence?: number;
      source?: string;
    };

    const relationship = await graph.createRelationship(
      body.from,
      body.to,
      body.type,
      body.properties,
      body.confidence,
      body.source
    );

    return NextResponse.json(relationship, { status: 201 });
  } catch (error: any) {
    console.error("POST /relationships error:", error);
    return NextResponse.json(
      { error: error.message ?? "Failed to create relationship" },
      { status: 500 }
    );
  }
}
