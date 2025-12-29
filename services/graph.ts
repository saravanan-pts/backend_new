import type { GraphDriver } from "./drivers/graph-driver.interface";
import { SurrealGraphDriver } from "./drivers/surreal-graph.driver";
import { CosmosGremlinGraphDriver } from "./drivers/cosmos-gremlin-graph.driver";

const GRAPH_DB = process.env.GRAPH_DB ?? "surreal";

let driver: GraphDriver;

switch (GRAPH_DB) {
  case "cosmos":
  case "cosmos-gremlin":
    console.log("[GraphFactory] Using Cosmos Gremlin Graph Driver");
    driver = new CosmosGremlinGraphDriver();
    break;

  case "surreal":
  default:
    console.log("[GraphFactory] Using SurrealDB Graph Driver");
    driver = new SurrealGraphDriver();
    break;
}

export const graph = driver;
