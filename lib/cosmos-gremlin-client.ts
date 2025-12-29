import gremlin from "gremlin";

const { Client, auth } = gremlin.driver;
const { PlainTextSaslAuthenticator } = auth;

let client: any = null;

export function getGremlinClient() {
  if (client) return client;

  const endpoint = process.env.COSMOS_GREMLIN_ENDPOINT!;
  const key = process.env.COSMOS_GREMLIN_KEY!;
  const database = process.env.COSMOS_GREMLIN_DATABASE!;
  const container = process.env.COSMOS_GREMLIN_CONTAINER!;

  if (!endpoint || !key || !database || !container) {
    throw new Error("Missing Cosmos Gremlin configuration");
  }

  const authenticator = new PlainTextSaslAuthenticator(
    `/dbs/${database}/colls/${container}`,
    key
  );

  client = new Client(endpoint, {
    authenticator,
    traversalsource: "g",
    rejectUnauthorized: true,
    mimeType: "application/vnd.gremlin-v2.0+json",
  });

  return client;
}
