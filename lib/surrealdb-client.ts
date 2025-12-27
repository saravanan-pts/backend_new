import { Surreal } from "surrealdb.js";
import { SCHEMA_QUERIES, TABLES } from "./schema";

class SurrealDBClient {
  private static instance: SurrealDBClient | null = null;
  private db: Surreal | null = null;
  private isConnected: boolean = false;
  private connectionAttempts: number = 0;
  private maxRetries: number = 5;
  private baseDelay: number = 1000; // 1 second
  private connectingPromise: Promise<void> | null = null; // Lock to prevent concurrent connections

  private constructor() {}

  static getInstance(): SurrealDBClient {
    if (!SurrealDBClient.instance) {
      SurrealDBClient.instance = new SurrealDBClient();
    }
    return SurrealDBClient.instance;
  }

  async connect(): Promise<void> {
    // If already connected, return immediately
    if (this.isConnected && this.db) {
      return;
    }

    // If a connection attempt is in progress, wait for it
    if (this.connectingPromise) {
      return this.connectingPromise;
    }

    // Start a new connection attempt
    this.connectingPromise = this._connect();
    
    try {
      await this.connectingPromise;
    } finally {
      this.connectingPromise = null;
    }
  }

  private async _connect(): Promise<void> {
    const url = process.env.NEXT_PUBLIC_SURREALDB_URL;
    const namespace = process.env.NEXT_PUBLIC_SURREALDB_NAMESPACE;
    const database = process.env.NEXT_PUBLIC_SURREALDB_DATABASE;
    // This code runs on the client side, so we can only access NEXT_PUBLIC_ prefixed env vars
    // Server-side env vars (without prefix) are not available in client bundles
    const token = process.env.NEXT_PUBLIC_SURREALDB_TOKEN;
    const username = process.env.NEXT_PUBLIC_SURREALDB_USERNAME;
    const password = process.env.NEXT_PUBLIC_SURREALDB_PASSWORD;

    if (!url || !namespace || !database) {
      throw new Error(
        "Missing SurrealDB configuration. Please check environment variables."
      );
    }

    try {
      // Clean up any existing connection before creating a new one
      if (this.db) {
        try {
          await this.db.close();
        } catch (e) {
          // Ignore errors when closing old connection
        }
        this.db = null;
      }

      this.db = new Surreal();
      
      // Set a connection timeout
      const connectPromise = this.db.connect(url);
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error("Connection timeout after 10 seconds")), 10000);
      });
      
      await Promise.race([connectPromise, timeoutPromise]);

      // Authenticate using JWT token (preferred for SurrealDB Cloud)
      if (token) {
        console.log("Authenticating with JWT token...");
        await this.db.authenticate(token);
        console.log("JWT authentication successful");
      }
      // Or authenticate using username/password (alternative method)
      else if (username && password) {
        console.log("Authenticating with username/password...", { username, hasPassword: !!password });
        try {
          // SurrealDB.js signin accepts { user, pass } format
          await this.db.signin({ user: username, pass: password });
          console.log("Username/password authentication successful");
        } catch (signinError: any) {
          console.error("Signin error:", signinError);
          // For SurrealDB Cloud, username/password might not work - need JWT token
          throw new Error(`Authentication failed: ${signinError.message || signinError}. Note: SurrealDB Cloud may require JWT token authentication instead of username/password.`);
        }
      } else {
        console.warn("No authentication credentials provided - connection will be unauthenticated");
        // If neither provided, connection will be unauthenticated (may have limited access)
      }

      await this.db.use({ ns: namespace, db: database });

      // Initialize schema (non-blocking - continue even if it fails)
      try {
        await this.initializeSchema();
      } catch (schemaError: any) {
        // Log but don't fail connection if schema init fails (might be permission issue)
        console.warn("Schema initialization warning:", schemaError);
        // Continue with connection even if schema init fails
      }

      this.isConnected = true;
      this.connectionAttempts = 0;
      console.log("Successfully connected to SurrealDB");
    } catch (error: any) {
      this.isConnected = false;
      this.connectionAttempts++;

      // Log the actual error for debugging
      const errorMessage = error?.message || String(error);
      const errorStack = error?.stack;
      
      console.error(
        `Connection failed (attempt ${this.connectionAttempts}/${this.maxRetries}):`,
        errorMessage
      );
      
      // Log more details in development
      if (process.env.NODE_ENV === "development") {
        console.error("Connection error details:", {
          url: url ? `${url.substring(0, 20)}...` : "missing",
          namespace,
          database,
          hasToken: !!token,
          hasUsername: !!username,
          error: errorMessage,
        });
      }

      if (this.connectionAttempts < this.maxRetries) {
        const delay = this.baseDelay * Math.pow(2, this.connectionAttempts - 1);
        console.log(
          `Connection failed. Retrying in ${delay}ms (attempt ${this.connectionAttempts}/${this.maxRetries})...`
        );
        await this.sleep(delay);
        // Call _connect directly since we're already in a connection attempt
        return this._connect();
      }

      console.error("Failed to connect to SurrealDB after multiple attempts:", error);
      throw new Error(
        `Failed to connect to SurrealDB after ${this.maxRetries} attempts: ${errorMessage}`
      );
    }
  }

  private async initializeSchema(): Promise<void> {
    if (!this.db) return;

    try {
      // First, remove any existing id field definitions that might cause conflicts
      // SurrealDB automatically manages the id field, so we shouldn't define it
      try {
        await this.db.query(`REMOVE FIELD id ON ${TABLES.DOCUMENT}`);
      } catch (e) {
        // Field might not exist, which is fine
      }
      try {
        await this.db.query(`REMOVE FIELD id ON ${TABLES.ENTITY}`);
      } catch (e) {
        // Field might not exist, which is fine
      }
      try {
        await this.db.query(`REMOVE FIELD id ON ${TABLES.RELATIONSHIP}`);
      } catch (e) {
        // Field might not exist, which is fine
      }

      // Execute schema definitions
      await this.db.query(SCHEMA_QUERIES.defineEntityTable);
      await this.db.query(SCHEMA_QUERIES.defineRelationshipTable);
      await this.db.query(SCHEMA_QUERIES.defineDocumentTable);
      console.log("Schema initialized successfully");
    } catch (error) {
      // Schema might already exist, which is fine
      console.warn("Schema initialization warning:", error);
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async disconnect(): Promise<void> {
    if (this.db) {
      try {
        await this.db.close();
        this.isConnected = false;
        this.db = null;
        console.log("Disconnected from SurrealDB");
      } catch (error) {
        console.error("Error disconnecting from SurrealDB:", error);
      }
    }
  }

  async healthCheck(): Promise<boolean> {
    try {
      if (!this.isConnected || !this.db) {
        await this.connect();
      }

      if (!this.db) {
        return false;
      }

      // Simple query to check connection
      // Use a simple query that works even if tables don't exist
      try {
        await this.db.query("INFO FOR DB");
        return true;
      } catch (err) {
        // If INFO fails, try a simple SELECT (might fail if no tables, but connection is OK)
        try {
          await this.db.query("SELECT * FROM entity LIMIT 1");
          return true;
        } catch {
          // Connection exists even if query fails (permissions issue)
          return true;
        }
      }
    } catch (error) {
      console.error("Health check failed:", error);
      this.isConnected = false;
      return false;
    }
  }

  getClient(): Surreal {
    if (!this.db || !this.isConnected) {
      throw new Error(
        "SurrealDB client not connected. Call connect() first."
      );
    }
    return this.db;
  }

  getConnectionStatus(): boolean {
    return this.isConnected;
  }
}

// Export singleton instance
export const surrealDB = SurrealDBClient.getInstance();

