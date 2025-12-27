import { useEffect, useState, useCallback } from "react";
import { surrealDB } from "@/lib/surrealdb-client";

export function useSurrealDB() {
  const [isConnected, setIsConnected] = useState(false);
  const [isConnecting, setIsConnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const connect = useCallback(async () => {
    setIsConnecting(true);
    setError(null);
    try {
      await surrealDB.connect();
      setIsConnected(true);
    } catch (err: any) {
      setError(err.message || "Failed to connect to SurrealDB");
      setIsConnected(false);
    } finally {
      setIsConnecting(false);
    }
  }, []);

  const disconnect = useCallback(async () => {
    try {
      await surrealDB.disconnect();
      setIsConnected(false);
    } catch (err: any) {
      setError(err.message || "Failed to disconnect from SurrealDB");
    }
  }, []);

  const checkHealth = useCallback(async () => {
    try {
      const healthy = await surrealDB.healthCheck();
      setIsConnected(healthy);
      return healthy;
    } catch (err: any) {
      setError(err.message || "Health check failed");
      setIsConnected(false);
      return false;
    }
  }, []);

  useEffect(() => {
    // Attempt to connect on mount
    connect();

    // Periodic health check
    const interval = setInterval(() => {
      checkHealth();
    }, 30000); // Every 30 seconds

    return () => {
      clearInterval(interval);
      disconnect();
    };
  }, [connect, disconnect, checkHealth]);

  return {
    isConnected,
    isConnecting,
    error,
    connect,
    disconnect,
    checkHealth,
  };
}

