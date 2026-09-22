/**
 * Vanth wake relay for OpenCode.
 *
 * A plain `opencode` TUI binds no TCP port and injects no session id into its
 * MCP children, so a job cannot wake it with `opencode run --attach`: there is
 * no URL to attach to and no isolated backend that the visible session would
 * ever see. This plugin closes that gap from the INSIDE.
 *
 * It registers the session it lives in with the Vanth daemon and long-polls the
 * same client relay protocol Codex Desktop uses (`/relay/register|poll|ack`).
 * A wake is injected with `client.session.promptAsync`, which goes through the
 * running process's own server, so the prompt lands in the session the user is
 * looking at. The delivery is acknowledged only after injection succeeds, and
 * carries the daemon-issued lease token, so a second client cannot steal it.
 *
 * Install: `vanth setup` copies this file to ~/.config/opencode/plugins/.
 * Nothing here is required for Vanth to run; if the daemon is unreachable the
 * loop simply backs off.
 */
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import type { Plugin } from "@opencode-ai/plugin";

const POLL_TIMEOUT_SECONDS = 30;
const POLL_ERROR_BACKOFF_MS = 5_000;
const NO_RELAY_BACKOFF_MS = 1_500;

type Connection = { url: string; token: string };
type Destination = { client_type: "opencode_thread"; session_id: string; directory: string };
type Delivery = {
  delivery_id: string;
  payload?: {
    prompt?: string;
    target?: { session_id?: string; sessionId?: string; thread_id?: string; threadId?: string };
  };
  lease_token?: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function readConnection(): Promise<Connection | null> {
  const home = process.env.VANTH_HOME ?? path.join(os.homedir(), ".vanth");
  let url = process.env.VANTH_URL ?? "";
  let token = process.env.VANTH_TOKEN ?? "";
  try {
    if (!url) {
      const meta = JSON.parse(await fs.readFile(path.join(home, "daemon.json"), "utf8"));
      if (typeof meta?.url === "string") url = meta.url;
    }
    if (!token) {
      const tokenPath = process.env.VANTH_TOKEN_FILE ?? path.join(home, "token");
      token = (await fs.readFile(tokenPath, "utf8")).trim();
    }
  } catch {
    return null;
  }
  if (!url || !token) return null;
  return { url: url.replace(/\/+$/, ""), token };
}

function targetSession(delivery: Delivery): string | undefined {
  const target = delivery.payload?.target ?? {};
  const value = target.session_id ?? target.sessionId ?? target.thread_id ?? target.threadId;
  return typeof value === "string" && value ? value : undefined;
}

// `serverUrl` from PluginInput is deliberately unused: it is the in-process
// server address, unreachable from the daemon, so it is never published.
export const VanthPlugin: Plugin = async ({ client, directory }) => {
  const clientId = `opencode-${process.pid}-${Math.random().toString(36).slice(2, 10)}`;
  const sessions = new Map<string, Destination>();
  let stopped = false;
  let registeredKey = "";

  const authHeaders = (connection: Connection): Record<string, string> => ({
    Authorization: `Bearer ${connection.token}`,
    "Content-Type": "application/json",
  });

  async function call<T>(connection: Connection, route: string, init: RequestInit): Promise<T> {
    const response = await fetch(`${connection.url}${route}`, {
      ...init,
      headers: { ...authHeaders(connection), ...(init.headers ?? {}) },
    });
    if (!response.ok) {
      throw new Error(`${route} -> ${response.status} ${await response.text().catch(() => "")}`.trim());
    }
    return (await response.json()) as T;
  }

  /** Re-register only when the destination set changed (the subscription row
   * persists across daemon restarts, so a steady state needs no traffic). */
  async function syncRegistration(connection: Connection, force = false): Promise<void> {
    const key = [...sessions.keys()].sort().join(",");
    if (!force && key === registeredKey) return;
    await register(connection);
    registeredKey = key;
  }

  async function register(connection: Connection): Promise<void> {
    await call(connection, "/relay/register", {
      method: "POST",
      body: JSON.stringify({
        client_id: clientId,
        client_type: "opencode_thread",
        destinations: [...sessions.values()],
      }),
    });
  }

  /** Learn the session this plugin instance lives in, and tell the daemon. */
  async function observe(sessionID?: string): Promise<void> {
    if (stopped || !sessionID || sessions.has(sessionID)) return;
    sessions.set(sessionID, { client_type: "opencode_thread", session_id: sessionID, directory });
    try {
      const connection = await readConnection();
      if (connection) await syncRegistration(connection, true);
    } catch {
      // Daemon down or not authorised yet: the poll loop re-registers on connect.
    }
  }

  async function inject(delivery: Delivery, sessionID: string): Promise<void> {
    const prompt = delivery.payload?.prompt;
    if (!prompt) throw new Error("delivery payload carries no prompt");
    const body = { parts: [{ type: "text" as const, text: prompt }] };
    if (typeof client.session.promptAsync === "function") {
      await client.session.promptAsync({ path: { id: sessionID }, body });
    } else {
      await client.session.prompt({ path: { id: sessionID }, body });
    }
    await client.tui
      .showToast({
        body: { title: "Vanth wake", message: prompt.split("\n").slice(0, 2).join(" "), variant: "info", duration: 7000 },
      })
      .catch(() => {});
  }

  async function drain(connection: Connection): Promise<void> {
    const query = `client_id=${encodeURIComponent(clientId)}&timeout_seconds=${POLL_TIMEOUT_SECONDS}`;
    const { deliveries } = await call<{ deliveries: Delivery[] }>(connection, `/relay/poll?${query}`, {
      method: "GET",
    });
    for (const delivery of deliveries) {
      const sessionID = targetSession(delivery);
      let status = "delivered";
      let error: string | undefined;
      try {
        if (!sessionID) throw new Error("delivery has no session_id");
        if (!sessions.has(sessionID)) await observe(sessionID);
        await inject(delivery, sessionID);
      } catch (failure) {
        status = "failed";
        error = failure instanceof Error ? failure.message : String(failure);
      }
      try {
        await call(connection, "/relay/ack", {
          method: "POST",
          body: JSON.stringify({
            client_id: clientId,
            delivery_id: delivery.delivery_id,
            status,
            error,
            lease_token: delivery.lease_token,
          }),
        });
      } catch {
        // The lease expires and the daemon re-offers it; never crash the loop.
      }
    }
  }

  void (async () => {
    while (!stopped) {
      const connection = await readConnection();
      if (!connection) {
        await sleep(POLL_ERROR_BACKOFF_MS);
        continue;
      }
      try {
        if (!sessions.size) {
          await sleep(NO_RELAY_BACKOFF_MS);
          continue;
        }
        await syncRegistration(connection);
        await drain(connection);
      } catch {
        // A poll failure can mean the daemon expired our subscription (restart
        // or an outage longer than the relay TTL). Forget the memoized key so
        // the next pass re-registers instead of polling an unknown client_id
        // forever — which would silently never wake anything.
        registeredKey = "";
        await sleep(POLL_ERROR_BACKOFF_MS);
      }
    }
  })();

  return {
    // `tool.execute.before` is awaited before the tool runs, so a job_start that
    // omits session_id can still be resolved by the daemon (the registration is
    // already in the table by the time the MCP call is processed).
    "tool.execute.before": async ({ sessionID }) => {
      await observe(sessionID);
    },
    event: async ({ event }) => {
      const properties = (event as { properties?: { sessionID?: unknown } }).properties;
      if (typeof properties?.sessionID === "string") await observe(properties.sessionID);
    },
    dispose: async () => {
      stopped = true;
      const connection = await readConnection();
      if (!connection) return;
      await call(connection, "/relay/unregister", {
        method: "POST",
        body: JSON.stringify({ client_id: clientId }),
      }).catch(() => {});
    },
  };
};

export default VanthPlugin;
