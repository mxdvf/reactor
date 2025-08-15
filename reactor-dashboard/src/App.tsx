import { useEffect, useMemo, useRef, useState, useCallback } from "react";
import './App.css'
import type { Node, NodeData } from "@/types";
import {
  SidebarInset,
  SidebarProvider,
} from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import Header from "@/components/header"
import NodeCard from "@/components/node-card"
import { Badge } from "@/components/ui/badge";
import fetchWithTimeout from "@/utils";
import { ThemeProvider } from "@/components/theme-provider"

const POLL_MS: number = Number(import.meta.env.VITE_POLL_MS ?? 5000);
const FETCH_TIMEOUT_MS: number = Number(import.meta.env.VITE_FETCH_TIMEOUT_MS ?? 4000);

function App() {
  const [pollingEnabled, setPollingEnabled] = useState<boolean>(true);
  const [nodes, setNodes] = useState<Node[]>([]);
  const nodesRef = useRef<Node[]>(nodes);
  const [lastSweep, setLastSweep] = useState<string | null>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);
  useEffect(() => {
    nodesRef.current = nodes;
  }, [nodes]);


  const handleAddNode = () => {
    const hostname = prompt("Enter node hostname (e.g. localhost):");
    if (!hostname || hostname.trim() === "") {
      alert("Hostname cannot be empty.");
      return;
    }

    const port = prompt("Enter node port (e.g. 3000):");
    if (!port || isNaN(Number(port)) || Number(port) < 1 || Number(port) > 65535) {
      alert("Please enter a valid port number (1–65535).");
      return;
    }

    const url = `http://${hostname}:${port}/status`;

    const newNode: Node = {
      hostname,
      url,
      data: {
        actors: [],
        loaded_libs: [],
        error: "Not polled yet",
        latencyMs: null,
        lastUpdated: null,
      },
    };

    setNodes((prev) => [...prev, newNode]);
  };

  const handleDeleteNode = (url: string) => {
    setNodes((prev) => prev.filter((n) => n.url !== url));
  };

  const pollOnce = async () => {
    const updatedNodes = await Promise.all(
      nodesRef.current.map(async (node) => {
        const res = await fetchWithTimeout(node.url, FETCH_TIMEOUT_MS);
        const newData: NodeData = res.ok
          ? {
              actors: Array.isArray(res.json.actors) ? res.json.actors : [],
              loaded_libs: Array.isArray(res.json.loaded_libs) ? res.json.loaded_libs : [],
              error: null,
              latencyMs: res.latencyMs,
              lastUpdated: new Date().toISOString(),
            }
          : {
              actors: [],
              loaded_libs: [],
              error: res.error,
              latencyMs: null,
              lastUpdated: new Date().toISOString(),
            };

        return { ...node, data: newData };
      })
    );

    setNodes(updatedNodes);
    setLastSweep(new Date().toISOString());
  };

  useEffect(() => {
    if (!pollingEnabled) return;
    pollOnce();
    timerRef.current = setInterval(pollOnce, POLL_MS);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [pollingEnabled]);

  const totals = useMemo(() => {
    let up = 0;
    let down = 0;

    nodes.forEach((node) => {
      if (!node.data || node.data.error) down++;
      else up++;
    });

    return { up, down, total: nodes.length };
  }, [nodes]);

  const totalActors = useMemo(
    () =>
      nodes.reduce((sum, node) => sum + (node.data?.actors.length || 0), 0),
    [nodes]
  );

  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <SidebarProvider
        style={
          {
            "--sidebar-width": "calc(var(--spacing) * 72)",
            "--header-height": "calc(var(--spacing) * 12)",
          } as React.CSSProperties
        }
      >
        <AppSidebar variant="inset" />
        <SidebarInset>
          <Header
            pollingEnabled={pollingEnabled}
            setPollingEnabled={setPollingEnabled}
            handleAddNode={handleAddNode}
            pollOnce={pollOnce}
            POLL_MS={POLL_MS}
            lastSweep={lastSweep}
          />
          <div className="flex flex-1 flex-col">
            <div className="@container/main flex flex-1 flex-col gap-2">
              
              <div className="m-4 flex flex-wrap items-center gap-2 text-sm">
                <Badge>Online: {totals.up}</Badge>
                <Badge variant="destructive">Offline: {totals.down}</Badge>
                <Badge variant="secondary">Total: {totals.total}</Badge>
                <Badge variant="secondary">Total Actors: {totalActors}</Badge>
              </div>

              {nodes.map((node) => (
                <NodeCard
                  key={node.url} // key directly on NodeCard
                  node={node}
                  handleDeleteNode={handleDeleteNode}
                />
              ))}
            </div>
          </div>
        </SidebarInset>
      </SidebarProvider>
      {/* Header */}

      {/* Main content 
      <main className="mx-auto max-w-6xl px-4 py-6">
        <div className="mb-4 flex flex-wrap items-center gap-2 text-sm">
          <Badge variant>Online: {totals.up}</Badge>
          <Badge variant="destructive">Offline: {totals.down}</Badge>
          <Badge variant="secondary">Total: {totals.total}</Badge>
          <Badge variant="secondary">Total Actors: {totalActors}</Badge>
        </div>

        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {nodes.map((node) => (
            <NodeCard
              key={node.url} // key directly on NodeCard
              node={node}
              handleDeleteNode={handleDeleteNode}
            />
          ))}
        </div>
      </main>*/}
    </ThemeProvider>
  )
}


export default App
