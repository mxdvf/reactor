export interface NodeData {
  actors: string[];
  loaded_libs: string[];
  error: string | null;
  latencyMs: number | null;
  lastUpdated: string | null;
}

export interface Node {
  hostname: string;
  url: string;
  data: NodeData;
}
