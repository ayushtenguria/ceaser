import { create } from "zustand";
import type { DatabaseConnection } from "@/types";

interface ConnectionsState {
  connections: DatabaseConnection[];
  activeConnectionId: string | null;
  activeConnectionIds: string[];

  setConnections: (connections: DatabaseConnection[]) => void;
  addConnection: (connection: DatabaseConnection) => void;
  removeConnection: (id: string) => void;
  setActiveConnection: (id: string | null) => void;
  setActiveConnectionIds: (ids: string[]) => void;
  toggleConnectionId: (id: string) => void;
  updateConnection: (
    id: string,
    updates: Partial<DatabaseConnection>
  ) => void;
}

export const useConnectionsStore = create<ConnectionsState>((set) => ({
  connections: [],
  activeConnectionId: null,
  activeConnectionIds: [],

  setConnections: (connections) => set({ connections }),

  addConnection: (connection) =>
    set((state) => ({
      connections: [...state.connections, connection],
    })),

  removeConnection: (id) =>
    set((state) => ({
      connections: state.connections.filter((c) => c.id !== id),
      activeConnectionId:
        state.activeConnectionId === id ? null : state.activeConnectionId,
    })),

  setActiveConnection: (id) => set({ activeConnectionId: id }),

  setActiveConnectionIds: (ids) => set({ activeConnectionIds: ids }),

  toggleConnectionId: (id) =>
    set((state) => {
      const ids = state.activeConnectionIds.includes(id)
        ? state.activeConnectionIds.filter((i) => i !== id)
        : [...state.activeConnectionIds, id];
      return { activeConnectionIds: ids, activeConnectionId: ids[0] || null };
    }),

  updateConnection: (id, updates) =>
    set((state) => ({
      connections: state.connections.map((c) =>
        c.id === id ? { ...c, ...updates } : c
      ),
    })),
}));
