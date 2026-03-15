import { create } from "zustand";
import type { Conversation, Message } from "@/types";

interface ChatState {
  conversations: Conversation[];
  activeConversationId: string | null;
  messages: Record<string, Message[]>;
  selectedModel: "gemini" | "claude";

  setConversations: (conversations: Conversation[]) => void;
  addConversation: (conversation: Conversation) => void;
  setActiveConversation: (id: string | null) => void;
  setMessages: (conversationId: string, messages: Message[]) => void;
  addMessage: (conversationId: string, message: Message) => void;
  updateMessage: (
    conversationId: string,
    messageId: string,
    updates: Partial<Message>
  ) => void;
  removeConversation: (id: string) => void;
  setSelectedModel: (model: "gemini" | "claude") => void;
}

export const useChatStore = create<ChatState>((set) => ({
  conversations: [],
  activeConversationId: null,
  messages: {},
  selectedModel: "gemini",

  setConversations: (conversations) => set({ conversations }),

  addConversation: (conversation) =>
    set((state) => ({
      conversations: [conversation, ...state.conversations],
    })),

  setActiveConversation: (id) => set({ activeConversationId: id }),

  setMessages: (conversationId, messages) =>
    set((state) => ({
      messages: { ...state.messages, [conversationId]: messages },
    })),

  addMessage: (conversationId, message) =>
    set((state) => ({
      messages: {
        ...state.messages,
        [conversationId]: [
          ...(state.messages[conversationId] || []),
          message,
        ],
      },
    })),

  updateMessage: (conversationId, messageId, updates) =>
    set((state) => {
      const conversationMessages = state.messages[conversationId] || [];
      return {
        messages: {
          ...state.messages,
          [conversationId]: conversationMessages.map((msg) =>
            msg.id === messageId ? { ...msg, ...updates } : msg
          ),
        },
      };
    }),

  removeConversation: (id) =>
    set((state) => {
      const { [id]: _, ...restMessages } = state.messages;
      return {
        conversations: state.conversations.filter((c) => c.id !== id),
        messages: restMessages,
        activeConversationId:
          state.activeConversationId === id ? null : state.activeConversationId,
      };
    }),

  setSelectedModel: (model) => set({ selectedModel: model }),
}));
