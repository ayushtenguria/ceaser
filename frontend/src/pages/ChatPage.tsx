import { useEffect } from "react";
import { useParams } from "react-router-dom";
import ChatInterface from "@/components/chat/ChatInterface";
import { useChatStore } from "@/store/chat";
import * as api from "@/lib/api";

export default function ChatPage() {
  const { conversationId } = useParams<{ conversationId: string }>();
  const {
    setActiveConversation,
    setMessages,
    setConversations,
  } = useChatStore();

  useEffect(() => {
    let cancelled = false;

    async function loadConversations() {
      try {
        const conversations = await api.getConversations();
        if (!cancelled) {
          setConversations(conversations);
        }
      } catch {
      }
    }

    loadConversations();
    return () => {
      cancelled = true;
    };
  }, [setConversations]);

  useEffect(() => {
    if (!conversationId) {
      setActiveConversation(null);
      return;
    }

    let cancelled = false;

    setActiveConversation(conversationId);

    // If we already have messages for this conversation in the store
    // (e.g. from an active streaming session), skip the API fetch
    // to avoid overwriting in-flight messages.
    const existing = useChatStore.getState().messages[conversationId];
    if (existing && existing.length > 0) return;

    async function loadMessages() {
      try {
        const messages = await api.getMessages(conversationId!);
        if (!cancelled) {
          setMessages(conversationId!, messages);
        }
      } catch {
      }
    }

    loadMessages();
    return () => {
      cancelled = true;
    };
  }, [conversationId, setActiveConversation, setMessages]);

  return (
    <div className="h-full">
      <ChatInterface />
    </div>
  );
}
