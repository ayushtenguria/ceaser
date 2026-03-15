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

  // Load conversations list
  useEffect(() => {
    let cancelled = false;

    async function loadConversations() {
      try {
        const conversations = await api.getConversations();
        if (!cancelled) {
          setConversations(conversations);
        }
      } catch {
        // Silent fail; sidebar will show empty state
      }
    }

    loadConversations();
    return () => {
      cancelled = true;
    };
  }, [setConversations]);

  // Load specific conversation if ID provided
  useEffect(() => {
    if (!conversationId) {
      setActiveConversation(null);
      return;
    }

    let cancelled = false;

    setActiveConversation(conversationId);

    async function loadMessages() {
      try {
        const messages = await api.getMessages(conversationId!);
        if (!cancelled) {
          setMessages(conversationId!, messages);
        }
      } catch {
        // Silent fail
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
