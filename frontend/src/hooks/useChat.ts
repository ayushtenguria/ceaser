import { useState, useCallback } from "react";
import { useChatStore } from "@/store/chat";
import { useConnectionsStore } from "@/store/connections";
import * as api from "@/lib/api";
import type { Message, PlotlyFigure, TableData, StreamChunk } from "@/types";

export function useChat() {
  const {
    conversations,
    activeConversationId,
    messages,
    selectedModel,
    addMessage,
    updateMessage,
    addConversation,
    setActiveConversation,
  } = useChatStore();

  const { activeConnectionId } = useConnectionsStore();
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const currentMessages = activeConversationId
    ? messages[activeConversationId] || []
    : [];

  const sendMessage = useCallback(
    async (content: string, fileId?: string) => {
      setError(null);
      let conversationId = activeConversationId;

      // Use a temp conversation ID if none yet — backend creates conversations automatically
      const tempConvId = conversationId || `temp-${Date.now()}`;

      // Add user message to store
      const userMessage: Message = {
        id: `temp-user-${Date.now()}`,
        conversationId: tempConvId,
        role: "user",
        content,
        messageType: "text",
        createdAt: new Date().toISOString(),
      };
      addMessage(tempConvId, userMessage);

      // Create placeholder assistant message
      const assistantMessageId = `temp-assistant-${Date.now()}`;
      const assistantMessage: Message = {
        id: assistantMessageId,
        conversationId: tempConvId,
        role: "assistant",
        content: "",
        messageType: "text",
        createdAt: new Date().toISOString(),
      };
      addMessage(tempConvId, assistantMessage);

      setIsStreaming(true);

      try {
        const stream = api.sendMessage({
          message: content,
          conversationId: conversationId || undefined,
          connectionId: activeConnectionId || undefined,
          fileId,
          model: selectedModel,
        });

        let currentConvId = tempConvId;
        let accumulatedContent = "";
        let sqlQuery: string | undefined;
        let codeBlock: string | undefined;
        let plotlyFigure: PlotlyFigure | undefined;
        let tableData: TableData | undefined;
        let messageType: Message["messageType"] = "text";

        for await (const chunk of stream) {
          switch (chunk.type) {
            case "conversation_id":
              // Backend created a new conversation
              if (!conversationId) {
                currentConvId = chunk.content;
                // Move messages from temp to real conversation ID
                const existingMsgs = useChatStore.getState().messages[tempConvId] || [];
                useChatStore.getState().setMessages(currentConvId, existingMsgs);
                setActiveConversation(currentConvId);
                conversationId = currentConvId;
                // Add to conversations list
                addConversation({
                  id: currentConvId,
                  title: content.slice(0, 80),
                  connectionId: activeConnectionId,
                  createdAt: new Date().toISOString(),
                  updatedAt: new Date().toISOString(),
                });
              }
              break;
            case "text":
              accumulatedContent += chunk.content;
              break;
            case "sql":
              sqlQuery = chunk.content;
              messageType = "sql_result";
              break;
            case "code":
              codeBlock = chunk.content;
              messageType = "code_execution";
              break;
            case "table":
              tableData = chunk.data as TableData;
              messageType = "sql_result";
              break;
            case "chart":
              plotlyFigure = chunk.data as PlotlyFigure;
              messageType = "visualization";
              break;
            case "error":
              accumulatedContent += chunk.content;
              messageType = "error";
              break;
            case "status":
              accumulatedContent = chunk.content;
              break;
            case "done":
              break;
          }

          updateMessage(currentConvId, assistantMessageId, {
            content: accumulatedContent,
            messageType,
            sqlQuery,
            codeBlock,
            plotlyFigure,
            tableData,
            error: messageType === "error" ? accumulatedContent : undefined,
          });
        }
      } catch (err) {
        const errorMessage =
          err instanceof Error ? err.message : "An unexpected error occurred";
        setError(errorMessage);
        updateMessage(tempConvId, assistantMessageId, {
          content: errorMessage,
          messageType: "error",
          error: errorMessage,
        });
      } finally {
        setIsStreaming(false);
      }
    },
    [
      activeConversationId,
      activeConnectionId,
      selectedModel,
      addMessage,
      updateMessage,
      addConversation,
      setActiveConversation,
    ]
  );

  return {
    messages: currentMessages,
    conversations,
    activeConversationId,
    isStreaming,
    error,
    sendMessage,
  };
}
