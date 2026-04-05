import { useState, useCallback, useEffect } from "react";
import { useNavigate } from "react-router-dom";
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

  const navigate = useNavigate();
  const { activeConnectionId, activeConnectionIds } = useConnectionsStore();
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pendingConvId, setPendingConvId] = useState<string | null>(null);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [streamStatus, setStreamStatus] = useState<string>("");

  const effectiveConvId = activeConversationId || pendingConvId;
  const currentMessages = effectiveConvId
    ? messages[effectiveConvId] || []
    : [];

  useEffect(() => {
    if (!activeConversationId || isStreaming) return;
    const msgs = messages[activeConversationId];
    if (msgs && msgs.length > 0) {
      api.getSuggestions(activeConnectionId || undefined, activeConversationId)
        .then(setSuggestions)
        .catch(() => setSuggestions([]));
    } else {
      setSuggestions([]);
    }
  }, [activeConversationId, activeConnectionId, messages]);

  const sendMessage = useCallback(
    async (content: string, fileId?: string, fileIds?: string[]) => {
      setError(null);
      let conversationId = activeConversationId;

      const tempConvId = conversationId || `temp-${crypto.randomUUID()}`;
      if (!conversationId) {
        setPendingConvId(tempConvId);
      }

      const userMessage: Message = {
        id: `temp-user-${crypto.randomUUID()}`,
        conversationId: tempConvId,
        role: "user",
        content,
        messageType: "text",
        createdAt: new Date().toISOString(),
      };
      addMessage(tempConvId, userMessage);

      const assistantMessageId = `temp-assistant-${crypto.randomUUID()}`;
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
          connectionIds: activeConnectionIds.length > 1 ? activeConnectionIds : undefined,
          fileId,
          fileIds,
          model: selectedModel,
        });

        let currentConvId = tempConvId;
        let accumulatedContent = "";
        let sqlQuery: string | undefined;
        let codeBlock: string | undefined;
        let plotlyFigure: PlotlyFigure | undefined;
        let tableData: TableData | undefined;
        const plotlyFigures: PlotlyFigure[] = [];
        const tableDatas: TableData[] = [];
        let messageType: Message["messageType"] = "text";
        let queryReasoning: string | undefined;
        let confidence: string | undefined;

        for await (const chunk of stream) {
          switch (chunk.type) {
            case "conversation_id":
              if (!conversationId) {
                currentConvId = chunk.content;
                const existingMsgs = useChatStore.getState().messages[tempConvId] || [];
                useChatStore.getState().setMessages(currentConvId, existingMsgs);
                setPendingConvId(null);
                conversationId = currentConvId;
                addConversation({
                  id: currentConvId,
                  title: content.slice(0, 80),
                  connectionId: activeConnectionId,
                  createdAt: new Date().toISOString(),
                  updatedAt: new Date().toISOString(),
                });
                // Navigate to the new conversation URL so ChatPage picks up
                // the conversationId from params and syncs the store correctly.
                navigate(`/chat/${currentConvId}`, { replace: true });
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
            case "table": {
              const td = chunk.data as TableData;
              tableData = td;
              tableDatas.push(td);
              messageType = "sql_result";
              break;
            }
            case "chart": {
              const pf = chunk.data as PlotlyFigure;
              plotlyFigure = pf;
              plotlyFigures.push(pf);
              messageType = "visualization";
              break;
            }
            case "error":
              if (!accumulatedContent) {
                accumulatedContent = chunk.content;
              }
              messageType = "error";
              break;
            case "suggestions": {
              const s = chunk.data as string[];
              if (Array.isArray(s) && s.length > 0) {
                setSuggestions(s);
              }
              break;
            }
            case "reasoning":
              queryReasoning = chunk.content;
              break;
            case "confidence":
              confidence = chunk.content;
              break;
            case "verified":
              confidence = "high";
              break;
            case "metric_card": {
              const metricData = chunk.data;
              updateMessage(currentConvId, assistantMessageId, {
                metricCard: metricData,
              });
              break;
            }
            case "disambiguation": {
              const disambigData = chunk.data;
              messageType = "text";
              accumulatedContent = disambigData?.message || "I need clarification before proceeding.";
              // Store disambiguation data on the message for the UI to render
              updateMessage(currentConvId, assistantMessageId, {
                content: accumulatedContent,
                messageType: "text",
                disambiguationData: disambigData,
              });
              break;
            }
            case "status":
              setStreamStatus(chunk.content || "");
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
            plotlyFigures: plotlyFigures.length > 1 ? [...plotlyFigures] : undefined,
            tableDatas: tableDatas.length > 1 ? [...tableDatas] : undefined,
            queryReasoning,
            confidence,
            error: messageType === "error" ? accumulatedContent : undefined,
          });
        }
      } catch (err) {
        let errorMessage =
          err instanceof Error ? err.message : "An unexpected error occurred";

        if (errorMessage.includes("429") || errorMessage.includes("limit reached") || errorMessage.includes("too many")) {
          errorMessage = "You've reached your plan limit. Visit Usage & Plan to upgrade.";
        } else if (errorMessage.includes("402") || errorMessage.includes("Upgrade")) {
          errorMessage = "This feature requires a plan upgrade. Visit Usage & Plan to see options.";
        }
        setError(errorMessage);
        updateMessage(tempConvId, assistantMessageId, {
          content: errorMessage,
          messageType: "error",
          error: errorMessage,
        });
      } finally {
        setIsStreaming(false);
        setPendingConvId(null);
        setStreamStatus("");
      }
    },
    [
      activeConversationId,
      activeConnectionId,
      activeConnectionIds,
      selectedModel,
      addMessage,
      updateMessage,
      addConversation,
      setActiveConversation,
      navigate,
    ]
  );

  return {
    messages: currentMessages,
    conversations,
    activeConversationId,
    isStreaming,
    streamStatus,
    error,
    sendMessage,
    suggestions,
  };
}
