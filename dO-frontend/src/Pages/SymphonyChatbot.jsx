import React, { useCallback, useEffect, useRef, useState } from "react";
import Chart from "chart.js/auto";

const API_BASE = import.meta.env.VITE_AZURE_FUNCTIONS_URL || "http://localhost:7071/api";

/* ── Chart renderer — callback ref ensures canvas is mounted before drawing ── */
const CHART_COLORS = [
  "#e74c3c", "#3498db", "#2ecc71", "#f39c12", "#9b59b6",
  "#1abc9c", "#e67e22", "#34495e", "#e91e63", "#00bcd4",
];
const CHART_COLORS_BG = CHART_COLORS.map((c) => c + "cc");

const ChartMessage = ({ config, rows, answer }) => {
  const chartInstanceRef = useRef(null);

  // callback ref — called with the DOM node when it mounts, null when it unmounts
  const canvasCallbackRef = useCallback((canvas) => {
    // cleanup previous instance
    if (chartInstanceRef.current) {
      chartInstanceRef.current.destroy();
      chartInstanceRef.current = null;
    }
    if (!canvas || !config) return;

    const effectiveRows = Array.isArray(rows) ? rows : [];
    const xKey = config.xKey || "label";

    // Resolve series keys
    const rawSeries = Array.isArray(config.series) ? config.series : [];
    let keys = rawSeries.map((s) => (typeof s === "string" ? s : s?.key)).filter(Boolean);
    // Auto-detect from first row if empty
    if (!keys.length && effectiveRows.length > 0) {
      keys = Object.keys(effectiveRows[0]).filter((k) => {
        if (k === xKey) return false;
        const v = effectiveRows[0][k];
        return typeof v === "number" || (v !== null && v !== undefined && !isNaN(Number(v)));
      });
    }

    const labels = effectiveRows.map((r) => String(r[xKey] ?? ""));
    const chartType = config.type === "line" ? "line" : config.type === "pie" ? "pie" : "bar";

    let datasets;
    if (chartType === "pie") {
      // Pie: one dataset, each slice is a different color
      const values = effectiveRows.map((r) => {
        const key = keys[0] || Object.keys(r).find((k) => k !== xKey);
        const v = key ? r[key] : 0;
        return v !== null && v !== undefined ? Number(v) : 0;
      });
      datasets = [{
        data: values,
        backgroundColor: CHART_COLORS_BG.slice(0, values.length),
        borderColor: CHART_COLORS.slice(0, values.length),
        borderWidth: 2,
      }];
    } else {
      // Bar/Line: one dataset per series key
      datasets = keys.map((key, i) => ({
        label: key,
        data: effectiveRows.map((r) => {
          const v = r[key];
          return v !== null && v !== undefined ? Number(v) : 0;
        }),
        backgroundColor: CHART_COLORS_BG[i % CHART_COLORS_BG.length],
        borderColor: CHART_COLORS[i % CHART_COLORS.length],
        borderWidth: 2,
        fill: false,
        tension: 0.3,
      }));
    }

    chartInstanceRef.current = new Chart(canvas, {
      type: chartType,
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: "top", labels: { font: { size: 12 } } },
          tooltip: { enabled: true },
        },
        scales: chartType !== "pie" ? {
          x: { ticks: { font: { size: 11 } } },
          y: { beginAtZero: true, ticks: { font: { size: 11 } } },
        } : {},
      },
    });
  }, [config, rows]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div style={{ minWidth: "300px", maxWidth: "500px" }}>
      {answer && <p style={{ marginBottom: "8px", fontWeight: 500, color: "#1e293b", fontSize: "13px" }}>{answer}</p>}
      <div style={{ position: "relative", height: "280px", width: "100%" }}>
        <canvas ref={canvasCallbackRef} />
      </div>
    </div>
  );
};

/* ── Scrollable table ── */
const TableMessage = ({ rows, columns, answer }) => {
  const cols = columns?.length > 0 ? columns : (rows?.length > 0 ? Object.keys(rows[0]) : []);
  if (!rows?.length) return <span>{answer || "No data found."}</span>;
  return (
    <div>
      {answer && <p style={{ marginBottom: "6px", fontWeight: 500, color: "#1e293b" }}>{answer}</p>}
      <div style={{ overflowX: "auto", maxHeight: "300px", overflowY: "auto", border: "1px solid #e5e7eb", borderRadius: "6px" }}>
        <table style={{ borderCollapse: "collapse", width: "100%", fontSize: "12px", minWidth: "400px" }}>
          <thead style={{ position: "sticky", top: 0, zIndex: 1 }}>
            <tr>
              {cols.map((k) => (
                <th key={k} style={{ border: "1px solid #d1d5db", padding: "7px 12px", background: "#0d3347", color: "#fff", textAlign: "left", whiteSpace: "nowrap" }}>
                  {k}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#f8fafc" }}>
                {cols.map((k, j) => (
                  <td key={j} style={{ border: "1px solid #e5e7eb", padding: "5px 12px", whiteSpace: "nowrap", color: "#374151" }}>
                    {row[k] !== null && row[k] !== undefined ? String(row[k]) : "—"}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p style={{ marginTop: "4px", fontSize: "11px", color: "#6b7280" }}>{rows.length} row(s) returned.</p>
    </div>
  );
};

/* ── Main chatbot ── */
const SymphonyChatbot = () => {
  const generateUniqueId = () => `id-${Math.random().toString(36).substr(2, 9)}`;

  const [messages, setMessages] = useState([{
    id: generateUniqueId(), sender: "bot", isWelcomeMessage: true,
    content: { type: "text", text: "Welcome to the Knowledge Sathi, it can give you information on ITR data. How can I help you today?" },
  }]);
  const [newMessage, setNewMessage] = useState("");
  const [isTyping, setIsTyping]     = useState(false);
  const [isListening, setIsListening]   = useState(false);
  const [voiceSupported]                = useState(() => "webkitSpeechRecognition" in window || "SpeechRecognition" in window);
  const recognitionRef = useRef(null);
  const chatEndRef     = useRef(null);

  const startListening = () => {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) return;
    const r = new SR();
    r.lang = "en-US"; r.interimResults = false; r.maxAlternatives = 1;
    r.onstart  = () => setIsListening(true);
    r.onresult = (e) => { setNewMessage(e.results[0][0].transcript); setIsListening(false); };
    r.onerror  = () => setIsListening(false);
    r.onend    = () => setIsListening(false);
    recognitionRef.current = r;
    r.start();
  };

  const stopListening = () => { recognitionRef.current?.stop(); setIsListening(false); };
  const toggleVoice   = () => isListening ? stopListening() : startListening();

  const handleSendMessage = async (event) => {
    event.preventDefault();
    if (!newMessage.trim()) return;

    const msgId      = generateUniqueId();
    const userText   = newMessage;
    setMessages((prev) => [...prev, { id: msgId, sender: "user", content: { type: "text", text: userText } }]);
    setNewMessage("");
    setIsTyping(true);

    try {
      const response = await fetch(`${API_BASE}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ q: userText }),
      });
      // Use text() first so we can sanitize NaN/Infinity before JSON.parse
      const raw = await response.text();
      const sanitized = raw
        .replace(/:\s*NaN\b/g, ": null")
        .replace(/:\s*Infinity\b/g, ": null")
        .replace(/:\s*-Infinity\b/g, ": null");
      let data;
      try { data = JSON.parse(sanitized); }
      catch (e) { throw new Error(`Could not parse server response: ${e.message}`); }
      if (!response.ok) throw new Error(data?.error || `Server error: ${response.status}`);
      const botId = generateUniqueId();

      let content;
      if (data.error) {
        content = { type: "text", text: data.error };
      } else if (data.type === "chart" && data.chart_config) {
        // backend sends chart rows in data.data, fallback to data.rows
        const rows = Array.isArray(data.data) && data.data.length > 0
          ? data.data
          : (Array.isArray(data.rows) ? data.rows : []);
        content = { type: "chart", config: data.chart_config, rows, answer: data.answer || "" };
      } else if (data.type === "table") {
        content = { type: "table", rows: data.rows || [], columns: data.columns || [], answer: data.answer || "" };
      } else {
        content = { type: "text", text: data.answer || "Sorry, something went wrong." };
      }

      setMessages((prev) => [...prev, {
        id: botId, sender: "bot", isWelcomeMessage: false,
        content,
        query:  data.query  || userText,
        script: data.script || "",
      }]);
    } catch (err) {
      console.error("[SymphonyChatbot] query error:", err);
      setMessages((prev) => [...prev, {
        id: generateUniqueId(), sender: "bot", isWelcomeMessage: false,
        content: { type: "text", text: `Error: ${err?.message || "Something went wrong. Please try again."}` },
      }]);
    } finally {
      setIsTyping(false);
    }
  };

  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  return (
    <div className="chat-window">
      <div className="chat-messages">
        {messages.map((message, index) => (
          <Message key={message.id} message={message} index={index} setMessages={setMessages} />
        ))}
        {isTyping && (
          <div className="chat-message bot">
            <div className="message-content" style={{ color: "#6b7280", fontStyle: "italic" }}>Thinking...</div>
          </div>
        )}
        <div ref={chatEndRef} />
      </div>

      <form
        className="chat-input"
        onSubmit={handleSendMessage}
        style={{ display: "flex", alignItems: "center", gap: "8px", padding: "10px 14px", borderTop: "1px solid #e5e7eb", background: "#fff" }}
      >
        <input
          type="text"
          value={newMessage}
          onChange={(e) => setNewMessage(e.target.value)}
          placeholder="Type a message..."
          style={{ flex: 1, padding: "10px 14px", border: "1.5px solid #e5e7eb", borderRadius: "8px", fontSize: "14px", outline: "none", background: "#f9fafb" }}
        />
        {voiceSupported && (
          <button
            type="button" onClick={toggleVoice}
            title={isListening ? "Stop listening" : "Speak your message"}
            style={{
              width: "42px", height: "42px", borderRadius: "50%", border: "none", cursor: "pointer",
              flexShrink: 0, display: "flex", alignItems: "center", justifyContent: "center",
              background: isListening ? "#0d3347" : "#e8f4f8", transition: "all 0.2s",
            }}
          >
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={isListening ? "#7ec8e3" : "#0d3347"} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/>
              <path d="M19 10v2a7 7 0 0 1-14 0v-2"/>
              <line x1="12" y1="19" x2="12" y2="23"/>
              <line x1="8" y1="23" x2="16" y2="23"/>
            </svg>
          </button>
        )}
        <button
          type="submit"
          style={{ padding: "0 20px", height: "42px", borderRadius: "8px", background: "#c0605a", color: "#fff", border: "none", fontSize: "14px", fontWeight: "600", cursor: "pointer", flexShrink: 0 }}
        >
          Send
        </button>
      </form>
    </div>
  );
};

export default SymphonyChatbot;

/* ── Message component ── */
const Message = ({ message, setMessages, index }) => {
  const [showQuery, setShowQuery]   = useState(false);
  const [showScript, setShowScript] = useState(false);
  const { content, sender, isWelcomeMessage, query, script } = message;

  const handleLike    = () => setMessages((prev) => prev.map((m, i) => i === index ? { ...m, liked: true }  : m));
  const handleDislike = () => setMessages((prev) => prev.map((m, i) => i === index ? { ...m, liked: false } : m));

  const renderContent = () => {
    if (!content) return null;
    if (content.type === "chart") return <ChartMessage config={content.config} rows={content.rows} answer={content.answer} />;
    if (content.type === "table") return <TableMessage rows={content.rows} columns={content.columns} answer={content.answer} />;
    return <span>{content.text}</span>;
  };

  const inlineBoxStyle = {
    marginTop: "6px",
    background: "#1e293b",
    color: "#e2e8f0",
    borderRadius: "6px",
    padding: "10px 14px",
    fontSize: "12px",
    fontFamily: "monospace",
    whiteSpace: "pre-wrap",
    wordBreak: "break-word",
    maxHeight: "200px",
    overflowY: "auto",
  };

  return (
    <>
      <div className={`chat-message ${sender}`}>
        <div className="message-content">{renderContent()}</div>
      </div>

      {sender === "bot" && !isWelcomeMessage && (
        <div style={{ paddingLeft: "4px", marginBottom: "8px" }}>
          {/* Like / Dislike + buttons row */}
          <div className="feedback-icons" style={{ display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
            <span className={`like-icon ${message.liked === true ? "liked" : ""}`} onClick={handleLike} style={{ cursor: "pointer" }}>
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M7 10v12"/><path d="M15 5.88 14 10h5.83a2 2 0 0 1 1.92 2.56l-2.33 8A2 2 0 0 1 17.5 22H4a2 2 0 0 1-2-2v-8a2 2 0 0 1 2-2h2.76a2 2 0 0 0 1.79-1.11L12 2a3.13 3.13 0 0 1 3 3.88Z"/>
              </svg>
            </span>
            <span className={`dislike-icon ${message.liked === false ? "disliked" : ""}`} onClick={handleDislike} style={{ cursor: "pointer" }}>
              <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M17 14V2"/><path d="M9 18.12 10 14H4.17a2 2 0 0 1-1.92-2.56l2.33-8A2 2 0 0 1 6.5 2H20a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2h-2.76a2 2 0 0 0-1.79 1.11L12 22a3.13 3.13 0 0 1-3-3.88Z"/>
              </svg>
            </span>

            <button
              className="button button4 show-query-button"
              onClick={() => { setShowQuery((v) => !v); setShowScript(false); }}
            >
              {showQuery ? "Hide Query" : "Show Query"}
            </button>
            <button
              className="button button4 show-script-button"
              onClick={() => { setShowScript((v) => !v); setShowQuery(false); }}
            >
              {showScript ? "Hide Script" : "Show Script"}
            </button>
          </div>

          {/* Inline expandable query */}
          {showQuery && (
            <div style={inlineBoxStyle}>{query || "(no query)"}</div>
          )}

          {/* Inline expandable script */}
          {showScript && (
            <div style={inlineBoxStyle}>{script || "(no script)"}</div>
          )}
        </div>
      )}
    </>
  );
};
