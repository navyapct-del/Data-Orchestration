import React, { useState, useRef, useEffect, useCallback } from "react";
import { LoadingIcon } from "@/base-components";
import Chart from "chart.js/auto";

const API_BASE = import.meta.env.VITE_AZURE_FUNCTIONS_URL || "http://localhost:7071/api";

/* ── Inline chart component using callback ref ── */
const CHART_COLORS = ["#e74c3c","#3498db","#2ecc71","#f39c12","#9b59b6","#1abc9c","#e67e22","#34495e","#e91e63","#00bcd4"];

const ChartBubble = ({ config, rows, answer }) => {
  const instanceRef = useRef(null);

  const canvasCallbackRef = useCallback((canvas) => {
    if (instanceRef.current) { instanceRef.current.destroy(); instanceRef.current = null; }
    if (!canvas || !config) return;
    const effectiveRows = Array.isArray(rows) ? rows : [];
    const xKey = config.xKey || "label";
    const rawSeries = Array.isArray(config.series) ? config.series : [];
    let keys = rawSeries.map((s) => typeof s === "string" ? s : s?.key).filter(Boolean);
    if (!keys.length && effectiveRows.length > 0) {
      keys = Object.keys(effectiveRows[0]).filter((k) => k !== xKey && !isNaN(Number(effectiveRows[0][k])));
    }
    const labels = effectiveRows.map((r) => String(r[xKey] ?? ""));
    const chartType = config.type === "line" ? "line" : config.type === "pie" ? "pie" : "bar";
    const bgColors = CHART_COLORS.map((c) => c + "cc");
    const datasets = chartType === "pie"
      ? [{ data: effectiveRows.map((r) => Number(r[keys[0]] ?? 0)), backgroundColor: bgColors.slice(0, effectiveRows.length), borderColor: CHART_COLORS.slice(0, effectiveRows.length), borderWidth: 2 }]
      : keys.map((key, i) => ({ label: key, data: effectiveRows.map((r) => Number(r[key] ?? 0)), backgroundColor: bgColors[i % bgColors.length], borderColor: CHART_COLORS[i % CHART_COLORS.length], borderWidth: 2, fill: false, tension: 0.3 }));
    instanceRef.current = new Chart(canvas, {
      type: chartType,
      data: { labels, datasets },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: "top" } }, scales: chartType !== "pie" ? { y: { beginAtZero: true } } : {} },
    });
  }, [config, rows]); // eslint-disable-line

  return (
    <div>
      {answer && <p style={{ marginBottom: "6px", fontWeight: 500 }}>{answer}</p>}
      <div style={{ position: "relative", height: "240px", width: "100%" }}>
        <canvas ref={canvasCallbackRef} />
      </div>
    </div>
  );
};

export default function SingleFileSathi() {
  const [file, setFile]           = useState(null);
  const [uploading, setUploading] = useState(false);
  const [fileReady, setFileReady] = useState(false);
  const [s3Key, setS3Key]         = useState("");
  const [uploadErr, setUploadErr] = useState("");
  const [messages, setMessages]   = useState([
    { role: "bot", text: "Welcome to Files Knowledge Bot, How can I help you today?" },
  ]);
  const [input, setInput]         = useState("");
  const [thinking, setThinking]   = useState(false);

  const fileRef    = useRef();
  const chatEndRef = useRef();

  /* ── pick & auto-upload file ── */
  const pickFile = async (f) => {
    if (!f) return;
    setFile(f);
    setFileReady(false);
    setUploadErr("");
    setUploading(true);
    try {
      const formData = new FormData();
      formData.append("file", f);
      formData.append("filename", f.name);

      const res = await fetch(`${API_BASE}/upload`, {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || "Upload failed");
      }

      const data = await res.json();
      // Store the filename so we can filter queries to this file
      setS3Key(data.filename || f.name);
      setFileReady(true);
      setMessages((prev) => [
        ...prev,
        { role: "bot", text: `✓ "${f.name}" uploaded successfully. Ask me anything about it!` },
      ]);
    } catch (err) {
      setUploadErr(err.message || "Upload failed. Please try again.");
    }
    setUploading(false);
  };

  /* ── send prompt ── */
  const sendPrompt = async () => {
    const text = input.trim();
    if (!text || !fileReady) return;
    setMessages((prev) => [...prev, { role: "user", text }]);
    setInput("");
    setThinking(true);
    try {
      const res = await fetch(`${API_BASE}/query`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ q: text, filename: s3Key }),
      });
      const raw = await res.text();
      const sanitized = raw.replace(/:\s*NaN\b/g, ": null").replace(/:\s*Infinity\b/g, ": null").replace(/:\s*-Infinity\b/g, ": null");
      const data = JSON.parse(sanitized);

      // Build a rich message object based on response type
      let msg;
      if (data.type === "table" && data.rows?.length > 0) {
        msg = { role: "bot", dataType: "table", rows: data.rows, columns: data.columns || [], answer: data.answer || "" };
      } else if (data.type === "chart" && data.chart_config) {
        const rows = Array.isArray(data.data) ? data.data : (data.rows || []);
        msg = { role: "bot", dataType: "chart", chartConfig: data.chart_config, rows, answer: data.answer || "" };
      } else {
        msg = { role: "bot", dataType: "text", text: data.answer || "No response received." };
      }
      setMessages((prev) => [...prev, msg]);
    } catch (err) {
      setMessages((prev) => [...prev, { role: "bot", dataType: "text", text: `Error: ${err?.message || "Please try again."}` }]);
    }
    setThinking(false);
    setTimeout(() => chatEndRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
  };

  /* ── feedback (no-op — Azure backend has no feedback endpoint) ── */
  const sendFeedback = async (_index, _like) => {};

  return (
    <div style={s.page}>
      {/* Chat window */}
      <div style={s.chatCard}>

        {/* Messages area */}
        <div style={s.messages}>
          {messages.map((m, i) => (
            <div key={i} style={{ marginBottom: "16px" }}>
              {m.role === "bot" ? (
                <div>
                  <div style={{ ...s.botBubble, maxWidth: m.dataType === "table" || m.dataType === "chart" ? "95%" : "70%" }}>
                    {m.dataType === "table" ? (
                      <div>
                        {m.answer && <p style={{ marginBottom: "6px", fontWeight: 500 }}>{m.answer}</p>}
                        <div style={{ overflowX: "auto", maxHeight: "280px", overflowY: "auto" }}>
                          <table style={{ borderCollapse: "collapse", width: "100%", fontSize: "12px" }}>
                            <thead>
                              <tr>{(m.columns?.length > 0 ? m.columns : Object.keys(m.rows[0])).map((k) => (
                                <th key={k} style={{ border: "1px solid #d1d5db", padding: "6px 10px", background: "#0d3347", color: "#fff", whiteSpace: "nowrap", textAlign: "left" }}>{k}</th>
                              ))}</tr>
                            </thead>
                            <tbody>
                              {m.rows.map((row, ri) => {
                                const cols = m.columns?.length > 0 ? m.columns : Object.keys(row);
                                return (
                                  <tr key={ri} style={{ background: ri % 2 === 0 ? "#fff" : "#f8fafc" }}>
                                    {cols.map((k, ci) => (
                                      <td key={ci} style={{ border: "1px solid #e5e7eb", padding: "5px 10px", whiteSpace: "nowrap" }}>
                                        {row[k] !== null && row[k] !== undefined ? String(row[k]) : "—"}
                                      </td>
                                    ))}
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                        <p style={{ marginTop: "4px", fontSize: "11px", color: "#6b7280" }}>{m.rows.length} row(s)</p>
                      </div>
                    ) : m.dataType === "chart" ? (
                      <ChartBubble config={m.chartConfig} rows={m.rows} answer={m.answer} />
                    ) : (
                      m.text
                    )}
                  </div>
                </div>
              ) : (
                <div style={s.userRow}>
                  <div style={s.userBubble}>{m.text}</div>
                </div>
              )}
            </div>
          ))}

          {/* Uploading indicator */}
          {uploading && (
            <div style={s.botBubble}>
              <span style={{ display: "flex", alignItems: "center", gap: "8px", color: "#6b7280" }}>
                <LoadingIcon icon="three-dots" className="w-8 h-4" />
                Uploading "{file?.name}"…
              </span>
            </div>
          )}

          {/* Thinking indicator */}
          {thinking && (
            <div style={s.botBubble}>
              <LoadingIcon icon="three-dots" className="w-8 h-4" />
            </div>
          )}

          {uploadErr && (
            <div style={{ ...s.botBubble, color: "#dc2626", borderColor: "#fca5a5", background: "#fff5f5" }}>
              {uploadErr}
            </div>
          )}

          <div ref={chatEndRef} />
        </div>

        {/* Input bar */}
        <div style={s.inputBar}>
          {/* File chip shown when a file is attached */}
          {file && (
            <div style={s.fileChip}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#1a6b8a" strokeWidth="2">
                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                <polyline points="14 2 14 8 20 8"/>
              </svg>
              <span style={s.chipName}>{file.name}</span>
              {fileReady && <span style={s.chipReady}>✓</span>}
              <button
                style={s.chipRemove}
                onClick={() => { setFile(null); setFileReady(false); setS3Key(""); }}
              >✕</button>
            </div>
          )}

          <div style={s.inputRow}>
            <input
              style={s.input}
              placeholder={fileReady ? "Type a message..." : "Attach a file to start chatting"}
              value={input}
              disabled={!fileReady || thinking || uploading}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && sendPrompt()}
            />

            {/* Paperclip */}
            <button
              style={s.clipBtn}
              title="Attach file"
              onClick={() => fileRef.current.click()}
              disabled={uploading}
            >
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/>
              </svg>
            </button>
            <input ref={fileRef} type="file" style={{ display: "none" }} onChange={(e) => pickFile(e.target.files[0])} />

            {/* Send */}
            <button
              style={{
                ...s.sendBtn,
                opacity: fileReady && input.trim() && !thinking ? 1 : 0.5,
                cursor: fileReady && input.trim() && !thinking ? "pointer" : "default",
              }}
              disabled={!fileReady || !input.trim() || thinking}
              onClick={sendPrompt}
            >
              Send
            </button>
          </div>
        </div>

      </div>
    </div>
  );
}

const s = {
  page: {
    fontFamily: "'Segoe UI', system-ui, sans-serif",
    height: "calc(100vh - 120px)",
    display: "flex",
    flexDirection: "column",
  },
  chatCard: {
    flex: 1,
    background: "#ffffff",
    border: "1.5px solid #e5e7eb",
    borderRadius: "16px",
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
  },

  /* Messages */
  messages: {
    flex: 1,
    padding: "24px",
    overflowY: "auto",
    background: "#f3f4f6",
    display: "flex",
    flexDirection: "column",
  },
  botBubble: {
    display: "inline-block",
    background: "#ffffff",
    border: "1px solid #e5e7eb",
    borderRadius: "0 12px 12px 12px",
    padding: "12px 16px",
    fontSize: "14px",
    color: "#1f2937",
    maxWidth: "70%",
    lineHeight: "1.6",
    boxShadow: "0 1px 3px rgba(0,0,0,0.06)",
  },
  userRow: {
    display: "flex",
    justifyContent: "flex-end",
  },
  userBubble: {
    display: "inline-block",
    background: "#0d3347",
    borderRadius: "12px 0 12px 12px",
    padding: "12px 16px",
    fontSize: "14px",
    color: "#ffffff",
    maxWidth: "70%",
    lineHeight: "1.6",
  },
  thumbRow: {
    display: "flex",
    gap: "6px",
    marginTop: "6px",
    paddingLeft: "4px",
  },
  thumbBtn: {
    background: "none",
    border: "none",
    cursor: "pointer",
    padding: "2px",
    display: "flex",
    alignItems: "center",
    transition: "color 0.15s",
  },

  /* Input bar */
  inputBar: {
    background: "#ffffff",
    borderTop: "1px solid #e5e7eb",
    padding: "12px 16px",
    display: "flex",
    flexDirection: "column",
    gap: "8px",
  },
  fileChip: {
    display: "inline-flex",
    alignItems: "center",
    gap: "6px",
    background: "#f0f7fa",
    border: "1px solid #bee3f8",
    borderRadius: "20px",
    padding: "4px 10px",
    fontSize: "12px",
    color: "#1a6b8a",
    alignSelf: "flex-start",
  },
  chipName: {
    maxWidth: "200px",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap",
    fontSize: "12px",
  },
  chipReady: {
    fontSize: "11px",
    fontWeight: "700",
    color: "#059669",
  },
  chipRemove: {
    background: "none",
    border: "none",
    cursor: "pointer",
    color: "#9ca3af",
    fontSize: "12px",
    padding: "0 2px",
    lineHeight: 1,
  },
  inputRow: {
    display: "flex",
    alignItems: "center",
    gap: "10px",
  },
  input: {
    flex: 1,
    padding: "13px 16px",
    fontSize: "14px",
    border: "1.5px solid #e5e7eb",
    borderRadius: "10px",
    outline: "none",
    background: "#f9fafb",
    color: "#1f2937",
    fontFamily: "inherit",
  },
  clipBtn: {
    width: "42px",
    height: "42px",
    borderRadius: "10px",
    background: "transparent",
    border: "1.5px solid #e5e7eb",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    cursor: "pointer",
    color: "#6b7280",
    flexShrink: 0,
    transition: "border-color 0.15s",
  },
  sendBtn: {
    padding: "0 22px",
    height: "42px",
    borderRadius: "10px",
    background: "#c0605a",
    color: "#ffffff",
    border: "none",
    fontSize: "14px",
    fontWeight: "600",
    cursor: "pointer",
    flexShrink: 0,
    transition: "opacity 0.2s",
  },
};
