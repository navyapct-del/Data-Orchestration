/**
 * ApiCall.jsx — all API calls now point to the Azure Functions backend.
 * Base URL is read from VITE_AZURE_FUNCTIONS_URL (set in .env).
 */

const API_BASE = import.meta.env.VITE_AZURE_FUNCTIONS_URL || "http://localhost:7071/api";

// ── Documents ────────────────────────────────────────────────────────────────

/**
 * List all uploaded documents from Azure Table Storage.
 * Replaces the old AWS `listObjects` / `pagination` action.
 * Returns an array of document objects:
 *   { name, filename, blob_url, summary, tags, description, date, type, size, status }
 */
export const listObjects = async (_type, _currentLoc) => {
  const response = await fetch(`${API_BASE}/documents`);
  if (!response.ok) throw new Error(`listObjects failed: ${response.status}`);
  const docs = await response.json();
  console.log("[ApiCall] /documents raw response:", docs);

  // Normalise to the shape the rest of the UI expects
  return docs.map((doc) => {
    const ext = (doc.filename || "").split(".").pop().toLowerCase();
    // Cards.jsx checks name.split("/").slice(1)[0] for type routing,
    // so we prefix with a fake path segment matching the file type
    const typeSegment = ["jpg","jpeg","png","gif","webp"].includes(ext)
      ? "image"
      : ["mp4","mov","avi","mkv","m3u8"].includes(ext)
      ? "video"
      : "document";
    const prefixedName = `files/${typeSegment}/${doc.filename || ""}`;

    return {
      id:          doc.id || "",
      name:        prefixedName,
      filename:    doc.filename || "",
      blob_url:    doc.blob_url || "",
      summary:     doc.summary  || "",
      tags:        doc.tags     || "",
      description: doc.summary  || "",
      date:        doc.created_at || "",
      type:        typeSegment,
      size:        "",
      status:      doc.status || "",
    };
  });
};

/**
 * Fetch tags from documents (used by TagsFilter / AdvanceSearchTags).
 * Replaces the old AWS `tags` action — we derive tags from the documents list.
 */
export const fetchTagsData = async (_type, _currentLoc, _checkboxes) => {
  const docs = await listObjects();
  const tagSet = new Set();
  docs.forEach((doc) => {
    if (doc.tags) {
      doc.tags.split(",").forEach((t) => {
        const trimmed = t.trim();
        if (trimmed) tagSet.add(trimmed);
      });
    }
  });
  return Array.from(tagSet);
};

// ── Upload ───────────────────────────────────────────────────────────────────

/**
 * Upload one or more files to the Azure backend.
 * Replaces the old AWS presigned-URL + S3 PUT + DynamoDB flow.
 * Each field: { selectedFile: FileList, description: string, manualtags: string[] }
 */
export const getUploadData = async (inputFields, _userEmail, _currentFolder) => {
  const requests = inputFields.map(async (field) => {
    const file = field.selectedFile?.[0];
    if (!file) return;

    const formData = new FormData();
    formData.append("file", file);
    formData.append("filename", file.name);
    formData.append("description", field.description || "");
    formData.append("tags", (field.manualtags || field.tags || []).join(", "));

    const response = await fetch(`${API_BASE}/upload`, {
      method: "POST",
      body: formData,
    });

    if (!response.ok) {
      const err = await response.json().catch(() => ({}));
      throw new Error(err.error || `Upload failed: ${response.status}`);
    }

    return response.json();
  });

  return Promise.all(requests);
};

// ── Query / Search ───────────────────────────────────────────────────────────

/**
 * Send a natural-language query to the Azure RAG backend.
 * Returns: { type: "text"|"table"|"chart", answer, sources, data, chart_config }
 */
export const queryDocuments = async (query, filenameFilter = "") => {
  const body = { q: query };
  if (filenameFilter) body.filename = filenameFilter;

  const response = await fetch(`${API_BASE}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err.error || `Query failed: ${response.status}`);
  }

  return response.json();
};

// ── Health ───────────────────────────────────────────────────────────────────

export const checkHealth = async () => {
  const response = await fetch(`${API_BASE}/health`);
  return response.json();
};

export const deleteDocument = async (id) => {
  const response = await fetch(`${API_BASE}/document/${id}`, {
    method: "DELETE",
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    throw new Error(err.error || `Delete failed: ${response.status}`);
  }
  return response.json();
};

// ── Stubs for removed AWS-specific features ──────────────────────────────────
// These were Kendra / Step Function specific — no equivalent in Azure backend.
// Kept as no-ops so existing imports don't break.

export const createKendraIndex    = async () => ({ message: "Not applicable with Azure backend." });
export const getKendraStatus      = async () => ({ message: "Not applicable with Azure backend." });
export const getStepFunctionStatus = async () => ({ message: "Not applicable with Azure backend." });
export const deleteKendraIndex    = async () => ({ message: "Not applicable with Azure backend." });
