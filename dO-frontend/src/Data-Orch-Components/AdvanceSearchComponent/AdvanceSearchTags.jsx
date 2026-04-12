import React, { useState, useEffect } from "react";

const API_BASE = import.meta.env.VITE_AZURE_FUNCTIONS_URL || "http://localhost:7071/api";

function AdvanceSearchTags({
  data,
  searchQuery,
  userEmail,
  onFilteredResultsChange,
}) {
  const [manualTags, setManualTags] = useState([]);
  const [selectedManualTags, setSelectedManualTags] = useState([]);

  // Derive tags from the Azure query response (data.sources[].summary tags)
  useEffect(() => {
    if (!data) return;
    const tagSet = new Set();
    const sources = data.sources || [];
    sources.forEach((src) => {
      if (src.summary) {
        // Extract comma-separated tags from summary if present
        src.summary.split(",").forEach((t) => {
          const trimmed = t.trim();
          if (trimmed && trimmed.length < 40) tagSet.add(trimmed);
        });
      }
    });
    setManualTags(Array.from(tagSet).map((v) => ({ value: v, count: 1 })));
  }, [data]);

  // Re-filter results when tag selection changes
  useEffect(() => {
    if (!data) return;
    const sources = data.sources || [];
    if (selectedManualTags.length === 0) {
      // No filter active — show all sources
      const results = sources.map((src) => ({
        title:   src.filename || "Untitled",
        excerpt: src.summary  || "",
        uri:     src.blob_url || "",
      }));
      onFilteredResultsChange(results);
      return;
    }

    // Filter sources whose summary contains any selected tag
    const filtered = sources
      .filter((src) =>
        selectedManualTags.some((tag) =>
          (src.summary || "").toLowerCase().includes(tag.toLowerCase())
        )
      )
      .map((src) => ({
        title:   src.filename || "Untitled",
        excerpt: src.summary  || "",
        uri:     src.blob_url || "",
      }));

    onFilteredResultsChange(filtered);
  }, [selectedManualTags]);

  const handleManualTagChange = (event) => {
    const { value, checked } = event.target;
    setSelectedManualTags((prev) =>
      checked ? [...prev, value] : prev.filter((t) => t !== value)
    );
  };

  return (
    <div className="intro-y box p-5 mt-14">
      <div className="mt-1">
        <h3 className="text-center font-semibold text-[14px] text-primary">
          Filter by Tags
        </h3>
        {manualTags.length > 0 ? (
          manualTags.map((tag, index) => (
            <div key={index} className="my-3">
              <input
                type="checkbox"
                value={tag.value}
                id={`tag-${index}`}
                onChange={handleManualTagChange}
              />
              <label htmlFor={`tag-${index}`} className="mx-2">
                {tag.value.slice(0, 25)}
              </label>
            </div>
          ))
        ) : (
          <div className="my-3 text-center text-[12px] text-[#E9A53F]">
            No tags found.
          </div>
        )}
      </div>
    </div>
  );
}

export default AdvanceSearchTags;
