import React, { useState, useEffect } from "react";
import { Lucide } from "@/base-components";

export default function Search({ allData, setFilteredData, selectedTags }) {
  const [searchQuery, setSearchQuery] = useState("");

  useEffect(() => {
    const filtered = allData.filter((item) => {
      // Split tags by comma and trim whitespace
      const tagsArray = item.tags
        ? item.tags.split(",").map((tag) => tag.trim().toLowerCase())
        : [];

      // Check if any of the selected tags match the item's tags
      const matchesTags =
        selectedTags.length === 0 ||
        selectedTags.some((tag) => tagsArray.includes(tag.toLowerCase()));

      // Check if the item's name or tags include the search query
      const matchesSearch =
        item.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        tagsArray.some((tag) => tag.includes(searchQuery.toLowerCase()));

      return matchesTags && matchesSearch;
    });

    setFilteredData(filtered);
  }, [searchQuery, selectedTags, allData, setFilteredData]);

  return (
    <div className="relative w-full sm:w-auto mt-3 sm:mt-0">
      <Lucide
        icon="Search"
        className="w-4 h-4 absolute my-auto inset-y-0 ml-3 left-0 z-10 text-slate-500"
      />
      <input
        type="text"
        className="form-control w-full sm:w-64 box px-10"
        placeholder="Search files"
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
      />
    </div>
  );
}
