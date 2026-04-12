import React, { useState, useEffect, useCallback } from "react";
import { fetchTagsData } from "../config/ApiCall";

const TagFilter = ({
  type,
  currentLoc,
  checkboxes,
  selectedTags,
  setSelectedTags,
}) => {
  const [tagsData, setTagsData] = useState([]);

  useEffect(() => {
    if (type) {
      fetchTagsData(type, currentLoc, checkboxes)
        .then((data) => {
          setTagsData(data || []);
        })
        .catch((error) => console.error("Error fetching tags data:", error));
    }
  }, [type, currentLoc, checkboxes]);

  const handleTagChange = useCallback(
    (event) => {
      const { value, checked } = event.target;
      setSelectedTags((prev) =>
        checked ? [...prev, value] : prev.filter((tag) => tag !== value)
      );
    },
    [setSelectedTags]
  );

  return (
    <div className="intro-y box p-5 mt-6">
      <div className="mt-1">
        {tagsData.length > 0 ? (
          tagsData.map((item) => (
            <div key={item.tags} className="flex items-center">
              <input
                type="checkbox"
                value={item.tags}
                className="ml-5 checkbox_tags"
                onChange={handleTagChange}
                checked={selectedTags.includes(item.tags)}
              />
              <p className="px-3 py-2 text-xs lg:text-[13px] cursor-pointer text-ellipsis overflow-hidden">
                {item.tags}
              </p>
            </div>
          ))
        ) : (
          <p className="text-center text-[#E9A53F] text-[12px]">
            No Tags Found
          </p>
        )}
      </div>
    </div>
  );
};

export default TagFilter;
