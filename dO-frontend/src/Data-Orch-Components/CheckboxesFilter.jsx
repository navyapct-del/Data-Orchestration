import React from "react";

function CheckboxesFilter({ checkboxes, setCheckboxes }) {
  const handleCheckboxChange = (e) => {
    const { name, checked } = e.target;
    setCheckboxes((prev) => ({
      ...prev,
      [name]: checked,
    }));
  };

  return (
    <div className="relative w-full sm:w-auto mt-3 sm:mt-0">
      <label className="ml-5">
        <input
          type="checkbox"
          name="imageCheck"
          checked={checkboxes.imageCheck}
          onChange={handleCheckboxChange}
        />
        <span className="px-3 py-2 text-xs lg:text-base cursor-pointer text-ellipsis overflow-hidden">
          Image
        </span>
      </label>
      <label className="ml-5">
        <input
          type="checkbox"
          name="documentCheck"
          checked={checkboxes.documentCheck}
          onChange={handleCheckboxChange}
        />
        <span className="px-3 py-2 text-xs lg:text-base cursor-pointer text-ellipsis overflow-hidden">
          Document
        </span>
      </label>
      <label className="ml-5">
        <input
          type="checkbox"
          name="videoCheck"
          checked={checkboxes.videoCheck}
          onChange={handleCheckboxChange}
        />
        <span className="px-3 py-2 text-xs lg:text-base cursor-pointer text-ellipsis overflow-hidden">
          Video
        </span>
      </label>
    </div>
  );
}

export default CheckboxesFilter;
