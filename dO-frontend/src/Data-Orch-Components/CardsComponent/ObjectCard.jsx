import React from "react";
import Cards from "./Cards";
import { LoadingIcon } from "@/base-components";

export default function ObjectCard({
  currentName,
  type,
  description,
  tags,
  subtitle,
  currentsize,
  date,
  currentloc,
  contentFlag,
  setFolder,
  loading,
  ids,
  filenames,
  blobUrls,
  onDelete,
}) {
  // Ensure `currentName` is an array and is not null
  // const isDataAvailable = Array.isArray(currentName) && currentName.length > 0;
  const isDataAvailable =
    (Array.isArray(currentName) && currentName.length > 0) ||
    (Array.isArray(description) && description.length > 0) ||
    (Array.isArray(tags) && tags.length > 0) ||
    (Array.isArray(currentsize) && currentsize.length > 0) ||
    (Array.isArray(date) && date.length > 0);

  if (loading) {
    return (
      <div className="col-span-12 lg:col-span-9 2xl:col-span-10">
        <div className="flex justify-center items-center mt-48">
          <LoadingIcon icon="circles" className="w-10 h-10" />
        </div>
      </div>
    );
  }

  if (!isDataAvailable && contentFlag === true) {
    return (
      <div className="col-span-12 lg:col-span-9 2xl:col-span-10">
        <div className="text-center mt-48 text-base tracking-wide">
          <h1 className="font-md mt-4 font-medium text-[#E9A53F]">
            No files available to display. Please upload files to get started.
          </h1>
        </div>
      </div>
    );
  }

  // Display message when no data is available and contentFlag is false
  if (!isDataAvailable && contentFlag === false) {
    return (
      <div className="col-span-12 lg:col-span-9 2xl:col-span-10">
        <div className="text-center mt-48 text-base tracking-wide">
          <h1 className="font-md mt-4 font-medium text-[#E9A53F]">
            No data found. Please adjust your search criteria or try again
            later.
          </h1>
        </div>
      </div>
    );
  }

  if (isDataAvailable) {
    return (
      <>
        {currentName.map((item, index) => (
          <div key={ids ? ids[index] : index} className="">
            <div className="relative">
              <Cards
                setFolder={setFolder}
                type={type}
                description={description[index]}
                tags={tags[index]}
                subtitle={subtitle}
                name={currentName[index]}
                id={ids ? ids[index] : undefined}
                filename={filenames ? filenames[index] : undefined}
                blob_url={blobUrls ? blobUrls[index] : undefined}
                size={currentsize[index]}
                alldatalength={currentName.length}
                objdate={date[index]}
                currentloc={currentloc}
                onDelete={onDelete}
              />
            </div>
          </div>
        ))}
      </>
    );
  }

  return null;
}
