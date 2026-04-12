import React from "react";

const QuerySummarization = ({ query, answer }) => {
  return (
    <div className="sticky top-5 h-auto">
      <div className="drop-shadow-lg px-5 py-5 shadow-xl text-md bg-white mt-5 h-auto overflow-hidden rounded-md">
        <h1 className="text-center text-lg text-cyan-900 font-semibold mb-5">
          Query Summarization
        </h1>

        <div className="grid grid-cols-12 gap-4">
          <div className="xl:col-span-4 lg:col-span-3 md:col-span-12">
            <h1 className="px-5 py-3 text-white bg-cyan-900 font-medium tracking-wide text-sm rounded-lg text-center">
              Question
            </h1>
          </div>
          <div className="xl:col-span-8 lg:col-span-9 md:col-span-12">
            <p className="px-5 py-3 tracking-wide text-sm rounded-lg bg-gray-100">
              {query || "No question available"}
            </p>
          </div>
        </div>

        <div className="grid grid-cols-12 gap-4 mt-5">
          <div className="xl:col-span-4 lg:col-span-3 md:col-span-12">
            <h1 className="px-5 py-3 text-white bg-cyan-900 font-medium tracking-wide text-sm rounded-lg text-center">
              Answer
            </h1>
          </div>
          <div className="xl:col-span-8 lg:col-span-9 md:col-span-12">
            <div className="px-5 py-3 tracking-wide text-sm rounded-lg overflow-x-hidden overflow-y-auto max-h-96 bg-gray-100">
              {answer || "No answer available"}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default QuerySummarization;
