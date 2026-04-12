import React, { useState, useEffect } from "react";
import {
  Lucide,
  Modal,
  ModalBody,
  PreviewComponent,
  Preview,
} from "@/base-components";
import { LoadingIcon } from "@/base-components";
import AdvanceSearchTags from "./AdvanceSearchTags";
import QuerySummarization from "./QuerySummarization";
import { queryDocuments } from "../../config/ApiCall";

function AdvanceSearch({ handleToggle, isToggled, userEmail, type }) {
  const [searchResults, setSearchResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [hasSearched, setHasSearched] = useState(false);
  const [querySummarization, setQuerySummarization] = useState({
    query: "",
    answer: "",
  });
  const [responseData, setResponseData] = useState(null);
  const [filteredResults, setFilteredResults] = useState([]);
  const [staticBackdropModalPreview, setStaticBackdropModalPreview] =
    useState(false);
  const [selecteddocument, setSelectedDocument] = useState("");

  const officelink = "https://view.officeapps.live.com/op/embed.aspx?src=";

  const fetchSearchResults = async () => {
    setHasSearched(true);
    setLoading(true);

    try {
      const data = await queryDocuments(searchQuery);

      // Azure backend returns { type, answer, sources, data }
      const results = (data.sources || []).map((src) => ({
        title:   src.filename,
        excerpt: src.summary || "",
        uri:     src.blob_url || "",
      }));

      setSearchResults(results);
      setResponseData(data);
      setQuerySummarization({
        query:  searchQuery,
        answer: data.answer || "",
      });
    } catch (error) {
      console.error("Error fetching search results:", error);
      setSearchResults([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSearchKeyDown = (e) => {
    if (e.key === "Enter") {
      fetchSearchResults();
    }
  };

  useEffect(() => {
    if (searchQuery === "") {
      setSearchResults([]);
      setHasSearched(false);
      setQuerySummarization({ query: "", answer: "" });
      setResponseData(null);
      setFilteredResults([]);
    }
  }, [searchQuery]);

  const handleFilteredResultsChange = (results) => {
    console.log("Filtered Results Updated:", results);
    setFilteredResults(results);
  };

  const [kendraStatus, setKendraStatus] = useState("Index Not Found");

  const handleKendraStatusChange = (status) => {
    setKendraStatus(status);
  };

  // Determine what results to display based on the availability of filtered results
  const displayResults =
    filteredResults.length > 0 ? filteredResults : searchResults;

  return (
    <>
      <div className="col-span-12 lg:col-span-3 2xl:col-span-2">
        {hasSearched && !loading && responseData && (
          <AdvanceSearchTags
            data={responseData}
            searchQuery={searchQuery}
            userEmail={userEmail}
            onFilteredResultsChange={handleFilteredResultsChange}
          />
        )}
      </div>
      <div className="col-span-12 lg:col-span-9 2xl:col-span-10">
        <div className="intro-y flex flex-col-reverse sm:flex-row items-center">
          <div className="w-full sm:w-auto relative mr-auto mt-3 sm:mt-0 flex items-center">
            <Lucide
              icon="Search"
              className="w-4 h-4 absolute my-auto inset-y-0 ml-3 left-0 z-10 text-slate-500"
            />
            <input
              type="text"
              className="form-control w-full sm:w-64 box px-10"
              placeholder="Advance Search files"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyDown={handleSearchKeyDown}
            />
            <span
              className={`w-16 h-10 flex items-center ml-4 p-1 ${
                isToggled ? "bg-cyan-900" : "bg-gray-300"
              } rounded-full cursor-pointer`}
              onClick={handleToggle}
            >
              <span
                className={`w-8 h-8 bg-white rounded-full shadow-md transform transition-transform ${
                  isToggled ? "translate-x-6" : ""
                }`}
              ></span>
            </span>
          </div>

          {/* <div className="w-full sm:w-auto flex">
            <Kendra onStatusChange={handleKendraStatusChange} />
          </div> */}
        </div>
        {loading ? (
          <div className="flex justify-center items-center min-h-screen">
            <div className="text-center">
              <LoadingIcon icon="circles" className="w-8 h-8" />
            </div>
          </div>
        ) : hasSearched && displayResults.length === 0 ? (
          <div className="col-span-12 lg:col-span-9 2xl:col-span-10">
            <div className="text-center mt-48 text-base tracking-wide">
              <h1 className="font-md mt-4 font-medium text-[#E9A53F]">
                No documents found. Please try again later.
              </h1>
            </div>
          </div>
        ) : (
          <>
            <div className="intro-y grid grid-cols-12 gap-3 sm:gap-6">
              <div className="col-span-12 lg:col-span-7">
                {displayResults.map((result, index) => (
                  <PreviewComponent className="intro-y box mt-5">
                    {({ toggle }) => (
                      <>
                        <div
                          className="intro-y col-span-12 lg:col-span-7"
                          key={index}
                        >
                          <div className="drop-shadow-lg px-5 py-5 shadow-xl text-md bg-white text-ellipsis overflow-hidden rounded-md my-5">
                            <a
                              className="py-5 font-medium text-primary cursor-pointer"
                              onClick={() => {
                                setStaticBackdropModalPreview(true);
                                setSelectedDocument(result.uri);
                              }}
                            >
                              {result.title}
                            </a>
                            <p className="py-3">{result.excerpt}</p>
                            <Modal
                              size="modal-xl"
                              backdrop="static"
                              show={staticBackdropModalPreview}
                              onHidden={() => {
                                setStaticBackdropModalPreview(false);
                              }}
                            >
                              <ModalBody>
                                <div className="">
                                  <a
                                    onClick={() => {
                                      setStaticBackdropModalPreview(false);
                                    }}
                                    className="absolute right-0 top-0 mt-3 mr-3"
                                    href="#"
                                  >
                                    <Lucide
                                      icon="X"
                                      className="w-3 h-3 text-slate-400 xl:w-6 xl:h-6 lg:w-5 lg:h-5 md:w-6 md:h-6 stroke-cyan-700 mb-5 stroke-2"
                                    />
                                  </a>

                                  <div className="grid grid-cols-12">
                                    <div className="col-span-12  py-5">
                                      {selecteddocument
                                        ?.split(".")
                                        .pop()
                                        .toLowerCase() === "pdf" ? (
                                        <iframe
                                          src={selecteddocument + "#toolbar=0"}
                                          style={{
                                            height: "750px",
                                            width: "100%",
                                          }}
                                          oncontextmenu="return false;"
                                        ></iframe>
                                      ) : selecteddocument
                                          ?.split(".")
                                          .pop()
                                          .toLowerCase() === "docx" ? (
                                        <iframe
                                          src={officelink + selecteddocument}
                                          align="center"
                                          style={{
                                            height: "750px",
                                            width: "100%",
                                          }}
                                        ></iframe>
                                      ) : (
                                        <>
                                          <iframe
                                            src={selecteddocument}
                                            width="100%"
                                            height="750px"
                                          ></iframe>
                                        </>
                                      )}
                                    </div>
                                  </div>
                                </div>
                              </ModalBody>
                            </Modal>
                          </div>
                        </div>
                      </>
                    )}
                  </PreviewComponent>
                ))}
              </div>
              {hasSearched && (
                <div className="col-span-12 lg:col-span-5">
                  <QuerySummarization
                    query={querySummarization.query}
                    answer={querySummarization.answer}
                  />
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </>
  );
}

export default AdvanceSearch;
