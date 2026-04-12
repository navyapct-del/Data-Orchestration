import React, { useContext, useEffect, useState, useCallback } from "react";
import { AccountContext } from "../config/Account";
import ObjectCard from "../Data-Orch-Components/CardsComponent/ObjectCard";
import { listObjects } from "../config/ApiCall";
import ReactPaginate from "react-paginate";
import { Lucide } from "@/base-components";
import TagFilter from "../Data-Orch-Components/TagsFilter";
import Search from "../Data-Orch-Components/SearchComponent/Search";
import Upload from "../Data-Orch-Components/UploadComponent/UploadMain";
import AdvanceSearch from "../Data-Orch-Components/AdvanceSearchComponent/AdvanceSearch";
import CheckboxesFilter from "../Data-Orch-Components/CheckboxesFilter";
import TabulatorFile from "../Data-Orch-Components/TabulatorFile";

export default function ContentManager(props) {
  const [currentPage, setCurrentPage]     = useState(0);
  const [objectsPerPage]                  = useState(10);
  const [cardEnabled, setCardEnabled]     = useState(false);
  const [allData, setAllData]             = useState([]);
  const [filteredData, setFilteredData]   = useState([]);
  const [selectedTags, setSelectedTags]   = useState([]);
  const { userdetails, userEmail }        = useContext(AccountContext);
  const indexOfLastObject                 = (currentPage + 1) * objectsPerPage;
  const indexOfFirstObject                = indexOfLastObject - objectsPerPage;
  const [loading, setLoading]             = useState(false);
  const [showUpload, setShowUpload]       = useState(false);
  const [currentLoc, setCurrentLoc]       = useState("");
  const [contentFlag, setContentFlag]     = useState(false);
  const [checkboxes, setCheckboxes]       = useState({ imageCheck: false, videoCheck: false, documentCheck: false });
  const [isToggled, setIsToggled]         = useState(false);
  const [isCheckboxFilterActive, setIsCheckboxFilterActive] = useState(false);
  const [showTabulator, setShowTabulator] = useState(false);
  const [kendraStatus, setKendraStatus]   = useState("Index Not Found");

  useEffect(() => {
    if (userdetails && props.type) {
      const location = props.type !== "file" ? `${userEmail}/${props.type}/` : `${userEmail}/`;
      setCurrentLoc(location);
      setCardEnabled(false);
    }
  }, [props.type, userdetails]);

  useEffect(() => {
    // Fetch as soon as props.type is known — don't wait for currentLoc
    if (!props.type) return;
    setLoading(true);
    listObjects(props.type, currentLoc)
      .then((data) => {
        console.log("[ContentManager] listObjects response:", data);
        if (Array.isArray(data) && data.length > 0) {
          setAllData(data); setFilteredData(data); setContentFlag(false);
        } else if (Array.isArray(data)) {
          setAllData([]); setFilteredData([]); setContentFlag(false);
        } else {
          setAllData([]); setFilteredData([]); setContentFlag(true);
        }
      })
      .catch((e) => { console.error("[ContentManager] fetch error:", e); setContentFlag(true); })
      .finally(() => setLoading(false));
  }, [props.type]);

  useEffect(() => {
    if (selectedTags.length > 0)
      setFilteredData(allData.filter((item) => selectedTags.some((tag) => item.tags.includes(tag))));
    else
      setFilteredData(allData);
  }, [allData, selectedTags]);

  useEffect(() => {
    let filtered = allData;
    if (checkboxes.imageCheck && checkboxes.documentCheck && checkboxes.videoCheck)
      filtered = allData.filter((i) => ["image","document","video"].includes(i.file_type));
    else if (checkboxes.imageCheck && checkboxes.documentCheck)
      filtered = allData.filter((i) => i.file_type === "image" || i.file_type === "document");
    else if (checkboxes.imageCheck && checkboxes.videoCheck)
      filtered = allData.filter((i) => i.file_type === "image" || i.file_type === "video");
    else if (checkboxes.documentCheck && checkboxes.videoCheck)
      filtered = allData.filter((i) => i.file_type === "document" || i.file_type === "video");
    else if (checkboxes.imageCheck)    filtered = allData.filter((i) => i.file_type === "image");
    else if (checkboxes.documentCheck) filtered = allData.filter((i) => i.file_type === "document");
    else if (checkboxes.videoCheck)    filtered = allData.filter((i) => i.file_type === "video");
    setFilteredData(filtered);
  }, [checkboxes, allData]);

  useEffect(() => {
    setIsCheckboxFilterActive(checkboxes.imageCheck || checkboxes.videoCheck || checkboxes.documentCheck);
  }, [checkboxes]);

  const handlePagination     = useCallback((e) => setCurrentPage(e.selected), []);
  const handleUploadClick    = () => setShowUpload((p) => !p);
  const handleToggle         = () => setIsToggled((p) => !p);
  const handleTabulatorClick = () => { setShowTabulator((p) => !p); setShowUpload(false); };

  const handleDeleteDoc = (deletedId) => {
    setAllData((prev) => prev.filter((d) => d.id !== deletedId));
    setFilteredData((prev) => prev.filter((d) => d.id !== deletedId));
  };
  const currentData = (filteredData || []).slice(indexOfFirstObject, indexOfLastObject);

  const pageTitle = {
    file: "File Manager", image: "Images",
    video: "Videos", document: "Documents", media: "Media",
  }[props.type] || "";

  return (
    <div style={s.page}>
      {isToggled ? (
        <AdvanceSearch handleToggle={handleToggle} isToggled={isToggled} userEmail={userEmail} />
      ) : (
        <div style={s.card}>

          {/* ── TOOLBAR ── */}
          <div style={s.toolbar}>
            {/* Left: title */}
            <h2 style={s.title}>{pageTitle}</h2>

            {/* Centre: search + toggle */}
            <div style={s.toolbarCentre}>
              {!isCheckboxFilterActive && (
                <div style={s.searchWrap}>
                  <svg style={s.searchIcon} viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2">
                    <circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>
                  </svg>
                  <Search allData={allData} setFilteredData={setFilteredData} selectedTags={selectedTags} />
                </div>
              )}

              {props.type === "file" && (
                <CheckboxesFilter checkboxes={checkboxes} setCheckboxes={setCheckboxes} />
              )}

              {/* Advanced search toggle for documents */}
              {props.type === "document" && (
                <div style={s.toggleWrap} onClick={handleToggle}>
                  <div style={{ ...s.toggleTrack, background: isToggled ? "#0d3347" : "#d1d5db" }}>
                    <div style={{ ...s.toggleThumb, transform: isToggled ? "translateX(20px)" : "translateX(2px)" }} />
                  </div>
                </div>
              )}
            </div>

            {/* Right: Upload button */}
            <button
              style={s.uploadBtn}
              onClick={handleUploadClick}
              onMouseEnter={(e) => (e.currentTarget.style.background = "#0a2535")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "#0d3347")}
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="16 16 12 12 8 16"/><line x1="12" y1="12" x2="12" y2="21"/>
                <path d="M20.39 18.39A5 5 0 0 0 18 9h-1.26A8 8 0 1 0 3 16.3"/>
              </svg>
              Upload New Files
            </button>
          </div>

          {/* ── CONTENT ── */}
          <div style={s.content}>
            {showTabulator ? (
              <TabulatorFile data={filteredData} />
            ) : showUpload ? (
              <Upload current_Folder={currentLoc} type={props.type} />
            ) : (
              <>
                {loading ? (
                  <div style={s.emptyState}>
                    <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#d1d5db" strokeWidth="1.5">
                      <path d="M22 12h-4l-3 9L9 3l-3 9H2"/>
                    </svg>
                    <p style={{ color: "#9ca3af", marginTop: "12px", fontSize: "14px" }}>Loading files…</p>
                  </div>
                ) : contentFlag || filteredData.length === 0 ? (
                  <div style={s.emptyState}>
                    <p style={s.emptyText}>
                      No files available to display. Please upload files to get started.
                    </p>
                  </div>
                ) : (
                  <>
                    <div style={s.grid}>
                      <ObjectCard
                        currentLoc={currentLoc}
                        type={props.type}
                        currentName={currentData.map((i) => i.name)}
                        currentsize={currentData.map((i) => i.size)}
                        description={currentData.map((i) => i.description)}
                        tags={currentData.map((i) => i.tags)}
                        date={currentData.map((i) => i.date)}
                        ids={currentData.map((i) => i.id)}
                        filenames={currentData.map((i) => i.filename)}
                        blobUrls={currentData.map((i) => i.blob_url)}
                        onDelete={handleDeleteDoc}
                        flag={cardEnabled}
                        contentFlag={contentFlag}
                        loading={loading}
                      />
                    </div>

                    {filteredData.length > objectsPerPage && (
                      <div style={s.paginationWrap}>
                        <ReactPaginate
                          previousLabel={<Lucide icon="ChevronLeft" className="w-4 h-4" />}
                          nextLabel={<Lucide icon="ChevronRight" className="w-4 h-4" />}
                          breakLabel="..."
                          pageCount={Math.ceil(filteredData.length / objectsPerPage)}
                          marginPagesDisplayed={2}
                          pageRangeDisplayed={5}
                          onPageChange={handlePagination}
                          containerClassName="pagination"
                          pageClassName="page-item"
                          pageLinkClassName="page-link"
                          previousClassName="page-item"
                          previousLinkClassName="page-link"
                          nextClassName="page-item"
                          nextLinkClassName="page-link"
                          breakClassName="page-item"
                          breakLinkClassName="page-link"
                          activeClassName="active"
                        />
                      </div>
                    )}
                  </>
                )}
              </>
            )}
          </div>

        </div>
      )}
    </div>
  );
}

const s = {
  page: {
    fontFamily: "'Segoe UI', system-ui, sans-serif",
    padding: "0",
  },

  card: {
    background: "#ffffff",
    borderRadius: "16px",
    overflow: "hidden",
    boxShadow: "0 2px 12px rgba(0,0,0,0.08)",
    minHeight: "500px",
    display: "flex",
    flexDirection: "column",
  },

  /* Toolbar */
  toolbar: {
    display: "flex",
    alignItems: "center",
    gap: "16px",
    padding: "20px 28px",
    borderBottom: "1px solid #f0f0f0",
    flexWrap: "wrap",
  },
  title: {
    fontSize: "18px",
    fontWeight: "600",
    color: "#1a1a2e",
    margin: 0,
    whiteSpace: "nowrap",
    minWidth: "100px",
  },
  toolbarCentre: {
    display: "flex",
    alignItems: "center",
    gap: "12px",
    flex: 1,
  },
  searchWrap: {
    position: "relative",
    display: "flex",
    alignItems: "center",
  },
  searchIcon: {
    position: "absolute",
    left: "10px",
    width: "16px",
    height: "16px",
    pointerEvents: "none",
    zIndex: 1,
  },

  /* Toggle */
  toggleWrap: {
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
  },
  toggleTrack: {
    width: "44px",
    height: "24px",
    borderRadius: "12px",
    position: "relative",
    transition: "background 0.2s",
  },
  toggleThumb: {
    position: "absolute",
    top: "2px",
    width: "20px",
    height: "20px",
    borderRadius: "50%",
    background: "#ffffff",
    boxShadow: "0 1px 3px rgba(0,0,0,0.2)",
    transition: "transform 0.2s",
  },

  /* Upload button */
  uploadBtn: {
    display: "flex",
    alignItems: "center",
    gap: "8px",
    padding: "10px 20px",
    background: "#0d3347",
    color: "#ffffff",
    border: "none",
    borderRadius: "8px",
    fontSize: "14px",
    fontWeight: "600",
    cursor: "pointer",
    transition: "background 0.2s",
    whiteSpace: "nowrap",
    marginLeft: "auto",
  },

  /* Content area */
  content: {
    flex: 1,
    padding: "24px 28px",
    background: "#f8f9fb",
  },

  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fill, minmax(140px, 1fr))",
    gap: "16px",
  },

  /* Empty state */
  emptyState: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    minHeight: "360px",
  },
  emptyText: {
    color: "#f59e0b",
    fontSize: "15px",
    fontWeight: "500",
    textAlign: "center",
    margin: 0,
  },

  /* Pagination */
  paginationWrap: {
    marginTop: "24px",
    display: "flex",
    justifyContent: "flex-start",
  },
};
