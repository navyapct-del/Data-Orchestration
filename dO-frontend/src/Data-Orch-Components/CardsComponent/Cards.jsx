import React, { useState } from "react";
import DocumentModal from "./DocumentModal";
import ImageModal from "./ImageModal";
import documentUrl from "../../assets/images/folder.png";
import VideoModal from "../../Data-Orch-Components/CardsComponent/VideoModal";
import pdf_Url  from "../../assets/images/pdf.png";
import docx_Url from "../../assets/images/docx.png";
import text_Url from "../../assets/images/text.png";
import other_Url from "../../assets/images/other.png";
import xlsx_Url from "../../assets/images/xlsx.svg";
import videojs from "video.js";
import "video.js/dist/video-js.css";
import "videojs-contrib-quality-levels";
import "videojs-http-source-selector";
import { deleteDocument } from "../../config/ApiCall";

const API_BASE = import.meta.env.VITE_AZURE_FUNCTIONS_URL || "http://localhost:7071/api";

// Use backend proxy to serve private blob images
const ImageWithSas = ({ blobUrl, docId, onClick }) => {
  const src = docId ? `${API_BASE}/file?id=${docId}` : (blobUrl || other_Url);
  return (
    <img
      alt="Image"
      src={src}
      onClick={onClick}
      onError={(e) => { e.target.onerror = null; e.target.src = other_Url; }}
      style={{ objectFit: "cover", width: "100%", height: "80px", borderRadius: "4px", cursor: "pointer" }}
    />
  );
};

// Utility function for formatting dates
const formatDate = (dateString) => {
  const date = new Date(dateString);
  const year = date.toLocaleString("default", { year: "numeric" });
  const month = date.toLocaleString("default", { month: "2-digit" });
  const day = date.toLocaleString("default", { day: "2-digit" });
  const time = date.toLocaleString().split(",")[1].trim();
  return `${day}-${month}-${year} ${time}`;
};

// Mapping for file extensions to icon images
const fileIcons = {
  pdf:     pdf_Url,
  docx:    docx_Url,
  doc:     docx_Url,
  txt:     text_Url,
  csv:     text_Url,
  xlsx:    xlsx_Url,
  xls:     xlsx_Url,
  default: other_Url,
};

const Cards = (props) => {
  const [showDocumentModal, setShowDocumentModal] = useState(false);
  const [showPreview, setShowPreview] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const handleDelete = async (e) => {
    e.stopPropagation();
    if (!window.confirm(`Delete "${props.filename || props.name}"?`)) return;
    setDeleting(true);
    if (props.onDelete) props.onDelete(props.id);
    try {
      await deleteDocument(props.id);
    } catch (err) {
      alert("Delete failed: " + err.message);
    } finally {
      setDeleting(false);
    }
  };

  const handleCloseBothModals = () => {
    setShowDocumentModal(false);
    setShowPreview(false); // Close the preview modal
  };
  const [showImageModal, setShowImageModal] = useState(false);
  const [showVideoModal, setShowVideoModal] = useState(false);

  const fileExtension = ((props.filename || props.name || "").split(".").pop() || "").toLowerCase();
  const documentName = fileIcons[fileExtension] || fileIcons.default;

  // Use backend proxy URL to serve private blobs — avoids PublicAccessNotPermitted
  const cloudfrontFileLink = props.id
    ? `${API_BASE}/file?id=${props.id}`
    : (props.blob_url || "");
  const fileDetails = {
    name: props.name,
    date: props.objdate ? formatDate(props.objdate) : "",
    description: props.description,
    tags: (props.tags || "").replace(/[{}'[\]]/g, ""),
    type: fileExtension,
    size: props.size,
  };

  const options = {
    sources: [
      {
        src: cloudfrontFileLink,
        type: "application/x-mpegURL",
        withCredentials: false,
      },
    ],

    muted: false,
    language: "en",
    preload: "auto",
    fluid: true,
    height: "200px",
    width: "300px",
    aspectRatio: "16:9",
    preferFullWindow: true,
    responsive: true,
    playbackRates: [0.5, 1, 1.5, 2],
    html5: {
      hls: {
        overrideNative: true,
        limitRenditionByPlayerDimensions: true,
        useDevicePixelRatio: true,
      },
      nativeAudioTracks: true,
      nativeVideoTracks: false,
      useBandwidthFromLocalStorage: true,
    },
    controlBar: {
      pictureInPictureToggle: false,
    },
    poster: {
      src: cloudfrontFileLink,
    },
  };

  const renderFolder = () => (
    <div className="file box rounded-md px-5 pt-8 pb-5 sm:px-5 relative zoom-in z-0">
      <div className="absolute left-0 top-0 mt-3 ml-3"></div>
      <div className="w-3/5 file__icon file__icon--image mx-auto">
        <div className="file__icon--image__preview image-fit">
          <img
            alt="Folder"
            src={documentUrl}
            onClick={() => props.setFolder(props.name)}
          />
        </div>
      </div>
      <a href="#" className="block font-medium mt-4 text-center truncate">
        {props.name.split("/").slice(-2)[0].slice(0, 8)}
      </a>
      <div className="text-slate-500 text-xs text-center mt-0.5">
        {props.objdate ? formatDate(props.objdate) : ""}
      </div>
    </div>
  );

  const renderDocument = () => (
    <>
      <div className="file box rounded-md px-5 pt-8 pb-5 sm:px-5 relative zoom-in">
        <button
          onClick={handleDelete}
          disabled={deleting}
          title="Delete"
          style={{ position:"absolute", top:"6px", right:"6px", background:"none", border:"none", cursor:"pointer", color:"#ef4444", padding:"2px" }}
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/>
          </svg>
        </button>
        <div className="absolute left-0 top-0 mt-3 ml-3"></div>
        <div className="w-3/5 file__icon file__icon--image mx-auto">
          <div className="file__icon--image__preview image-fit">
            <img
              alt="Document"
              src={documentName}
              onClick={() => setShowDocumentModal(true)}
            />
          </div>
        </div>
        <div className="block font-medium mt-4 text-center truncate">
          {(props.filename || (props.name || "").split("/").pop()).slice(0, 12)}
        </div>
        <div className="text-slate-500 text-xs text-center mt-0.5">
          {props.objdate ? formatDate(props.objdate) : ""}
        </div>
      </div>
      <DocumentModal
        showModal={showDocumentModal}
        onClose={handleCloseBothModals}
        cloudfrontFileLink={cloudfrontFileLink}
        documentName={documentName}
        fileDetails={fileDetails}
        showPreview={showPreview}
        setShowPreview={setShowPreview}
      />
    </>
  );

  const renderImage = () => (
    <>
      <div className="file box rounded-md px-5 pt-8 pb-5 sm:px-5 relative zoom-in">
        <button
          onClick={handleDelete}
          disabled={deleting}
          title="Delete"
          style={{ position:"absolute", top:"6px", right:"6px", background:"none", border:"none", cursor:"pointer", color:"#ef4444", padding:"2px" }}
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4h6v2"/>
          </svg>
        </button>
        <div className="absolute left-0 top-0 mt-3 ml-3"></div>
        <div className="w-3/5 file__icon file__icon--image mx-auto">
          <div className="file__icon--image__preview image-fit">
            <ImageWithSas
              blobUrl={cloudfrontFileLink}
              docId={props.id}
              onClick={() => setShowImageModal(true)}
            />
          </div>
        </div>
        <div className="block font-medium mt-4 text-center truncate">
          {(props.filename || (props.name || "").split("/").pop()).slice(0, 12)}
        </div>
        <div className="text-slate-500 text-xs text-center mt-0.5">
          {props.objdate ? formatDate(props.objdate) : ""}
        </div>
      </div>
      <ImageModal
        showModal={showImageModal}
        onClose={() => setShowImageModal(false)}
        cloudfrontFileLink={cloudfrontFileLink}
        fileDetails={fileDetails}
      />
    </>
  );

  const renderVideo = () => (
    <>
      <div className="file box rounded-md px-5 pt-8 pb-5 sm:px-5 relative zoom-in">
        <div className="absolute left-0 top-0 mt-3 ml-3"></div>
        <div
          className="w-3/5 file__icon file__icon--image mx-auto"
          onClick={() => setShowVideoModal(true)}
        >
          <div className="file__icon--image__preview image-fit mx-auto">
            <video
              alt="Video"
              src={cloudfrontFileLink}
              className="md:h-24 lg:h-24 xl:h-24 2xl:h-28"
            />
          </div>
        </div>
        <div className="block font-medium mt-4 text-center truncate">
          {props.name.split("/").pop().slice(0, 8)}...
        </div>
        <div className="text-slate-500 text-xs text-center mt-0.5">
          {props.objdate ? formatDate(props.objdate) : ""}
        </div>
      </div>
      <VideoModal
        showModal={showVideoModal}
        onClose={() => setShowVideoModal(false)}
        videoSrc={cloudfrontFileLink}
        fileDetails={fileDetails}
      />
    </>
  );

  const renderMedia = () => (
    <>
      <div className="file box rounded-md px-5 pt-8 pb-5 sm:px-5 relative zoom-in">
        <div className="absolute left-0 top-0 mt-3 ml-3"></div>
        <div className="w-4/5 h-36 file__icon file__icon--image mx-auto">
          <div
            className="file__icon--image__preview image-fit"
            onClick={() => setShowMediaModal(true)}
          >
            <video
              width="95%"
              height="100%"
              poster="https://th.bing.com/th/id/OIP.hTRblcX_DPa_dbLBZSkFDgHaEM?pid=ImgDet&w=200&h=113&c=7&dpr=1.5"
              className="rounded-md"
            />
          </div>
        </div>
        <div className="block font-medium mt-4 text-center truncate">
          {props.name.split("/").pop().slice(0, 8)}...
        </div>
        <div className="text-slate-500 text-xs text-center mt-0.5">
          {props.objdate ? formatDate(props.objdate) : ""}
        </div>
      </div>
      <MediaModal
        showModal={showMediaModal}
        onClose={() => setShowMediaModal(false)}
        cloudfrontFileLink={cloudfrontFileLink}
        fileDetails={fileDetails}
        subtitle={props.subtitle}
      />
    </>
  );

  return props.name.endsWith("/")
    ? renderFolder()
    : props.name.split("/").slice(1)[0] === "image"
    ? renderImage()
    : props.name.split("/").slice(1)[0] === "video"
    ? renderVideo()
    : props.type === "media"
    ? renderMedia()
    : props.name.split("/").slice(1)[0] === "document"
    ? renderDocument()
    : renderDocument(); // Fallback to renderDocument for other types
};

export default Cards;
