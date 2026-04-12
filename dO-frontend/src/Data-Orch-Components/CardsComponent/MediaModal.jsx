import React from "react";
import videojs from "video.js";
import "video.js/dist/video-js.css";
import "videojs-contrib-quality-levels";
import "videojs-http-source-selector";

const MediaModal = ({
  showModal,
  onClose,
  cloudfrontFileLink,
  fileDetails,
  subtitle,
}) => {
  const videoOptions = {
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
    height: "70%",
    width: "100%",
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
    poster: cloudfrontFileLink,
  };

  if (!showModal) return null;

  return (
    <div className="modal">
      <div className="modal-content">
        <span className="close" onClick={onClose}>
          &times;
        </span>
        <div className="media-content">
          <video
            id="media-player"
            className="video-js vjs-default-skin"
            controls
            playsInline
            preload="auto"
            data-setup="{}"
          >
            <source src={cloudfrontFileLink} type="application/x-mpegURL" />
            {subtitle && (
              <track
                kind="captions"
                src={subtitle}
                srcLang="en"
                label="English"
                default
              />
            )}
          </video>
        </div>
        <div className="file-details">
          <h3>{fileDetails.name}</h3>
          <p>{fileDetails.description}</p>
          <p>{fileDetails.tags}</p>
          <p>{fileDetails.size}</p>
        </div>
      </div>
    </div>
  );
};

export default MediaModal;
