import React, { useEffect, useRef, useState } from "react";
import { Modal, ModalBody, Lucide } from "@/base-components";
import videojs from "video.js";
import "video.js/dist/video-js.css";
import "videojs-contrib-quality-levels";
import "videojs-http-source-selector";

const VideoModal = ({ showModal, onClose, videoSrc, fileDetails }) => {
  const videoRef = useRef(null);

  useEffect(() => {
    if (showModal) {
      const player = videojs(videoRef.current, {
        sources: [
          {
            src: videoSrc,
            type: "application/x-mpegURL",
          },
        ],
        controls: true,
        responsive: true,
        fluid: true,
        playbackRates: [0.5, 1, 1.5, 2],
        height: 300,
        width: 600,
        controlBar: {
          pictureInPictureToggle: false,
        },
      });

      return () => {
        if (player) {
          player.dispose();
        }
      };
    }
  }, [showModal, videoSrc]);

  const getFilename = (filePath) => {
    const fullFileName = filePath.split("/").pop();
    return fullFileName.split(".").slice(0, -1).join(".");
  };

  return (
    <Modal
      size="modal-xl"
      backdrop="static"
      show={showModal}
      onHidden={onClose}
    >
      <ModalBody>
        <div>
          <a
            onClick={onClose}
            className="absolute right-0 top-0 mt-3 mr-3"
            href="#"
          >
            <Lucide
              icon="X"
              className="w-5 h-5 text-slate-400 xl:w-8 xl:h-8 lg:w-8 lg:h-8 md:w-6 md:h-6"
            />
          </a>
          <div className="grid grid-cols-6 xl:grid-cols-12 lg:grid-cols-12 overflow-x-hidden">
            <div className="col-span-6 xl:col-span-6 lg:col-span-6">
              <video
                ref={videoRef}
                className="video-js vjs-default-skin"
                controls
              >
                <source src={videoSrc} type="application/x-mpegURL" />
              </video>

              <p className="mx-5 mt-2 pt-5 px-5 text-center text-md">
                <span className="font-medium">Name :</span>
                {getFilename(fileDetails.name)}
              </p>
              <p className="mx-5 my-2 pb-1 px-5 text-center text-md">
                <span className="font-medium">Date : </span>
                {fileDetails.date}
              </p>
            </div>
            <div className="xl:col-span-6 lg:col-span-6 md:col-span-6 col-span-6 px-2 py-4">
              <div className="grid grid-cols-12">
                <div className="col-span-4">
                  <h1 className="relative text-md py-2">
                    <span className="font-medium">Description</span>
                  </h1>
                </div>
                <div className="col-span-8">
                  <h1 className="relative text-md py-2">
                    <span className="font-medium"></span>
                    <b>:</b> {fileDetails.description}
                  </h1>
                </div>
              </div>

              <div className="grid grid-cols-12">
                <div className="col-span-4">
                  <h1 className="relative text-md py-2">
                    <span className="font-medium">Tags</span>
                  </h1>
                </div>
                <div className="col-span-8 text-clip overflow-hidden">
                  <h1 className="relative text-md py-2">
                    <span className="font-medium"></span>
                    <b>:</b> {fileDetails.tags}
                  </h1>
                </div>
              </div>
              <div className="grid grid-cols-12">
                <div className="col-span-4">
                  <h1 className="relative text-md py-2">
                    <span className="font-medium">File Type</span>
                  </h1>
                </div>
                <div className="col-span-8">
                  <h1 className=" relative text-md py-2">
                    <span className="font-medium"></span>
                    <b>:</b> {fileDetails.type}
                  </h1>
                </div>
              </div>
              <div className="grid grid-cols-12">
                <div className="col-span-4">
                  <h1 className="relative text-md py-2">
                    <span className="font-medium">Size</span>
                  </h1>
                </div>
                <div className="col-span-8">
                  <h1 className=" relative text-md py-2">
                    <span className="font-medium"></span>
                    <b>:</b> {fileDetails.size}
                  </h1>
                </div>
              </div>
            </div>
          </div>
        </div>
      </ModalBody>
    </Modal>
  );
};

export default VideoModal;
