import React, { useState } from "react";
import { Modal, ModalBody, Lucide } from "@/base-components";
import downloadFile from "../../config/downloadfile";
import { useNavigate } from "react-router-dom";

const DocumentModal = ({
  showModal,
  onClose,
  cloudfrontFileLink,
  documentName,
  fileDetails,
  showPreview,
  setShowPreview,
}) => {
  const officelink = "https://view.officeapps.live.com/op/embed.aspx?src=";
  const navigate = useNavigate();
  // Function to get filename from the file path
  const getFilename = (filePath) => {
    const fullFileName = filePath.split("/").pop(); // Extract the full file name with extension
    return fullFileName.split(".").slice(0, -1).join("."); // Remove the extension
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
          {!showPreview ? (
            <div className="grid lg:grid-cols-12 sm:grid-cols-6 overflow-x-hidden">
              {/* File details and description */}
              <div className="lg:col-span-5 sm:col-span-6">
                <div className="w-3/5 file__icon file__icon--image mx-auto">
                  <img
                    src={documentName}
                    className="file__icon--image__preview image-fit rounded-lg"
                  />
                </div>
                <p className="mx-5 mt-2 pt-5 px-5 text-center text-md">
                  <span className="font-medium">Name: </span>
                  {getFilename(fileDetails.name)}
                </p>
                <p className="mx-5 my-2 pb-1 px-5 text-center text-md">
                  <span className="font-medium">Date: </span>
                  {fileDetails.date}
                </p>
              </div>
              <div className="lg:col-span-7 sm:col-span-6 px-2 py-4">
                <div className="grid grid-cols-12">
                  <div className="col-span-4">
                    <h1 className="relative text-md py-2">
                      <span className="font-medium">Description</span>
                    </h1>
                  </div>
                  <div className="col-span-8">
                    <h1 className="relative text-md py-2">
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
                  <div className="col-span-8">
                    <h1 className="relative text-md py-2">
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
                    <h1 className="relative text-md py-2">
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
                    <h1 className="relative text-md py-2">
                      <b>:</b> {fileDetails.size}
                    </h1>
                  </div>
                </div>
                <div className="grid grid-cols-6 md:grid-cols-12 lg:grid-cols-12 gap-8">
                  <div className="col-span-6 lg:col-span-4 md:col-span-6">
                    <button
                      onClick={() => downloadFile(cloudfrontFileLink)}
                      className="btn btn-primary align-top my-2 py-3 text-center text-md mt-10 lg:w-16 md:w-60 md:mx-auto xl:w-36 text-sm"
                    >
                      Click To Download
                    </button>
                  </div>
                  <div className="col-span-6 lg:col-span-4 md:col-span-6 sm:-mr-72">
                    <button
                      onClick={() => setShowPreview(true)}
                      className="btn btn-primary align-top my-2 py-3 text-center text-md mt-10 lg:w-16 md:w-60 md:mx-auto xl:w-36"
                    >
                      Click To Preview
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div className="w-full h-full">
              <button
                onClick={() => {
                  setShowPreview(false);
                  onClose(); // Close both modals
                  navigate("/top-menu/documentscontent");
                }}
                className="btn btn-secondary my-4"
              >
                Back
              </button>
              {fileDetails.type === "pdf" ? (
                <iframe
                  src={`${cloudfrontFileLink}#toolbar=0&navpanes=0&scrollbar=0&embedded=true`}
                  title="PDF Preview"
                  className="w-full"
                  style={{ height: "550px" }}
                />
              ) : fileDetails.type === "docx" || fileDetails.type === "doc" ? (
                <iframe
                  src={`${officelink}${cloudfrontFileLink}&embedded=true`}
                  title="Office Document Preview"
                  className="w-full"
                  style={{ height: "550px" }}
                />
              ) : (
                <iframe
                  src={cloudfrontFileLink}
                  title="File Preview"
                  className="w-full"
                  style={{ height: "550px" }}
                />
              )}
            </div>
          )}
        </div>
      </ModalBody>
    </Modal>
  );
};

export default DocumentModal;
