import React, { useState } from "react";
import { Modal, ModalBody, Lucide } from "@/base-components";
import { useNavigate } from "react-router-dom";

const getFilename = (filePath) => {
  const fullFileName = filePath.split("/").pop();
  return fullFileName.split(".").slice(0, -1).join(".");
};

const ImageModal = ({
  showModal,
  onClose,
  cloudfrontFileLink,
  fileDetails,
}) => {
  const navigate = useNavigate();

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
              <img
                src={cloudfrontFileLink}
                className="lg:w-96 lg:h-72 sm:w-72 sm:h-64 sm:mx-2 md:w-full md:px-8 md:mt-5 lg:mx-5 my-2 rounded-md file__icon--image__preview image-fit w-3/5 file__icon file__icon--image mx-auto "
              />

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

export default ImageModal;
