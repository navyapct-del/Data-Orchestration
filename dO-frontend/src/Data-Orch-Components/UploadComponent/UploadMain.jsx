import React from "react";
import ImageUpload from "./Image/ImageUpload";
import VideoUpload from "./Video/VideoUpload.jsx";
import DocumentUpload from "./Document/DocumentUpload";

function Upload(props) {
  const { type } = props;

  return (
    <div className="upload-component">
      {type === "image" && <ImageUpload {...props} />}
      {type === "video" && <VideoUpload {...props} />}
      {type === "document" && <DocumentUpload {...props} />}
    </div>
  );
}

export default Upload;
