import React, { useState, useEffect } from "react";
import {
  createKendraIndex,
  getKendraStatus,
  getStepFunctionStatus,
  deleteKendraIndex,
} from "../../config/ApiCall";
import { LoadingIcon } from "@/base-components";

function Kendra({ onStatusChange }) {
  const [kendraCurrentStatus, setKendraCurrentStatus] =
    useState("Index Not Found");
  const [stepFunctionStatus, setStepFunctionStatus] =
    useState("Index Not Found");
  const [kendraButtonText, setKendraButtonText] = useState("Create Index");
  const [isLoading, setIsLoading] = useState(false);

  const fetchKendraStatus = async () => {
    setIsLoading(true);
    try {
      const data = await getKendraStatus();
      console.log("Kendra Status:", data.status);
      setKendraCurrentStatus(data.status);
      onStatusChange(data.status);
      if (data.status === "Index Not Found") {
        setKendraButtonText("Create Index");
      } else if (data.status === "CREATING") {
        setKendraButtonText("Creating Index");
      } else if (data.status === "ACTIVE") {
        await checkSFStatus();
      }
    } catch (error) {
      console.error("Error fetching Kendra status:", error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchKendraStatus();
  }, []);

  const createKendraInstance = async () => {
    setIsLoading(true);
    try {
      const response = await createKendraIndex();
      if (response.statusCode === 200) {
        console.log("Step function started:", response.body);
        // Assume index is in CREATING state after starting step function
        setKendraCurrentStatus("CREATING");
        setKendraButtonText("Creating Index");
      } else {
        console.error("Unexpected response:", response);
      }
    } catch (error) {
      console.error("Error creating Kendra index:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const checkSFStatus = async () => {
    try {
      const data = await getStepFunctionStatus();
      console.log("Step Function Status:", data.status);
      setStepFunctionStatus(data.status);
      if (data.status === "RUNNING") {
        setKendraCurrentStatus("CREATING");
        setKendraButtonText("Creating Index");
      } else if (data.status === "SUCCEEDED") {
        setKendraCurrentStatus("ACTIVE");
        setKendraButtonText("Delete Index");
      } else {
        setKendraCurrentStatus("Index Not Found");
        setKendraButtonText("Create Index");
      }
    } catch (error) {
      console.error("Error checking step function status:", error);
    }
  };

  const deleteKendraInstance = async () => {
    setIsLoading(true);
    try {
      const data = await deleteKendraIndex();
      if (data.Status === 200) {
        setKendraCurrentStatus("Index Not Found");
        setKendraButtonText("Create Index");
      }
    } catch (error) {
      console.error("Error deleting Kendra index:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKendraButtonClick = () => {
    console.log("Current Status on Click:", kendraCurrentStatus);
    if (kendraCurrentStatus === "Index Not Found") {
      createKendraInstance();
    } else if (kendraCurrentStatus === "CREATING") {
      checkSFStatus();
    } else if (
      kendraCurrentStatus === "ACTIVE" &&
      stepFunctionStatus === "SUCCEEDED"
    ) {
      deleteKendraInstance();
    }
  };

  return (
    <>
      <button
        className={`btn shadow-md mr-2 text-xs lg:text-base text-white ${
          kendraCurrentStatus === "Index Not Found"
            ? "bg-blue-800"
            : kendraCurrentStatus === "ACTIVE" &&
              stepFunctionStatus === "SUCCEEDED"
            ? "bg-red-800"
            : "bg-cyan-900"
        } ${isLoading ? "cursor-not-allowed" : ""}`}
        onClick={handleKendraButtonClick}
        disabled={
          kendraCurrentStatus === "CREATING" ||
          (kendraCurrentStatus === "ACTIVE" && stepFunctionStatus === "RUNNING")
        }
      >
        {kendraButtonText}
        {kendraCurrentStatus === "CREATING" && (
          <LoadingIcon
            icon="tail-spin"
            className="w-10 h-7 lg:ml-3 stroke-white stroke-2"
          />
        )}
      </button>
    </>
  );
}

export default Kendra;
