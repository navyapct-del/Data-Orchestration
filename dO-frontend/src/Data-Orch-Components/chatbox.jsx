import React, { useContext, useState } from "react";
import { AccountContext } from "../components/Account";
import { LoadingIcon } from "@/base-components";
import { queryDocuments } from "../config/ApiCall";

const API_BASE = import.meta.env.VITE_AZURE_FUNCTIONS_URL || "http://localhost:7071/api";

const Chatbox = () => {
  const [userInput, setUserInput] = useState("");
  const { userdetails, userEmail } = useContext(AccountContext);

  const [chatData, setChatData] = useState([]);
  const [loading, setLoading] = useState(false); // Track loading state
  const [chatboxloader, setChatBoxLoader] = useState(false);

  const handleUserInput = (e) => {
    setUserInput(e.target.value);
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (userInput.trim() !== "") {
      setLoading(true); // Set loading state to true
      setChatBoxLoader(true);
      const newChatData = [
        ...chatData,
        {
          id: chatData.length + 1,
          sender: "user",
          message: userInput,
          question: true,
        },
      ];
      setChatData(newChatData);
      setUserInput("");

      queryDocuments(userInput)
        .then((res) => {
          if (!res) {
            const newBotChatData = [...newChatData, {
              message: "Sorry, we're currently experiencing issues fetching data. Please try again later.",
              noResponseMessage: true,
            }];
            setChatData(newBotChatData);
            return;
          }

          // Azure backend returns { type, answer, sources, data }
          const summaryData = { message: res.answer || "", summary: true };
          const sourceItems = (res.sources || []).map((src) => ({
            azureSource: true,
            filename: src.filename,
            blob_url: src.blob_url,
            summary: src.summary,
          }));

          setChatData([...newChatData, summaryData, ...sourceItems]);
        })
        .catch((err) => {
          console.log("error", err);
          setChatData([...newChatData, {
            message: "Sorry, something went wrong. Please try again.",
            noResponseMessage: true,
          }]);
        })
        .finally(() => {
          setLoading(false);
          setChatBoxLoader(false);
        });
    }
  };

  const handleReload = () => {
    // Implement reload functionality here
    // For example, you can reset chat data and clear input
    setChatData([]);
    setUserInput("");
  };

  return (
    <div className="chatbox-container overflow-auto max-h-96">
      <div className="chatbox">
        {/* Show loader if loading state is true */}
        {chatData?.map((chatItem, index) => {
          console.log("Chat Item:", chatItem);
          console.log("noResponseMessage:", chatItem?.noResponseMessage); // Add this console log

          if (chatItem?.question) {
            return (
              <div
                className="m-2 bg-primary text-white p-2 rounded-md"
                key={index}
              >
                {chatItem?.message}
              </div>
            );
          } else if (chatItem?.summary) {
            return (
              <div
                className="m-2 bg-secondary text-primary p-2 rounded-md"
                key={index}
              >
                {chatItem?.message}
              </div>
            );
          } else if (chatItem?.systemGeneratedResponse) {
            return (
              <span className="mx-auto px-5 font-bold" key={index}>
                {chatItem?.message}
              </span>
            );
          } else if (chatItem?.noResponseMessage) {
            console.log("Rendering noResponseMessage:", chatItem);
            return (
              <span
                className="mx-auto px-5 font-bold text-red-500 text-center"
                key={index}
              >
                {chatItem?.message}
              </span>
            );
          } else if (chatItem?.azureSource) {
            return (
              <div
                className="m-2 bg-secondary text-primary p-2 rounded-md cursor-pointer relative"
                key={index}
                onClick={() => window.open(chatItem.blob_url, "_blank")}
                style={{ position: "relative" }}
              >
                <span className="overflow-hidden whitespace-nowrap w-full block" title={chatItem.filename}>
                  {chatItem.filename?.length > 30
                    ? chatItem.filename.slice(0, 30) + "..."
                    : chatItem.filename}
                </span>
              </div>
            );
          } else {
            return chatItem?.res?.ResultItems?.map((result, index) => {
              const fileName = result?.DocumentTitle?.Text;
              const maxLength = 10;
              const truncatedFileName = fileName
                ? fileName.length > maxLength
                  ? fileName.slice(0, maxLength) + "..."
                  : fileName
                : "";

              return (
                <div
                  className="m-2 bg-secondary text-primary p-2 rounded-md cursor-pointer relative"
                  key={index}
                  onClick={() => {
                    window.open(result?.DocumentURI, "_blank");
                  }}
                  style={{ position: "relative" }}
                >
                  <span
                    className="overflow-hidden whitespace-nowrap w-full block"
                    title={fileName}
                    onMouseEnter={(e) => { e.target.title = fileName; }}
                  >
                    {truncatedFileName}
                  </span>
                </div>
              );
            });
          }
        })}
        {chatboxloader && (
          <div className="flex justify-start">
            <div className="bg-gray-200 py-2 px-4 rounded-lg my-3">
              <LoadingIcon icon="three-dots" className="w-8 h-8" />
            </div>
          </div>
        )}
      </div>

      <form onSubmit={handleSubmit}>
        <div className="mt-4 flex items-center overflow-auto max-h-96 py-5">
          <input
            type="text"
            placeholder="Type your question..."
            value={userInput}
            className="border border-gray-300 rounded-full py-2 px-4 w-full focus:outline-none focus:border-gray-300 focus:ring-transparent text-sm"
            onChange={handleUserInput}
          />
          <button
            type="submit"
            className="btn btn-primary text-white ml-2 py-2 px-4 rounded-full"
          >
            Send
          </button>
          <button
            type="submit"
            onClick={handleReload}
            className="btn btn-danger text-white ml-2 py-2 px-4 rounded-full"
          >
            Reset
          </button>
        </div>
      </form>
    </div>
  );
};

export default Chatbox;
