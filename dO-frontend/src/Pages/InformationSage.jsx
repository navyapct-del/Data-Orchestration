import React, { useState } from "react";
import ChatInfoSage from "../Data-Orch-Components/ChatInfoSage";
import SymphonyChatbot from "./SymphonyChatbot";

function InformationSage() {
  const [showChatbot, setShowChatbot] = useState(false);

  const handleToggleChatbot = () => {
    setShowChatbot(!showChatbot);
  };

  return (
    <div>
      <SymphonyChatbot />
      <div className="bg-gray-100 flex flex-col justify-end">
        <button
          onClick={handleToggleChatbot}
          className={`fixed bottom-8 right-8 md:right-10 p-0 w-14 h-14 flex items-center justify-center rounded-full bg-cyan-900 transition-all duration-200 ease-in-out shadow-lg focus:outline-none ${
            showChatbot ? "animate-pulse" : ""
          } ${showChatbot ? "transform rotate-90" : ""}`}
          style={{
            borderBottom: "1px dotted black",
            animation: showChatbot ? "none" : "pulse 1s infinite",
          }}
        >
          <span
            className={`absolute transition-opacity duration-300 ease-in-out ${
              showChatbot ? "opacity-0" : "opacity-100"
            }`}
          >
            {/* Chatbot icon */}
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
              <circle cx="9" cy="10" r="1" fill="white"/><circle cx="12" cy="10" r="1" fill="white"/><circle cx="15" cy="10" r="1" fill="white"/>
            </svg>
          </span>
          <span
            className={`absolute transition-opacity duration-300 ease-in-out ${
              showChatbot ? "opacity-100" : "opacity-0"
            }`}
          >
            {/* Close icon */}
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
              <line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
            </svg>
          </span>
        </button>

        {showChatbot && <ChatInfoSage />}
      </div>

      {/* Adding the keyframes animation via inline styles */}
      <style>{`
        @keyframes pulse {
          0% {
            box-shadow: 0 0 0 0 rgba(20, 80, 210, 0.4);
          }
          70% {
            box-shadow: 0 0 0 10px rgba(20, 80, 210, 0);
          }
          100% {
            box-shadow: 0 0 0 0 rgba(20, 80, 210, 0);
          }
        }
      `}</style>
    </div>
  );
}

export default InformationSage;
