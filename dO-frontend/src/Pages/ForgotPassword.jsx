import DarkModeSwitcher from "@/components/dark-mode-switcher/Main";
import dom from "@left4code/tw-starter/dist/js/dom";
import logoUrl from "@/assets/images/white-logo.png";
import illustrationUrl from "@/assets/images/illustration.svg";
import { CognitoUser } from "amazon-cognito-identity-js";
import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import Pool from "../UserPool";

function Main() {
  const [stage, setStage] = useState(1);
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  let navigate = useNavigate();
  const getUser = () => {
    return new CognitoUser({
      Username: email.toLowerCase(),
      Pool,
    });
  };

  const sendCode = (event) => {
    event.preventDefault();

    getUser().forgotPassword({
      onSuccess: (data) => {
        console.log("onSuccess:", data);
      },
      onFailure: (err) => {
        console.error("onFailure:", err);
      },
      inputVerificationCode: (data) => {
        console.log("Input code:", data);
        setStage(2);
      },
    });
  };

  const resetPassword = (event) => {
    event.preventDefault();

    if (password !== confirmPassword) {
      console.error("Passwords are not the same");
      return;
    }

    getUser().confirmPassword(code, password, {
      onSuccess: (data) => {
        console.log("onSuccess:", data);
        navigate("/");
      },
      onFailure: (err) => {
        console.error("onFailure:", err);
      },
    });
  };

  useEffect(() => {
    dom("body").removeClass("main").removeClass("error-page").addClass("login");
  }, []);

  return (
    <>
      <div>
        {/* <DarkModeSwitcher /> */}
        <div className="container sm:px-10">
          <div className="block xl:grid grid-cols-2 gap-4">
            {/* BEGIN: Register Info */}
            <div className="hidden xl:flex flex-col min-h-screen">
              <a href="/" className="-intro-x flex items-center pt-5">
                <img
                  alt="Midone Tailwind HTML Admin Template"
                  className="w-auto h-6"
                  src={logoUrl}
                />
              </a>
              <div className="my-auto">
                <img
                  alt="Midone Tailwind HTML Admin Template"
                  className="-intro-x w-1/2 -mt-16"
                  src={illustrationUrl}
                />
                <div className="-intro-x text-white font-medium text-4xl leading-tight mt-10">
                  A few more clicks to <br />
                  sign up to your account.
                </div>
                <div className="-intro-x mt-5 text-lg text-white text-opacity-70 dark:text-slate-400">
                  Manage all your data in one place
                </div>
              </div>
            </div>
            <div className="h-screen xl:h-auto flex py-5 xl:py-0 my-10 xl:my-0">
              <div className="my-auto mx-auto xl:ml-20 bg-white dark:bg-darkmode-600 xl:bg-transparent px-5 sm:px-8 py-8 xl:p-0 rounded-md shadow-md xl:shadow-none w-full sm:w-3/4 lg:w-2/4 xl:w-auto">
                <h2 className="intro-x font-bold text-2xl xl:text-3xl text-center xl:text-left">
                  Reset Password
                </h2>
                <div className="intro-x mt-2 text-slate-400 dark:text-slate-400 xl:hidden text-center">
                  A few more clicks to sign in to your account. Manage all your
                  e-commerce accounts in one place
                </div>
                {stage === 1 && (
                  <form
                    className="mt-6"
                    action="#"
                    method="POST"
                    onSubmit={sendCode}
                  >
                    <div className="intro-x mt-8">
                      <input
                        type="email"
                        className="intro-x login__input form-control py-3 px-4 block mt-5"
                        placeholder="Email"
                        autoComplete="username"
                        value={email}
                        onChange={(event) => setEmail(event.target.value)}
                      />
                      <button className="btn btn-primary py-3 px-4 w-full xl:w-48 xl:mr-3 align-top mt-5">
                        Send Verification Code
                      </button>
                    </div>
                  </form>
                )}
                {stage === 2 && (
                  <form onSubmit={resetPassword}>
                    <input
                      className="intro-x login__input form-control py-3 px-4 block mt-6"
                      placeholder="Enter Code"
                      value={code}
                      onChange={(event) => setCode(event.target.value)}
                    />
                    <input
                      className="intro-x login__input form-control py-3 px-4 block mt-6"
                      placeholder="Enter Password"
                      type="password"
                      value={password}
                      onChange={(event) => setPassword(event.target.value)}
                    />
                    <input
                      className="intro-x login__input form-control py-3 px-4 block mt-6"
                      placeholder="Enter Confirm Password"
                      type="password"
                      value={confirmPassword}
                      onChange={(event) =>
                        setConfirmPassword(event.target.value)
                      }
                    />
                    <button
                      type="submit"
                      className="btn btn-primary py-3 px-4 w-full xl:w-48 xl:mr-3 align-top mt-6"
                    >
                      Change password
                    </button>
                  </form>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

export default Main;
