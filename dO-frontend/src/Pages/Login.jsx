import DarkModeSwitcher from "@/components/dark-mode-switcher/Main";
import dom from "@left4code/tw-starter/dist/js/dom";
import logoUrl from "@/assets/images/white-logo.png";
import illustrationUrl from "@/assets/images/illustration.svg";
import { AuthenticationDetails, CognitoUser } from "amazon-cognito-identity-js";
import React, { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import UserPool from "../UserPool";

function Main() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [iserror, setIserror] = useState("");

  let navigate = useNavigate();

  const onSubmit = (event) => {
    event.preventDefault();

    const user = new CognitoUser({
      Username: email,
      Pool: UserPool,
    });
    const authDetails = new AuthenticationDetails({
      Username: email,
      Password: password,
    });

    user.authenticateUser(authDetails, {
      onSuccess: (data) => {
        navigate("/top-menu/documentscontent");
      },
      onFailure: (err) => {
        console.error("onFailure: ", err);
        setIserror(
          <h1 className="text-red-500 mt-3 text-center">
            Incorrect username or password.
          </h1>
        );
      },
      newPasswordRequired: (data) => {
        console.log("newPasswordRequired: ", data);
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
            <div className="hidden xl:flex flex-col min-h-screen">
              <a href="" className="-intro-x flex items-center pt-5">
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
                  sign in to your account.
                </div>
                <div className="-intro-x mt-5 text-lg text-white text-opacity-70 dark:text-slate-400">
                  Manage all your data in one place
                </div>
              </div>
            </div>

            <div className="h-screen xl:h-auto flex py-5 xl:py-0 my-10 xl:my-0">
              <div className="my-auto mx-auto xl:ml-20 bg-white dark:bg-darkmode-600 xl:bg-transparent px-5 sm:px-8 py-8 xl:p-0 rounded-md shadow-md xl:shadow-none w-full sm:w-3/4 lg:w-2/4 xl:w-auto">
                <h2 className="intro-x font-bold text-2xl xl:text-3xl text-center xl:text-left">
                  Sign In
                </h2>
                <div className="intro-x mt-2 text-slate-400 xl:hidden text-center">
                  A few more clicks to sign in to your account. Manage all your
                  e-commerce accounts in one place
                </div>
                <form className="mt-6" onSubmit={onSubmit}>
                  <div className="intro-x mt-8">
                    <input
                      type="text"
                      className="intro-x login__input form-control py-3 px-4 block"
                      placeholder="Email"
                      value={email}
                      onChange={(event) => setEmail(event.target.value)}
                    />
                    <input
                      type="password"
                      className="intro-x login__input form-control py-3 px-4 block mt-4"
                      placeholder="Password"
                      value={password}
                      onChange={(event) => setPassword(event.target.value)}
                    />
                  </div>
                  <div>{iserror}</div>
                  <div className="intro-x flex text-slate-600 dark:text-slate-500 text-xs sm:text-sm mt-4">
                    <div className="flex items-center mr-auto">
                      <input
                        id="remember-me"
                        type="checkbox"
                        className="form-check-input border mr-2"
                      />
                      <label
                        className="cursor-pointer select-none"
                        htmlFor="remember-me"
                      >
                        Remember me
                      </label>
                    </div>
                    <a href="/forgetpassword">Forgot Password?</a>
                  </div>
                  <div className="intro-x mt-5 xl:mt-8 text-center xl:text-left">
                    <button className="btn bg-cyan-900 text-white py-3 px-4 w-full xl:w-32 xl:mr-3 align-top">
                      Login
                    </button>
                    <button
                      className="btn btn-outline-secondary py-3 px-4 w-full xl:w-32 mt-3 xl:mt-0 align-top"
                      onClick={() => {
                        setIserror("");
                        navigate("/register");
                      }}
                    >
                      Register
                    </button>
                  </div>
                  <div className="intro-x mt-10 xl:mt-8 text-slate-600 dark:text-slate-500 text-center xl:text-left tracking-wide">
                    By signin up, you agree to our&nbsp;&nbsp;
                    <a className="text-orange-500 dark:text-slate-200" href="">
                      Terms and Conditions&nbsp;&nbsp;
                    </a>
                    &&nbsp;&nbsp;
                    <a className="text-primary dark:text-slate-200" href="">
                      Privacy Policy
                    </a>
                  </div>
                </form>
              </div>
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

export default Main;
