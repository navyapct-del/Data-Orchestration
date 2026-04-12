import React, { createContext, useState, useEffect } from "react";
import { CognitoUser, AuthenticationDetails } from "amazon-cognito-identity-js";
import Pool from "../UserPool";

const AccountContext = createContext();

const Account = (props) => {
  const [userdetails, setUserdetails] = useState({});
  const [userEmail, setUserEmail] = useState("");
  const [userName, setUserName] = useState("");

  useEffect(() => {
    getSession();
  }, []);

  const getSession = async () =>
    await new Promise((resolve, reject) => {
      const user = Pool.getCurrentUser();
      if (user) {
        user.getSession(async (err, session) => {
          if (session) {
            setUserEmail(session.idToken.payload.email);
            setUserName(session.idToken.payload["custom:Full_Name"]);
            setUserdetails(session);
          }
          if (err) {
            reject();
          } else {
            const attributes = await new Promise((resolve, reject) => {
              user.getUserAttributes((err, attributes) => {
                if (err) {
                  reject(err);
                } else {
                  const results = {};

                  for (let attribte of attributes) {
                    const { Name, Value } = attribte;

                    results[Name] = Value;
                  }

                  setUserdetails({
                    ...userdetails,
                    userattributes: results,
                  });
                  resolve(results);
                }
              });
            });

            resolve({
              user,
              ...session,
              ...attributes,
            });

            setUserdetails(session.getIdToken());
          }
        });
      } else {
        reject();
      }
    });

  const authenticate = async (Username, Password) =>
    await new Promise((resolve, reject) => {
      const user = new CognitoUser({ Username, Pool });
      const authDetails = new AuthenticationDetails({ Username, Password });

      user.authenticateUser(authDetails, {
        onSuccess: (data) => {
          resolve(data);
        },
        onFailure: (err) => {
          reject(err);
        },
      });
    });

  const logout = () => {
    const user = Pool.getCurrentUser();
    if (user) {
      user.signOut();
      window.location.href = "/";
    }
  };

  return (
    <AccountContext.Provider
      value={{
        authenticate,
        getSession,
        logout,
        userdetails,
        userEmail,
        userName,
      }}
    >
      {props.children}
    </AccountContext.Provider>
  );
};

export { Account, AccountContext };
