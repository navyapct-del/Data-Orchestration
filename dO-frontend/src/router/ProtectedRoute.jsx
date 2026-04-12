import React from "react";
import { Navigate } from "react-router-dom";
import { CognitoUserPool } from "amazon-cognito-identity-js";
import UserPool from "../../src/UserPool";

const ProtectedRoute = ({ children }) => {
  let authorization = false;
  const auth = new CognitoUserPool({
    UserPoolId: UserPool.userPoolId,
    ClientId: UserPool.clientId,
  });
  let user = auth.getCurrentUser();
  if (user != null) {
    // console.log("user", user);
    authorization = true;
  }
  return authorization == true ? children : <Navigate to="/"></Navigate>;
};
export default ProtectedRoute;
