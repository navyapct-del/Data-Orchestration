import { CognitoUserPool } from "amazon-cognito-identity-js";

const poolData = {
  UserPoolId: "ap-south-1_DN6b4AGES",
  ClientId: "3uhi26nlvasnamim33tlc8c6e6",
};

export default new CognitoUserPool(poolData);
