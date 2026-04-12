import { useRoutes } from "react-router-dom";
import TopMenu from "../layouts/top-menu/Main";
import Login from "../Pages/Login";
import Register from "../Pages/Register";
import ForgetPassword from "../Pages/ForgotPassword";
import ContentManager from "../Pages/ContentManager";
import ProtectedRoute from "./ProtectedRoute";
import InformationSage from "../Pages/InformationSage";
import SingleFileSathi from "../Pages/SingleFileSathi";

function Router() {
  const routes = [
    { path: "/", element: <Login /> },
    { path: "/register", element: <Register /> },
    { path: "/forgetpassword", element: <ForgetPassword /> },
    {
      path: "/top-menu",
      element: (
        <ProtectedRoute>
          <TopMenu />
        </ProtectedRoute>
      ),
      children: [
        { path: "filemanager",      element: <ContentManager type="file" /> },
        { path: "imagescontent",    element: <ContentManager type="image" /> },
        { path: "videoscontent",    element: <ContentManager type="video" /> },
        { path: "documentscontent", element: <ContentManager type="document" /> },
        { path: "mediacontent",     element: <ContentManager type="media" /> },
        { path: "singlefile",       element: <SingleFileSathi /> },
        { path: "informationsage",  element: <InformationSage /> },
      ],
    },
  ];
  return useRoutes(routes);
}

export default Router;
