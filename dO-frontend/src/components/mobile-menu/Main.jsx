import { Transition } from "react-transition-group";
import { useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { helper as $h } from "@/utils";
import { sideMenu as useSideMenuStore } from "@/stores/side-menu";
import { useRecoilValue } from "recoil";
import { nestedMenu } from "@/layouts/side-menu";
import { toggleMobileMenu, linkTo, enter, leave } from "./index";
import { Lucide } from "@/base-components";
import logoUrl from "@/assets/images/white-logo.png";
import classnames from "classnames";

function Main(props) {
  const navigate = useNavigate();
  const location = useLocation();
  const [formattedMenu, setFormattedMenu] = useState([]);
  const sideMenuStore = useRecoilValue(useSideMenuStore);
  const mobileMenu = () => nestedMenu($h.toRaw(sideMenuStore.menu), location);
  const [activeMobileMenu, setActiveMobileMenu] = useState(false);

  useEffect(() => {
    setFormattedMenu(mobileMenu());
  }, [sideMenuStore, location.pathname]);

  return (
    <>
      {/* BEGIN: Mobile Menu */}
      <div className="mobile-menu md:hidden">
        <div className="mobile-menu-bar">
          <a href="" className="flex mr-auto">
            <img
              alt="Midone Tailwind HTML Admin Template"
              className="w-auto h-4"
              src={logoUrl}
            />
          </a>
          <a
            href="#"
            onClick={(e) => e.preventDefault()}
            id="mobile-menu-toggler"
          >
            <Lucide
              icon="BarChart2"
              className="w-8 h-8 text-white transform -rotate-90"
              onClick={() => {
                toggleMobileMenu(activeMobileMenu, setActiveMobileMenu);
              }}
            />
          </a>
        </div>
        <Transition
          in={activeMobileMenu}
          onEnter={enter}
          onExit={leave}
          timeout={300}
        >
          <ul className="border-t border-white/[0.08] py-5 hidden">
                <li>
                  <a
                    href="/top-menu/dashboard"
                  >
                      <span className="inline-flex relative ml-5">
                      <Lucide icon="Home" className="text-white"/><h1 className="text-white ml-3">Dashboard</h1></span>
                  </a>
                </li>
                <li className="menu__devider my-5"></li>
                <li>
                  <a
                    href="/top-menu/filemanager"
                  >
                      <span className="inline-flex relative ml-5">
                      <Lucide icon="File" className="text-white"/><h1 className="text-white ml-3">File Manager</h1></span>
                  </a>
                </li>
                <li className="menu__devider my-5"></li>
                <li>
                  <a
                    href="/top-menu/imagescontent"
                  >
                      <span className="inline-flex relative ml-5">
                      <Lucide icon="Image" className="text-white"/><h1 className="text-white ml-3">Images</h1></span>
                  </a>
                </li>
                <li className="menu__devider my-5"></li>
                <li>
                  <a
                    href="/top-menu/videoscontent"
                  >
                    <span className="inline-flex relative ml-5">
                      <Lucide icon="Video" className="text-white"/><h1 className="text-white ml-3">Videos</h1></span>
                  </a>
                </li>
          </ul>
        </Transition>
        
      </div>
      {/* END: Mobile Menu */}
    </>
  );
}

export default Main;
