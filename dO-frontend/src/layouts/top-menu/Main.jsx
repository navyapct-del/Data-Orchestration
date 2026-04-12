import { useState, useEffect, useContext } from "react";
import { Link, Outlet, useLocation, useNavigate } from "react-router-dom";
import { helper as $h } from "@/utils";
import { topMenu as useTopMenuStore } from "@/stores/top-menu";
import { useRecoilValue } from "recoil";
import { linkTo, nestedMenu } from "@/layouts/side-menu";
import {
  Lucide,
  Dropdown,
  DropdownToggle,
  DropdownMenu,
  DropdownContent,
  DropdownItem,
  DropdownHeader,
  DropdownDivider,
} from "@/base-components";
import classnames from "classnames";
import MobileMenu from "@/components/mobile-menu/Main";
import { AccountContext } from "../../config/Account";
import logoUrl from "@/assets/images/white-logo.png";
function Main() {
  const { getSession, logout } = useContext(AccountContext);
  const navigate = useNavigate();
  const location = useLocation();
  const [formattedMenu, setFormattedMenu] = useState([]);
  const [sessionDetails, setSessionDetails] = useState();
  const topMenuStore = useRecoilValue(useTopMenuStore);
  const topMenu = () => nestedMenu($h.toRaw(topMenuStore.menu), location);

  useEffect(() => {
    dom("body").removeClass("error-page").removeClass("login").addClass("main");
    setFormattedMenu(topMenu());
  }, [topMenuStore, location.pathname]);

  useEffect(() => { getSessionDetails(); }, []);

  const getSessionDetails = async () => {
    await getSession().then((data) => setSessionDetails(data));
  };

  const getInitials = () => {
    if (!sessionDetails) return "U";
    const name = sessionDetails["custom:Full_Name"] || sessionDetails["email"] || "";
    return name.split(" ").map((n) => n[0]).join("").toUpperCase().slice(0, 2);
  };

  return (
    <div style={{ minHeight: "100vh", background: "#f4f6f9", fontFamily: "'Segoe UI', system-ui, sans-serif" }}>
      <MobileMenu />
      <header style={{
        display: "flex", alignItems: "center",
        background: "#0d3347",
        padding: "0 24px", height: "64px",
        boxShadow: "0 2px 8px rgba(0,0,0,0.3)",
        position: "sticky", top: 0, zIndex: 100, gap: "8px",
      }}>
        {/* Logo — embedded as base64, no import needed */}
        <Link to="/top-menu/documentscontent" style={{ display: "flex", alignItems: "center", gap: "12px", textDecoration: "none", flexShrink: 0, marginRight: "16px" }}>
          <img
              src={logoUrl}
              alt="CloudThat"
              style={{ height: "28px", width: "auto", objectFit: "contain", display: "block" }}
          />
            
          
          
        </Link>

        {/* Nav */}
        <nav style={{ display: "flex", alignItems: "center", gap: "4px", flex: 1 }}>
          {formattedMenu.map((menu, menuKey) => {
            const isActive = menu.active;
            return (
              <a
                key={menuKey}
                href={menu.subMenu ? "#" : menu.pathname}
                style={{
                  display: "flex", alignItems: "center", gap: "7px",
                  padding: "8px 14px", borderRadius: "6px",
                  fontSize: "13px", fontWeight: "500",
                  color: isActive ? "#7ec8e3" : "rgba(255,255,255,0.65)",
                  background: isActive ? "rgba(126,200,227,0.15)" : "transparent",
                  borderBottom: isActive ? "2px solid #7ec8e3" : "2px solid transparent",
                  textDecoration: "none", cursor: "pointer",
                  transition: "all 0.15s", whiteSpace: "nowrap",
                }}
                onClick={(e) => { e.preventDefault(); linkTo(menu, navigate); }}
              >
                <Lucide icon={menu.icon} className="w-4 h-4" />
                {menu.title}
              </a>
            );
          })}
        </nav>

        {/* User */}
        <Dropdown>
          <DropdownToggle tag="div" role="button" style={{
            display: "flex", alignItems: "center", gap: "10px",
            background: "rgba(255,255,255,0.1)", border: "1px solid rgba(255,255,255,0.15)",
            borderRadius: "8px", padding: "6px 12px 6px 6px", cursor: "pointer",
          }}>
            <div style={{
              width: "32px", height: "32px", borderRadius: "6px",
              background: "#7ec8e3", color: "#0d3347",
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: "12px", fontWeight: "700", flexShrink: 0,
            }}>
              {getInitials()}
            </div>
            <div style={{ display: "flex", flexDirection: "column", lineHeight: 1.3 }}>
              <span style={{ fontSize: "12px", fontWeight: "600", color: "#fff" }}>
                {sessionDetails?.["custom:Full_Name"] || "User"}
              </span>
              <span style={{ fontSize: "10px", color: "rgba(255,255,255,0.45)", maxWidth: "160px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {sessionDetails?.["email"] || ""}
              </span>
            </div>
            <Lucide icon="ChevronDown" className="w-3 h-3" style={{ color: "rgba(255,255,255,0.5)" }} />

            <DropdownMenu className="w-52">
              <DropdownContent>
                <DropdownHeader tag="div" className="!font-normal">
                  <div style={{ fontSize: "13px", color: "#374151" }}>{sessionDetails?.["email"]}</div>
                </DropdownHeader>
                <DropdownDivider />
                <DropdownItem style={{ cursor: "pointer" }} onClick={logout}>
                  <Lucide icon="LogOut" className="w-4 h-4 mr-2" /> Logout
                </DropdownItem>
              </DropdownContent>
            </DropdownMenu>
          </DropdownToggle>
        </Dropdown>
      </header>

      <main style={{ padding: "28px", maxWidth: "1400px", margin: "0 auto" }}>
        <Outlet />
      </main>
    </div>
  );
}

export default Main;
