import { atom } from "recoil";

const topMenu = atom({
  key: "topMenu",
  default: {
    menu: [
      {
        icon: "Folder",
        pathname: "/top-menu/documentscontent",
        title: "Files Knowledge Sathi",
      },
      {
        icon: "MessageSquare",
        pathname: "/top-menu/informationsage",
        title: "Bot Knowledge Sathi",
      },
      {
        icon: "FileText",
        pathname: "/top-menu/singlefile",
        title: "Files Knowledge Bot",
      },
    ],
  },
});

export { topMenu };
