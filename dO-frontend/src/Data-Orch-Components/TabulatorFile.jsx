import pdf_Url from "../assets/images/pdf.png";
import docx_Url from "../assets/images/docx.png";
import text_Url from "../assets/images/text.png";
import { useEffect, createRef, useRef, useState } from "react";
import Tabulator from "tabulator-tables";
import { createIcons, icons } from "lucide";
import ReactPaginate from "react-paginate";
import { Lucide } from "@/base-components";

function TabulatorFile({ data }) {
  const tableRef = createRef();
  const tabulator = useRef();
  const [pageCount, setPageCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(0);
  const pageSize = 10; // Number of items per page

  const extractFileName = (path) => {
    const fileName = path.split("/").pop();
    return fileName.split(".").slice(0, -1).join(".");
  };

  const formatDate = (dateString) => {
    const date = new Date(dateString);

    const day = String(date.getDate()).padStart(2, "0");
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const year = date.getFullYear();

    const hours = String(date.getHours() % 12 || 12).padStart(2, "0");
    const minutes = String(date.getMinutes()).padStart(2, "0");
    const seconds = String(date.getSeconds()).padStart(2, "0");

    const ampm = date.getHours() >= 12 ? "pm" : "am";

    return `${day}-${month}-${year} ${hours}:${minutes}:${seconds} ${ampm}`;
  };

  const formatTags = (tags) => {
    let cleanedTags = tags.replace(/{|}|'/g, "");
    let tagsArray = cleanedTags.split(",").map((tag) => tag.trim());

    if (tagsArray[0] === "") {
      tagsArray.shift();
    }

    return tagsArray.join(", ");
  };

  const getImageUrlByType = (type) => {
    switch (type.toLowerCase()) {
      case "pdf":
        return pdf_Url;
      case "doc":
      case "docx":
        return docx_Url;
      case "text":
        return text_Url;
      default:
        return ""; // Return a default or empty string if no match is found
    }
  };

  const initTabulator = () => {
    tabulator.current = new Tabulator(tableRef.current, {
      layout: "fitColumns",
      responsiveLayout: "collapse",
      data: data.slice(0, pageSize), // Initialize with first page data
      pagination: false,
      paginationSize: pageSize,
      paginationDataSent: {
        page: "page",
        size: "pageSize",
      },
      paginationDataReceived: {
        page: "page",
        size: "pageSize",
        count: "totalPages",
      },
      columns: [
        {
          formatter: "responsiveCollapse",
          width: 40,
          minWidth: 30,
          hozAlign: "center",
          resizable: false,
          headerSort: false,
        },
        {
          title: "Icon",
          minWidth: 40,
          responsive: 0,
          field: "name",
          vertAlign: "middle",
          print: false,
          download: false,
          formatter: (cell) => {
            const type = cell.getData().type || "";
            const imageUrl = getImageUrlByType(type);
            const imageName = cell.getValue();
            const cloudfrontLink = `https://dwmovw8jpo7wl.cloudfront.net/${imageName.replace(
              / /g,
              "%20"
            )}`;
            // const cloudfrontLink = `https://d3te6sdt3808us.cloudfront.net/${imageName.replace(
            //   / /g,
            //   "%20"
            // )}`;

            return `<div class="flex lg:justify-center">
                      <div class="intro-x w-10 h-10 image-fit">
                        <img alt="File Icon" class="rounded-md"
                         src="${
                           imageUrl || cloudfrontLink
                         }" alt="${imageName}" />
                      </div>
                    </div>`;
          },
        },
        {
          title: "Name",
          minWidth: 200,
          responsive: 0,
          field: "name",
          vertAlign: "middle",
          hozAlign: "center",
          print: false,
          download: false,
          formatter(cell) {
            const name = cell.getData().name || "N/A";
            const formattedName = extractFileName(name);
            const truncatedName =
              formattedName.length > 20
                ? formattedName.slice(0, 20) + "..."
                : formattedName;

            return `<div>
              <div class="whitespace-nowrap" title="${formattedName}">
                ${truncatedName}
              </div>
            </div>`;
          },
        },

        {
          title: "Type",
          minWidth: 20,
          field: "type",
          hozAlign: "center",
          vertAlign: "middle",
          print: false,
          download: false,
          formatter(cell) {
            return `<div class="flex lg:justify-center">
              ${cell.getData().type || "N/A"}
              </div>`;
          },
        },
        {
          title: "Size",
          minWidth: 20,
          field: "size",
          hozAlign: "center",
          vertAlign: "middle",
          print: false,
          download: false,
          formatter(cell) {
            return `<div class="flex lg:justify-center">
              ${cell.getData().size || "N/A"}
              </div>`;
          },
        },
        {
          title: "Date",
          minWidth: 200,
          field: "type",
          hozAlign: "center",
          vertAlign: "middle",
          print: false,
          download: false,
          formatter(cell) {
            const date = cell.getData().date || "";
            const formattedDate = formatDate(date);
            return `<div class="whitespace-nowrap">${formattedDate}</div>`;
          },
        },
        {
          title: "Description",
          minWidth: 200,
          field: "description",
          hozAlign: "center",
          // vertAlign: "middle",
          print: false,
          download: false,
          formatter(cell) {
            return `<div class="flex lg:justify-center">
              ${cell.getData().description || "N/A"}
              </div>`;
          },
        },
        {
          title: "Tags",
          minWidth: 200,
          field: "tags",
          vertAlign: "middle",
          print: false,
          download: false,
          formatter(cell) {
            const tags = cell.getData().tags || "";
            const formattedTags = formatTags(tags);
            const truncatedTags =
              formattedTags.length > 20
                ? formattedTags.slice(0, 20) + "..."
                : formattedTags;

            return `<div>
                     <div class="whitespace-nowrap" title="${formattedTags}">
                     ${truncatedTags}
                     </div>
                    </div>`;
          },
        },
      ],
      renderComplete() {
        if (typeof createIcons === "function") {
          createIcons({
            icons,
            "stroke-width": 1.5,
            nameAttr: "data-lucide",
          });
        }
      },
    });
  };

  useEffect(() => {
    initTabulator();

    const handleResize = () => {
      tabulator.current.redraw();
      if (typeof createIcons === "function") {
        createIcons({
          icons,
          "stroke-width": 1.5,
          nameAttr: "data-lucide",
        });
      }
    };

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
    };
  }, [data]);

  useEffect(() => {
    if (tabulator.current) {
      const startIndex = currentPage * pageSize;
      const endIndex = startIndex + pageSize;
      tabulator.current.setData(data.slice(startIndex, endIndex));
      setPageCount(Math.ceil(data.length / pageSize));
    }
  }, [data, currentPage]);

  const handlePageChange = ({ selected }) => {
    setCurrentPage(selected);
  };

  return (
    <div className="intro-y box p-5 mt-5">
      <div className="overflow-x-auto scrollbar-hidden">
        <div
          id="tabulator"
          ref={tableRef}
          className="mt-5 table-report table-report--tabulator"
        ></div>
      </div>
      <div className="intro-y flex flex-wrap sm:flex-row sm:flex-nowrap items-center mt-6">
        <nav className="w-full sm:w-auto sm:mr-auto">
          <ReactPaginate
            previousLabel={<Lucide icon="ChevronLeft" className="w-4 h-4" />}
            nextLabel={<Lucide icon="ChevronRight" className="w-4 h-4" />}
            breakLabel="..."
            pageCount={pageCount}
            marginPagesDisplayed={2}
            pageRangeDisplayed={5}
            onPageChange={handlePageChange}
            containerClassName="pagination"
            pageClassName="page-item"
            pageLinkClassName="page-link"
            previousClassName="page-item"
            previousLinkClassName="page-link"
            nextClassName="page-item"
            nextLinkClassName="page-link"
            breakClassName="page-item"
            breakLinkClassName="page-link"
            activeClassName="active"
          />
        </nav>
      </div>
    </div>
  );
}

export default TabulatorFile;
