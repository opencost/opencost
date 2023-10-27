import { makeStyles } from "@material-ui/styles";
import * as React from "react";
import { useLocation } from "react-router-dom";
import { SidebarNav } from "./Nav/SidebarNav";

const useStyles = makeStyles({
  wrapper: {
    position: "relative",
    height: "100vh",
    flexGrow: 1,
    overflowX: "auto",
    paddingLeft: "2rem",
    paddingRight: "rem",
    paddingTop: "2.5rem",
  },
  flexGrow: {
    display: "flex",
    flexFlow: "column",
    flexGrow: 1,
  },
  body: {
    display: "flex",
    overflowY: "scroll",
    margin: "0px",
    backgroundColor: "f3f3f3",
  },
});

const Page = (props) => {
  const classes = useStyles();

  const { pathname } = useLocation();

  return (
    <div className={classes.body}>
      <SidebarNav active={pathname} />
      <div className={classes.flexGrow}>
        <div className={classes.wrapper}>
          <div className={classes.flexGrow}>{props.children}</div>
        </div>
      </div>
    </div>
  );
};

export default Page;
