import * as React from "react";
import { Drawer, List } from "@material-ui/core";

import { NavItem } from "./NavItem";
import { BarChart } from "@material-ui/icons";
import { Cloud } from "@material-ui/icons";
import { makeStyles } from "@material-ui/styles";

const DRAWER_WIDTH = 200;

const SidebarNav = ({ active }) => {
  const useStyles = makeStyles({
    drawer: {
      width: DRAWER_WIDTH,
      flexShrink: 0,
    },
    drawerPaper: {
      backgroundColor: "inherit",
      border: 0,
      width: DRAWER_WIDTH,
      paddingTop: "2.5rem",
    },
    text: {
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    },
  });

  const classes = useStyles();

  const [init, setInit] = React.useState(false);

  React.useEffect(() => {
    if (!init) {
      setInit(true);
    }
  }, [init]);

  const top = [
    {
      name: "Cost Allocation",
      href: "allocation",
      icon: <BarChart />,
    },
    { name: "Cloud Costs", href: "cloud", icon: <Cloud /> },
  ];

  return (
    <Drawer
      anchor={"left"}
      className={classes.drawer}
      classes={{ paper: classes.drawerPaper }}
      variant={"permanent"}
    >
      <img
        src={require("../../images/logo.png")}
        alt="OpenCost"
        style={{ flexShrink: 1, padding: "1rem" }}
      />
      <List style={{ flexGrow: 1 }}>
        {top.map((l) => (
          <NavItem active={active === `/${l.href}`} key={l.name} {...l} />
        ))}
      </List>
    </Drawer>
  );
};

export { SidebarNav };
