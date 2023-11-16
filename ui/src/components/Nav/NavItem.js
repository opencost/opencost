import * as React from "react";
import { ListItem, ListItemIcon, ListItemText } from "@material-ui/core";
import { Link } from "react-router-dom";
import { makeStyles } from "@material-ui/styles";

const NavItem = ({ active, href, name, onClick, secondary, title, icon }) => {
  const useStyles = makeStyles({
    root: {
      cursor: "pointer",
      "&:hover": {
        backgroundColor: "#ebebeb",
      },
      "&:selected": {
        backgroundColor: "#e1e1e1",
      },
    },
    text: {
      maxWidth: 200,
      overflow: "hidden",
      textOverflow: "ellipsis",
      whiteSpace: "nowrap",
    },
    activeIcon: {
      color: "#346ef2",
      minWidth: 36,
    },
    activeText: {
      color: "#346ef2",
    },
    icon: {
      color: "#4e4e4e",
      minWidth: 36,
    },
  });
  const classes = useStyles();

  const listItemIconClasses = { root: classes.icon };
  const listItemTextClasses = {
    secondary: classes.text,
  };

  if (active) {
    listItemIconClasses.root = classes.activeIcon;
    listItemTextClasses.primary = classes.activeText;
  }

  const renderListItemCore = () => (
    <ListItem
      className={active ? "active" : ""}
      classes={{ root: classes.root }}
      onClick={(e) => {
        if (onClick) {
          onClick();
          e.stopPropagation();
        }
      }}
      selected={active}
      title={title}
    >
      <ListItemIcon classes={listItemIconClasses}>{icon}</ListItemIcon>
      <ListItemText
        classes={listItemTextClasses}
        primary={name}
        secondary={secondary}
      />
    </ListItem>
  );

  return href && !active ? (
    <Link style={{ textDecoration: "none", color: "inherit" }} to={`${href}`}>
      {renderListItemCore()}
    </Link>
  ) : (
    renderListItemCore()
  );
};

export { NavItem };
