import React from "react";
import Page from "./components/Page";
import Header from "./components/Header";
import IconButton from "@material-ui/core/IconButton";
import RefreshIcon from "@material-ui/icons/Refresh";
import { makeStyles } from "@material-ui/styles";
import { Paper } from "@material-ui/core";

const useStyles = makeStyles({
  reportHeader: {
    display: "flex",
    flexFlow: "row",
    padding: 24,
  },
  titles: {
    flexGrow: 1,
  },
});

const CloudCostReports = () => {
  const classes = useStyles();

  return (
    <Page active="cloud.html">
      <Header>
        <IconButton aria-label="refresh" onClick={() => setFetch(true)}>
          <RefreshIcon />
        </IconButton>
      </Header>

      <Paper id="cloud-cost">
        <div>Cloud Cost </div>
      </Paper>
    </Page>
  );
};

export default React.memo(CloudCostReports);
