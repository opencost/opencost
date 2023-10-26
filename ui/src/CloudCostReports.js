import React from "react";
import Page from "./components/Page";
import Header from "./components/Header";
import IconButton from "@material-ui/core/IconButton";
import RefreshIcon from "@material-ui/icons/Refresh";
import { makeStyles } from "@material-ui/styles";
import { Paper, Typography } from "@material-ui/core";
import CircularProgress from "@material-ui/core/CircularProgress";
import { get, find, sortBy, toArray } from "lodash";

import { useLocation, useHistory } from "react-router";

import CloudCostEditControls from "./CloudCost/Controls/CloudCostEditControls";
import Subtitle from "./components/Subtitle";
import Warnings from "./components/Warnings";

import {
  windowOptions,
  costMetricOptions,
  aggregationOptions,
} from "./CloudCost/tokens";
import { currencyCodes } from "./constants/currencyCodes";

const CloudCostReports = () => {
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
  const classes = useStyles();

  // Form state, which controls form elements, but not the report itself. On
  // certain actions, the form state may flow into the report state.
  const [title, setTitle] = React.useState(
    "Cumulative cost for last 7 days by account"
  );
  const [window, setWindow] = React.useState(windowOptions[0].value);
  const [aggregateBy, setAggregateBy] = React.useState(
    aggregationOptions[0].value
  );
  const [costMetric, setCostMetric] = React.useState(
    costMetricOptions[0].value
  );
  const [currency, setCurrency] = React.useState("USD");
  // page and settings state
  const [init, setInit] = React.useState(false);
  const [fetch, setFetch] = React.useState(false);
  const [loading, setLoading] = React.useState(true);
  const [errors, setErrors] = React.useState([]);

  function generateTitle({ window, aggregateBy, costMetric }) {
    let windowName = get(find(windowOptions, { value: window }), "name", "");
    if (windowName === "") {
      if (checkCustomWindow(window)) {
        windowName = toVerboseTimeRange(window);
      } else {
        console.warn(`unknown window: ${window}`);
      }
    }

    let aggregationName = get(
      find(aggregationOptions, { value: aggregateBy }),
      "name",
      ""
    ).toLowerCase();
    if (aggregationName === "") {
      console.warn(`unknown aggregation: ${aggregateBy}`);
    }

    let str = `${windowName} by ${aggregationName}`;

    if (!costMetric) {
      str = `${str} amoritizedNetCost`;
    }

    return str;
  }

  // parse any context information from the URL
  const routerLocation = useLocation();
  const searchParams = new URLSearchParams(routerLocation.search);
  const routerHistory = useHistory();

  async function initialize() {
    setInit(true);
  }

  async function fetchData() {
    setLoading(true);
    setErrors([]);
    try {
      console.log("look at me fetching data");
    } catch (error) {
      console.error(error);
    }
    setLoading(false);
  }

  React.useEffect(() => {
    setWindow(searchParams.get("window") || "7d");
    setAggregateBy(searchParams.get("agg") || "service");
    setCostMetric(searchParams.get("costMetric") || "AmortizedNetCost");
    setCurrency(searchParams.get("currency") || "USD");
  }, [routerLocation]);

  // Initialize once, then fetch report each time setFetch(true) is called
  React.useEffect(() => {
    if (!init) {
      initialize();
    }
    if (init && fetch) {
      fetchData();
    }
  }, [init, fetch]);

  React.useEffect(() => {
    setFetch(true);
    setTitle(generateTitle({ window, aggregateBy, costMetric }));
  }, [window, aggregateBy, costMetric]);

  return (
    <Page active="cloud.html">
      <Header>
        <IconButton aria-label="refresh" onClick={() => setFetch(true)}>
          <RefreshIcon />
        </IconButton>
      </Header>

      {!loading && errors.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <Warnings warnings={errors} />
        </div>
      )}

      {init && (
        <Paper id="cloud-cost">
          <div className={classes.reportHeader}>
            <div className={classes.titles}>
              <Typography variant="h5">{title}</Typography>
              <Subtitle report={{ window, aggregateBy }} />
            </div>
            <CloudCostEditControls
              windowOptions={windowOptions}
              window={window}
              setWindow={(win) => {
                searchParams.set("window", win);
                routerHistory.push({
                  search: `?${searchParams.toString()}`,
                });
              }}
              aggregationOptions={aggregationOptions}
              aggregateBy={aggregateBy}
              setAggregateBy={(agg) => {
                searchParams.set("agg", agg);
                routerHistory.push({
                  search: `?${searchParams.toString()}`,
                });
              }}
              costMetricOptions={costMetricOptions}
              costMetric={costMetric}
              setCostMetric={(c) => {
                searchParams.set("costMetric", c);
                routerHistory.push({
                  search: `?${searchParams.toString()}`,
                });
              }}
              title={title}
              // cumulativeData={cumulativeData}
              currency={currency}
              currencyOptions={currencyCodes}
              setCurrency={(curr) => {
                searchParams.set("currency", curr);
                routerHistory.push({
                  search: `?${searchParams.toString()}`,
                });
              }}
            />
          </div>

          {loading && (
            <div style={{ display: "flex", justifyContent: "center" }}>
              <div style={{ paddingTop: 100, paddingBottom: 100 }}>
                <CircularProgress />
              </div>
            </div>
          )}

          {!loading && <div>Cloud Cost Report goes here</div>}
        </Paper>
      )}
    </Page>
  );
};

export default React.memo(CloudCostReports);
