import React from "react";
import { isArray, filter, map, reduce, reverse, sortBy } from "lodash";

import Typography from "@material-ui/core/Typography";

import RangeChart from "./RangeChart";

const CloudCostChart = ({ graphData, currency, n, height }) => {
  if (graphData.length === 0) {
    return <Typography variant="body2">No data</Typography>;
  }
  return <RangeChart data={graphData} currency={currency} height={height} />;
};

export default React.memo(CloudCostChart);
