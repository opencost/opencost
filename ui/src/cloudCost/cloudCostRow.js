import * as React from "react";

import { TableCell, TableRow } from "@material-ui/core";

import { toCurrency } from "../util";
import { primary } from "../constants/colors";

const displayCurrencyAsLessThanPenny = (amount, currency) =>
  amount > 0 && amount < 0.01
    ? `<${toCurrency(0.01, currency)}`
    : toCurrency(amount, currency);

const CloudCostRow = ({
  cost,
  costSuffix,
  currency,
  drilldown,
  kubernetesPercent,
  name,
  row,
  sampleData,
}) => {
  function calculatePercent() {
    const totalPercent = (kubernetesPercent * 100).toFixed();
    return `${totalPercent}%`;
  }

  const whichPercent = sampleData
    ? `${(kubernetesPercent * 100).toFixed(1)}%`
    : calculatePercent();
  return (
    <TableRow onClick={() => drilldown(row)}>
      <TableCell
        align={"left"}
        style={{ cursor: "pointer", color: "#346ef2", padding: "1rem" }}
      >
        {name}
      </TableCell>
      <TableCell align={"right"}>{whichPercent}</TableCell>
      {/* total cost */}
      <TableCell align={"right"} style={{ paddingRight: "2em" }}>
        {`${displayCurrencyAsLessThanPenny(cost, currency)}${costSuffix}`}
      </TableCell>
    </TableRow>
  );
};

export { CloudCostRow };
