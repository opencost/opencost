import React from "react";
import { get, round } from "lodash";
import { makeStyles } from "@material-ui/styles";
import {
  Typography,
  TableContainer,
  TableCell,
  TableHead,
  TablePagination,
  TableRow,
  TableSortLabel,
  Table,
  TableBody,
} from "@material-ui/core";

import { toCurrency } from "../util";
import CloudCostChart from "./CloudCostChart";

const CloudCost = ({ cumulativeData, totalData, graphData, currency }) => {
  const useStyles = makeStyles({
    noResults: {
      padding: 24,
    },
  });

  const classes = useStyles();

  function descendingComparator(a, b, orderBy) {
    if (get(b, orderBy) < get(a, orderBy)) {
      return -1;
    }
    if (get(b, orderBy) > get(a, orderBy)) {
      return 1;
    }
    return 0;
  }

  function getComparator(order, orderBy) {
    return order === "desc"
      ? (a, b) => descendingComparator(a, b, orderBy)
      : (a, b) => -descendingComparator(a, b, orderBy);
  }

  function stableSort(array, comparator) {
    const stabilizedThis = array.map((el, index) => [el, index]);
    stabilizedThis.sort((a, b) => {
      const order = comparator(a[0], b[0]);
      if (order !== 0) return order;
      return a[1] - b[1];
    });
    return stabilizedThis.map((el) => el[0]);
  }

  const headCells = [
    { id: "name", numeric: false, label: "Name", width: "auto" },
    {
      id: "kubernetesPercent",
      numeric: true,
      label: "K8's Utilization",
      width: 90,
    },
    {
      id: "cost",
      numeric: true,
      label: "Sum of Sample Data",
      width: 90,
    },
  ];

  const [order, setOrder] = React.useState("desc");
  const [orderBy, setOrderBy] = React.useState("totalCost");
  const [page, setPage] = React.useState(0);
  const [rowsPerPage, setRowsPerPage] = React.useState(25);
  const numData = cumulativeData.length;

  const lastPage = Math.floor(numData / rowsPerPage);

  const handleChangePage = (event, newPage) => setPage(newPage);

  const handleChangeRowsPerPage = (event) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const createSortHandler = (property) => (event) =>
    handleRequestSort(event, property);

  const handleRequestSort = (event, property) => {
    const isDesc = orderBy === property && order === "desc";
    setOrder(isDesc ? "asc" : "desc");
    setOrderBy(property);
  };

  const orderedRows = stableSort(cumulativeData, getComparator(order, orderBy));
  const pageRows = orderedRows.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage
  );

  React.useEffect(() => {
    setPage(0);
  }, [numData]);

  if (cumulativeData.length === 0) {
    return (
      <Typography variant="body2" className={classes.noResults}>
        No results
      </Typography>
    );
  }

  return (
    <div id="cloud-cost">
      <div id="cloud-graph-">
        <CloudCostChart
          currency={currency}
          graphData={graphData}
          height={300}
          n={10}
        />
      </div>
      <div id="cloud-cost-table">
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                {headCells.map((cell) => (
                  <TableCell
                    key={cell.id}
                    colSpan={cell.colspan}
                    align={cell.numeric ? "right" : "left"}
                    sortDirection={orderBy === cell.id ? order : false}
                    style={{ width: cell.width }}
                  >
                    <TableSortLabel
                      active={orderBy === cell.id}
                      direction={orderBy === cell.id ? order : "asc"}
                      onClick={createSortHandler(cell.id)}
                    >
                      {cell.label}
                    </TableSortLabel>
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              <TableRow>
                {headCells.map((cell) => {
                  return (
                    <TableCell
                      key={cell.id}
                      colSpan={cell.colspan}
                      align={cell.numeric ? "right" : "left"}
                      style={{ fontWeight: 500 }}
                    >
                      {cell.id === "kubernetesPercent"
                        ? round(totalData[cell.id] * 100, 2)
                        : toCurrency(round(totalData[cell.id]), currency)}
                    </TableCell>
                  );
                })}
              </TableRow>
              {pageRows.map((row, key) => {
                return (
                  <TableRow key={key}>
                    <TableCell align="left">{row.name}</TableCell>
                    <TableCell align="right">
                      {round(row.kubernetesPercent * 100)}
                    </TableCell>
                    <TableCell align="right">
                      {toCurrency(row.cost, currency)}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          component="div"
          count={numData}
          rowsPerPage={rowsPerPage}
          rowsPerPageOptions={[10, 25, 50]}
          page={Math.min(page, lastPage)}
          onChangePage={handleChangePage}
          onChangeRowsPerPage={handleChangeRowsPerPage}
        />
      </div>
    </div>
  );
};

export default React.memo(CloudCost);
