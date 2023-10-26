import React from "react";
import { BrowserRouter as Router, Route, Switch } from "react-router-dom";

import Reports from "./Reports.js";
import CloudCostReports from "./CloudCostReports.js";
import Sidebar from "./components/Sidebar.js";

const Routes = () => {
  return (
    <Router>
      <Switch>
        <Route exact path="/">
          <Reports />
        </Route>
        <Route exact path="/cloud">
          <CloudCostReports />
        </Route>
      </Switch>
    </Router>
  );
};

export default Routes;
