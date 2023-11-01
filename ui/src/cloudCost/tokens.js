const windowOptions = [
  { name: "Today", value: "today" },
  { name: "Yesterday", value: "yesterday" },
  { name: "Last 24h", value: "24h" },
  { name: "Last 48h", value: "48h" },
  { name: "Week-to-date", value: "week" },
  { name: "Last week", value: "lastweek" },
  { name: "Last 7 days", value: "7d" },
  { name: "Last 14 days", value: "14d" },
];

const aggregationOptions = [
  { name: "Account", value: "accountID" },
  { name: "Invoice Entity", value: "invoiceEntityID" },
  { name: "Provider", value: "provider" },
  { name: "Service ", value: "service" },
  { name: "Category", value: "category" },
  { name: "Item", value: "item" },
];

const costMetricOptions = [
  { name: "Amortized Net Cost", value: "AmortizedNetCost" },
  { name: "List Cost", value: "ListCost" },
  { name: "Invoiced Cost", value: "InvoicedCost" },
  { name: "Amortized Cost", value: "AmortizedCost" },
];

const aggMap = {
  invoiceEntityID: "Invoice Entity",
  provider: "Provider",
  service: "Service",
  accountID: "Account",
};

const costMetricToPropName = {
  AmortizedNetCost: "amortizedNetCost",
  AmortizedCost: "amortizedCost",
  ListCost: "listCost",
  NetCost: "netCost",
  InvoicedCost: "invoicedCost",
};

export {
  windowOptions,
  aggregationOptions,
  costMetricOptions,
  aggMap,
  costMetricToPropName,
};
