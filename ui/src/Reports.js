import CircularProgress from '@material-ui/core/CircularProgress'
import IconButton from '@material-ui/core/IconButton'
import Link from '@material-ui/core/Link'
import Paper from '@material-ui/core/Paper'
import Typography from '@material-ui/core/Typography'
import RefreshIcon from '@material-ui/icons/Refresh'
import SettingsIcon from '@material-ui/icons/Settings'
import { makeStyles } from '@material-ui/styles'
import { filter, find, forEach, get, isArray, sortBy, toArray, trim } from 'lodash'
import React, { useEffect, useState } from 'react'
import ReactDOM from 'react-dom'
import { useLocation, useHistory } from 'react-router';

import AllocationReport from './components/AllocationReport';
import Controls from './components/Controls';
import DetailsDialog from './components/DetailsDialog';
import Header from './components/Header';
import Page from './components/Page';
import Subtitle from './components/Subtitle';
import Warnings from './components/Warnings';
import AllocationService from './services/allocation';
import { cumulativeToTotals, rangeToCumulative } from './util';

const windowOptions = [
  { name: 'Today', value: 'today' },
  { name: 'Yesterday', value: 'yesterday' },
  { name: 'Week-to-date', value: 'week' },
  { name: 'Month-to-date', value: 'month' },
  { name: 'Last week', value: 'lastweek' },
  { name: 'Last month', value: 'lastmonth' },
  { name: 'Last 7 days', value: '6d' },
  { name: 'Last 30 days', value: '29d' },
  { name: 'Last 60 days', value: '59d' },
  { name: 'Last 90 days', value: '89d' },
]

const aggregationOptions = [
  { name: 'Cluster', value: 'cluster' },
  { name: 'Node', value: 'node' },
  { name: 'Namespace', value: 'namespace' },
  { name: 'Controller kind', value: 'controllerKind' },
  { name: 'Controller', value: 'controller' },
  { name: 'Service', value: 'service' },
  { name: 'Pod', value: 'pod' },
]

const idleOptions = [
  { name: 'Hide', value: "hide" },
  { name: 'Share', value: "share" },
  { name: 'Separate', value: "separate" },
]

const accumulateOptions = [
  { name: 'Entire window', value: true },
  { name: 'Daily', value: false },
]

const shareSplitOptions = [
  { name: 'Share evenly', value: 'even' },
  { name: 'Share weighted by cost', value: 'weighted' },
]

const useStyles = makeStyles({
  reportHeader: {
    display: 'flex',
    flexFlow: 'row',
    padding: 24,
  },
  titles: {
    flexGrow: 1,
  },
})

const ReportsPage = () => {
  const classes = useStyles()

  // Allocation data state
  const [allocationData, setAllocationData] = useState([])
  const [cumulativeData, setCumulativeData] = useState({})
  const [totalData, setTotalData] = useState({})

  // When allocation data changes, create a cumulative version of it
  useEffect(() => {
    const cumulative = rangeToCumulative(allocationData, aggregateBy)
    setCumulativeData(toArray(cumulative))
    setTotalData(cumulativeToTotals(cumulative))
  }, [allocationData])

  // Form state, which controls form elements, but not the report itself. On
  // certain actions, the form state may flow into the report state.
  const [window, setWindow] = useState(windowOptions[0].value)
  const [aggregateBy, setAggregateBy] = useState(aggregationOptions[0].value)
  const [accumulate, setAccumulate] = useState(accumulateOptions[0].value)
  const [idle, setIdle] = useState(idleOptions[0].value)
  const [filters, setFilters] = useState([])
  const [shareCost, setShareCost] = useState(0.0)
  const [shareNamespaces, setShareNamespaces] = useState([])
  const [shareLabels, setShareLabels] = useState(0.0)
  const [shareSplit, setShareSplit] = useState('weighted')

  // Context is used for drill-down; each drill-down gets pushed onto the
  // context stack. Clearing resets to an empty stack. Using a breadcrumb
  // should pop everything above that on the stack.
  const [context, setContext] = useState([])

  const clearContext = () => {
    if (context.length > 0) {
      searchParams.set('agg', context[0].property.toLowerCase());
    }
    searchParams.set('context', btoa(JSON.stringify([])));
  }

  const goToContext = (i) => {
    if (!isArray(context)) {
      console.warn(`context is not an array: ${context}`)
      return
    }

    if (i > context.length-1) {
      console.warn(`selected context out of range: ${i} with context length ${context.length}`)
      return
    }

    if (i === context.length-1) {
      console.warn(`selected current context: ${i} with context length ${context.length}`)
    }

    searchParams.set('agg', context[i+1].property.toLowerCase());
    searchParams.set('context', btoa(JSON.stringify(context.slice(0, i+1))));
    routerHistory.push({ search: `?${searchParams.toString()}` });
  }

  const drillDown = (row) => {
    if (aggregateBy === "pod") {

      let pod = row.name
      let cluster = ""
      let namespace = ""
      let controllerKind = ""
      let controller = ""

      forEach(context, ctx => {
        if (ctx.property.toLowerCase() == "cluster") {
          cluster = ctx.value
        } else if (ctx.property.toLowerCase() == "namespace") {
          namespace = ctx.value
        } else if (ctx.property.toLowerCase() == "controller") {
          const tokens = ctx.value.split("/")
          if (tokens.length == 2) {
            controllerKind = tokens[0]
            controller = tokens[1]
          } else {
            controller = ctx.value
          }
        }
      })

      openDetails(cluster, namespace, controllerKind, controller, pod)
    }

    if (aggregateBy === "controller") {
      const ctx = [ ...context, {
        property: "Controller",
        value: row.name,
        name: row.name,
      }];

      searchParams.set('agg', 'pod');
      searchParams.set('context', btoa(JSON.stringify(ctx)));
      routerHistory.push({
        search: `?${searchParams.toString()}`,
      });
    }

    if (aggregateBy === "cluster") {
      let cluster = get(row, "cluster", "")
      let clusterTokens = get(row, "cluster", "").split("/")
      if (clusterTokens.length > 0) {
        cluster = clusterTokens[0]
      }

      const ctx = [ ...context, {
        property: "Cluster",
        value: cluster,
        name: cluster,
      }];

      searchParams.set('agg', 'namespace');
      searchParams.set('context', btoa(JSON.stringify(ctx)));
      routerHistory.push({
        search: `?${searchParams.toString()}`,
      });
    }

    if (aggregateBy === "namespace") {
      const ctx = [ ...context, {
        property: 'Namespace',
        value: row.namespace,
        name: row.namespace,
      }];

      searchParams.set('agg', 'controller');
      searchParams.set('context', btoa(JSON.stringify(ctx)));
      routerHistory.push({
        search: `?${searchParams.toString()}`,
      });
    }

    if (aggregateBy === "controllerKind") {
      const ctx = [ ...context, {
        property: "Controller Kind",
        value: row.controllerKind,
        name: row.controllerKind,
      }];

      searchParams.set('agg', 'controller');
      searchParams.set('context', btoa(JSON.stringify(ctx)));
      routerHistory.push({
        search: `?${searchParams.toString()}`,
      });
    }

    if (aggregateBy === "node") {
      const ctx = [ ...context, {
        property: "Node",
        value: row.node,
        name: row.node,
      }];
      searchParams.set('agg', 'controller');
      searchParams.set('context', btoa(JSON.stringify(ctx)));
      routerHistory.push({
        search: `?${searchParams.toString()}`,
      });
    }

    // TODO labels?
  }

  // Setting details to null closes the details dialog. Setting it to an
  // object describing a controller opens it with that state.
  const [details, setDetails] = useState(null)

  const closeDetails = () => {
    searchParams.set('details', btoa(JSON.stringify(null)));
    routerHistory.push({ search: `?${searchParams.toString()}` });
  }

  const openDetails = (cluster, namespace, controllerKind, controller, pod) => {
    searchParams.set('details', btoa(JSON.stringify({ cluster, namespace, controllerKind, controller, pod })));
    routerHistory.push({ search: `?${searchParams.toString()}` });
  }

  // Report state, including current report and saved options
  const [title, setTitle] = useState("")
  const [titleField, setTitleField] = useState("")

  // When parameters changes, clear context and fetch data. This should be the
  // only mechanism used to fetch data. Also update title to saved title, if
  // possible, or generate a sensible title from the paramters
  useEffect(() => {
    setFetch(true)

    // Use "aggregateBy" by default, but if we're within a context, then
    // only use the top-level context; e.g. if we started by namespace, but
    // drilled down, we'll have (aggregateBy == "controller"), but the
    // report should keep the title of "by namespace".
    let aggBy = aggregateBy
    if (context.length > 0) {
      aggBy = context[0].property.toLowerCase()
    }

    const curr = { window, aggregateBy: aggBy, accumulate, idle, filters }

  }, [window, aggregateBy, accumulate, idle, filters, shareSplit])

  // page and settings state
  const [init, setInit] = useState(false)
  const [fetch, setFetch] = useState(false)
  const [loading, setLoading] = useState(true)
  const [errors, setErrors] = useState([])
  const [currency, setCurrency] = useState('USD')

  // Initialize once, then fetch report each time setFetch(true) is called
  useEffect(() => {
    if (!init) {
      initialize()
    }
    if (init && fetch) {
      fetchData()
    }
  }, [init, fetch])

  // parse any context information from the URL
  const routerLocation = useLocation();
  const searchParams = new URLSearchParams(routerLocation.search);
  const routerHistory = useHistory();
  useEffect(() => {
    let ctx = searchParams.get('context');
    let deets = searchParams.get('details');
    let fltr = searchParams.get('filters');

    try {
      ctx = JSON.parse(atob(ctx)) || [];
    } catch (err) {
      ctx = [];
    }

    try {
      deets = JSON.parse(atob(deets)) || null;
    } catch (err) {
      deets = null;
    }

    try {
      fltr = JSON.parse(atob(fltr)) || [];
    } catch (err) {
      fltr = [];
    }
    setWindow(searchParams.get('window') || '6d');
    setAggregateBy(searchParams.get('agg') || 'namespace');
    setAccumulate((searchParams.get('acc') === 'true') || false);
    setIdle(searchParams.get('idle') || 'separate');
    setTitle(searchParams.get('title') || 'Last 7 days by namespace daily');
    setShareSplit(searchParams.get('split') || 'weighted');
    setContext(ctx);
    setDetails(deets);
    setFilters(fltr);
  }, [routerLocation]);

  async function initialize() {
    setInit(true)
  }

  async function fetchData() {
    setLoading(true)
    setErrors([])

    try {
      let queryFilters = []
      if (context.length > 0) {
        forEach(context, (contextFilter) => {
          queryFilters.push(contextFilter)
        })
      }
      forEach(filters, (filter) => {
        queryFilters.push(filter)
      })

      const resp = await AllocationService.fetchAllocation(window, aggregateBy, {
        accumulate: accumulate,
        filters: queryFilters,
      })
      if (resp.data && resp.data.length > 0) {
        const allocationRange = resp.data
        for (const i in allocationRange) {
          // update cluster aggregations to use clusterName/clusterId names
          if (aggregateBy == "cluster") {
            for (const k in allocationRange[i]) {
              allocationRange[i][k].name = 'cluster-one';
            }
          }
          allocationRange[i] = sortBy(allocationRange[i], a => a.totalCost)
        }
        setAllocationData(allocationRange)
      } else {
        if (resp.message && resp.message.indexOf('boundary error') >= 0) {
          let match = resp.message.match(/(ETL is \d+\.\d+% complete)/)
          let secondary = 'Try again after ETL build is complete'
          if (match.length > 0) {
            secondary = `${match[1]}. ${secondary}`
          }
          setErrors([{
            primary: "Data unavailable while ETL is building",
            secondary: secondary,
          }])
        }
        setAllocationData([])
      }
    } catch (err) {
      if (err.message.indexOf("404") === 0) {
        setErrors([{
          primary: "Failed to load report data",
          secondary: "Please update Kubecost to the latest version, then contact support if problems persist."
        }])
      } else {
        let secondary = "Please contact Kubecost support with a bug report if problems persist."
        if (err.message.length > 0) {
          secondary = err.message
        }
        setErrors([{
          primary: "Failed to load report data",
          secondary: secondary,
        }])
      }
      setAllocationData([])
    }

    setLoading(false)
    setFetch(false)
  }
  return (
    <Page active="reports.html">
      <Header breadcrumbs={[
        { 'name': 'Reports', 'href': 'new_index.html#/reports' },
        { 'name': title, 'href': `new_index.html#/reports` },
      ]}>
        <IconButton aria-label="refresh" onClick={() => setFetch(true)}>
          <RefreshIcon />
        </IconButton>
      </Header>

      {!loading && errors.length > 0 && (
        <div style={{ marginBottom: 20 }}>
          <Warnings warnings={errors} />
        </div>
      )}

      {init && <Paper id="report">
        <div className={classes.reportHeader}>
          <div className={classes.titles}>
            <Typography variant="h5">{title}</Typography>
            <Subtitle
              report={{ window, aggregateBy, accumulate, idle, filters }}
              context={context}
              clearContext={() => { clearContext(); routerHistory.push({ search: `?${searchParams.toString()}`}); }}
              goToContext={goToContext}
            />
          </div>

          <Controls
            windowOptions={windowOptions}
            window={window}
            setWindow={(win) => {
              searchParams.set('window', win);
              routerHistory.push({
                search: `?${searchParams.toString()}`,
              });
            }}
            aggregationOptions={aggregationOptions}
            aggregateBy={aggregateBy}
            setAggregateBy={(agg) => {
              searchParams.set('agg', agg);
              routerHistory.push({
                search: `?${searchParams.toString()}`,
              });
            }}
            accumulateOptions={accumulateOptions}
            accumulate={accumulate}
            setAccumulate={(acc) => {
              searchParams.set('acc', acc);
              routerHistory.push({
                search: `?${searchParams.toString()}`
              });
            }}
            idleOptions={idleOptions}
            idle={idle}
            setIdle={(idle) => {
              searchParams.set('idle', idle);
              routerHistory.push({
                search: `?${searchParams.toString()}`,
              });
            }}
            title={title}
            cumulativeData={cumulativeData}
            currency={currency}
            titleField={titleField}
            filters={filters}
            setFilters={(filters) => {
              const fltr = btoa(JSON.stringify(filters));
              searchParams.set('filters', fltr);
              routerHistory.push({
                search: `?${searchParams.toString()}`,
              });
            }}
            clearContext={clearContext}
            context={context}
          />
        </div>

        {loading && (
          <div style={{ display: 'flex', justifyContent: 'center' }}>
            <div style={{ paddingTop: 100, paddingBottom: 100 }}>
              <CircularProgress />
            </div>
          </div>
        )}
        {!loading && (
          <AllocationReport
            allocationData={allocationData}
            cumulativeData={cumulativeData}
            totalData={totalData}
            currency={currency}
            drillDown={drillDown}
          />
        )}
      </Paper>}

      <DetailsDialog
        open={details != null}
        close={() => closeDetails()}
        details={details}
        window={window}
        currency={currency}
      />
    </Page>
  )
}

export default React.memo(ReportsPage);
