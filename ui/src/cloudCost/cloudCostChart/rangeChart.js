import * as React from "react";
import { makeStyles } from "@material-ui/styles";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import { primary, greyscale, browns } from "../../constants/colors";
import { toCurrency } from "../../util";

const RangeChart = ({ data, currency, height }) => {
  const useStyles = makeStyles({
    tooltip: {
      borderRadius: 2,
      background: "rgba(255, 255, 255, 0.95)",
      padding: 12,
    },
    tooltipLineItem: {
      fontSize: "1rem",
      margin: 0,
      marginBottom: 4,
      padding: 0,
    },
  });

  const accents = [...primary, ...greyscale, ...browns];

  const _IDLE_ = "__idle__";
  const _OTHER_ = "others";

  const getItemCost = (item) => {
    return item.value;
  };

  function toBar({ end, graph, start }) {
    const points = graph.map((item) => ({
      ...item,
      window: { end, start },
    }));

    const dateFormatter = Intl.DateTimeFormat(navigator.language, {
      year: "numeric",
      month: "numeric",
      day: "numeric",
      timeZone: "UTC",
    });

    const timeFormatter = Intl.DateTimeFormat(navigator.language, {
      hour: "numeric",
      minute: "numeric",
      timeZone: "UTC",
    });

    const s = new Date(start);
    const e = new Date(end);
    const interval = (e.valueOf() - s.valueOf()) / 1000 / 60 / 60;

    const bar = {
      end: new Date(end),
      key: interval >= 24 ? dateFormatter.format(s) : timeFormatter.format(s),
      items: {},
      start: new Date(start),
    };

    points.forEach((item) => {
      const windowStart = new Date(item.window.start);
      const windowEnd = new Date(item.window.end);
      const windowHours =
        (windowEnd.valueOf() - windowStart.valueOf()) / 1000 / 60 / 60;

      if (windowHours >= 24) {
        bar.key = dateFormatter.format(bar.start);
      } else {
        bar.key = timeFormatter.format(bar.start);
      }

      bar.items[item.name] = getItemCost(item);
    });

    return bar;
  }

  const getDataForCloudDay = (dayData) => {
    const { end, start } = dayData;
    const copy = [...dayData.items];

    // find items for idle and other
    const idleIndex = copy.findIndex((item) => item.name === _IDLE_);
    let idle = undefined;
    if (idleIndex > -1) {
      idle = copy[idleIndex];
      copy.splice(idleIndex, 1);
    }
    const otherIndex = copy.findIndex(
      (i) => i.name === _OTHER_ || i.name === "other"
    );
    let other = undefined;
    if (otherIndex > -1) {
      other = { ...copy[otherIndex], name: "other" };
      copy.splice(otherIndex, 1);
    }

    // sort and remove any items < top 8
    const sortedItems = copy.slice().sort((a, b) => {
      return a.value > b.value ? -1 : 1;
    });

    const top8 = sortedItems.slice(0, 8);
    // get items that didn't make the cut and shove into other
    const lefovers = sortedItems.slice(8);
    if (lefovers.length > 0) {
      const othersTotal = lefovers.reduce((a, b) => a.value + b.value);
      if (other) {
        other.value += othersTotal;
      } else if (othersTotal) {
        other = {
          name: "other",
          value: othersTotal,
        };
      }
    }
    // add in idle and other
    if (idle) {
      top8.unshift(idle);
    }
    if (other) {
      top8.unshift(other);
    }

    return { end, start, graph: top8 };
  };

  const getDataForGraph = (dataPoints) => {
    // for each day, we want top 8 + Idle and Other
    const orderedDataPoints = dataPoints.map(getDataForCloudDay);
    const bars = orderedDataPoints.map(toBar);

    const keyToFill = {};
    // we want to keep track of the order of fill assignment
    const assignmentOrder = [];
    let p = 0;

    orderedDataPoints.forEach(({ graph, start, end }) => {
      graph.forEach(({ name }) => {
        const key = name;
        if (keyToFill[key] === undefined) {
          assignmentOrder.push(key);
          if (key === _IDLE_) {
            keyToFill[key] = browns;
          } else if (key === _OTHER_ || key === "other") {
            keyToFill[key] = greyscale;
          } else {
            // non-idle/other allocations get the next available color
            keyToFill[key] = accents[p];
            p = (p + 1) % accents.length;
          }
        }
      });
    });
    // list of dataKeys and fillColors in order of importance (price w/ 'others' last)
    const labels = assignmentOrder.map((dataKey) => ({
      dataKey,
      fill: keyToFill[dataKey],
    }));

    return { bars, labels, keyToFill };
  };

  const { bars: barData, labels: barLabels, keyToFill } = getDataForGraph(data);

  const classes = useStyles();

  const CustomTooltip = (params) => {
    const { active, payload } = params;

    if (!payload || payload.length == 0) {
      return null;
    }

    const total = payload.reduce((sum, item) => sum + item.value, 0.0);
    if (active) {
      return (
        <div className={classes.tooltip}>
          <p
            className={classes.tooltipLineItem}
            style={{ color: "#000000" }}
          >{`Total: ${toCurrency(total, currency)}`}</p>

          {payload
            .slice()
            .map((item, i) => (
              <div
                key={item.name}
                style={{
                  display: "grid",
                  gridTemplateColumns: "20px 1fr",
                  gap: ".5em",
                  margin: ".25em",
                }}
              >
                <div>
                  <div
                    style={{
                      backgroundColor: keyToFill[item.payload.items[i][0]],
                      width: 18,
                      height: 18,
                    }}
                  />
                </div>
                <div>
                  <p className={classes.tooltipLineItem}>{`${
                    item.payload.items[i][0]
                  }: ${toCurrency(item.value, currency)}`}</p>
                </div>
              </div>
            ))
            .reverse()}
        </div>
      );
    }

    return null;
  };

  const orderedBars = barData.map((bar) => {
    return {
      ...bar,
      items: Object.entries(bar.items).sort((a, b) => {
        if (a[0] === "other") {
          return -1;
        }
        if (b[0] === "other") {
          return 1;
        }
        return a[1] > b[1] ? -1 : 1;
      }),
    };
  });

  return (
    <ResponsiveContainer height={height} width={"100%"}>
      <BarChart
        data={orderedBars}
        margin={{ top: 30, right: 35, left: 30, bottom: 45 }}
      >
        <CartesianGrid strokeDasharray={"3 3"} vertical={false} />
        <XAxis dataKey={"key"} />
        <YAxis tickFormatter={(val) => toCurrency(val, currency, 2, true)} />
        <Tooltip content={<CustomTooltip />} wrapperStyle={{ zIndex: 1000 }} />

        {new Array(10).fill(0).map((item, idx) => (
          <Bar
            dataKey={(entry) => (entry.items[idx] ? entry.items[idx][1] : null)}
            stackId="x"
          >
            {orderedBars.map((bar) =>
              bar.items[idx] ? (
                <Cell fill={keyToFill[bar.items[idx][0]]} />
              ) : (
                <Cell />
              )
            )}
          </Bar>
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
};

export default RangeChart;
