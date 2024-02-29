import axios from "axios";
import { parseFilters } from "../util";
import { costMetricToPropName } from "../cloudCost/tokens";

function formatItemsForCost({ data, costType }) {
  return data.sets.map(({ cloudCosts, window }) => {
    return {
      date: window.start,
      cost: Object.values(cloudCosts).reduce(
        (acc, costs) => acc + costs[costType || "amortizedNetCost"].cost,
        0
      ),
    };
  });
}

class CloudCostDayTotalsService {
  BASE_URL = process.env.BASE_URL || "{PLACEHOLDER_BASE_URL}";

  async fetchCloudCostData(window, aggregate, costMetric, filters) {
    if (this.BASE_URL.includes("PLACEHOLDER_BASE_URL")) {
      this.BASE_URL = `http://localhost:9090/model`;
    }
    if (aggregate.includes("item")) {
      const resp = await axios.get(
        `${
          this.BASE_URL
        }/cloudCost?window=${window}&costMetric=${costMetric}&filter=${parseFilters(
          filters
        )}`
      );
      const costMetricProp = costMetricToPropName[costMetric];

      const result_2 = await resp.data;
      return { data: formatItemsForCost(result_2, costMetricProp) };
    }

    return [];
  }
}

export default new CloudCostDayTotalsService();
