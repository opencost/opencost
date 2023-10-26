import axios from "axios";
import { getCloudFilters } from "../util";

export function formatItemsForCost({ data }, costType) {
  return data.sets.map(({ cloudCosts, window }) => {
    return {
      date: window.start,
      cost: Object.values(cloudCosts).reduce(
        (acc, costs) => acc + costs[costType].cost,
        0
      ),
    };
  });
}

class CloudCostDayTotalsService {
  BASE_URL = process.env.BASE_URL || "{PLACEHOLDER_BASE_URL}";

  async fetchCloudCostData(window, aggregate, costMetric) {
    if (this.BASE_URL.includes("PLACEHOLDER_BASE_URL")) {
      this.BASE_URL = `http://localhost:9090/model`;
    }

    if (aggregate.includes("item")) {
      const resp = await axios.get(
        `${
          this.BASE_URL
        }/model/cloudCost/top?window=${window}&costMetric=${costMetric}${getCloudFilters(
          filters
        )}`
      );
      const result_2 = await resp.data;

      return { data: formatItemsForCost(result_2) };
    }

    return [];
  }
}

export default new CloudCostDayTotalsService();
