import axios from "axios";
import { getCloudFilters, formatSampleItemsForGraph } from "../util";

class CloudCostTopService {
  BASE_URL = process.env.BASE_URL || "{PLACEHOLDER_BASE_URL}";

  async fetchCloudCostData(window, aggregate, costMetric, filters) {
    if (this.BASE_URL.includes("PLACEHOLDER_BASE_URL")) {
      this.BASE_URL = `http://localhost:9090/model`;
    }

    const params = {
      window,
      aggregate,
      costMetric,
      filters,
    };

    if (aggregate.includes("Item")) {
      console.log("here");
      console.log(aggregate);
      const resp = await fetch(
        `${
          this.BASE_URL
        }/model/cloudCost/top?window=${window}&costMetric=${costMetric}${getCloudFilters(
          filters
        )}`
      );
      const result_2 = await Promise.resolve(resp.data.json());
      const whatData = formatSampleItemsForGraph(result_2);
      console.log(whatData);
      return formatSampleItemsForGraph(result_2);
    }

    const result = await axios.get(`${this.BASE_URL}/model/cloudCost/view`, {
      params,
    });
    return result.data;
  }
}

export default new CloudCostTopService();
