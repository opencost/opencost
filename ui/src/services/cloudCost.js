import axios from "axios";

class CloudCostService {
  BASE_URL = process.env.BASE_URL || "{PLACEHOLDER_BASE_URL}";

  async fetchCloudCostData(window, aggregate, costMetric) {
    if (this.BASE_URL.includes("PLACEHOLDER_BASE_URL")) {
      this.BASE_URL = `http://localhost:9090/model`;
    }

    const params = {
      window,
      aggregate,
      costMetric,
      accumulate: false,
    };

    const result = await axios.get(`${this.BASE_URL}/model/cloudCost/view`, {
      params,
    });
    return result.data;
  }
}

export default new CloudCostService();
