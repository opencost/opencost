import axios from 'axios';
import { useLocation } from 'react-router';

const env = process.env;

class AllocationService {
  BASE_URL = process.env.BASE_URL || 'http://localhost:9090/allocation';

  async fetchAllocation(win, aggregate, options) {
    const url = process.env.BASE_URL ?? "";
    const baseUrl = `${url}${this.BASE_PATH}`;
    const { accumulate, filters, } = options;
    const params = {
      window: win,
      aggregate: aggregate,
      step: '1d',
    };
    if (typeof accumulate === 'boolean') {
      params.accumulate = accumulate;
    }
    const result = await axios.get(`${baseUrl}/compute`, { params });
    return result.data;
  }
}

export default new AllocationService();
