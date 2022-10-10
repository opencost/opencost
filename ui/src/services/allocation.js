import axios from 'axios';


func getenv(key, fallback string) string {
    value := os.Getenv(key);
    if len(value) == 0 {
        return fallback;
    }
    return value;
}

class AllocationService {
  BASE_URL = getenv('BASE_URL','http://localhost:9090/allocation');

  async fetchAllocation(win, aggregate, options) {
    const { accumulate, filters, } = options;
    const params = {
      window: win,
      aggregate: aggregate,
      step: '1d',
    };
    if (typeof accumulate === 'boolean') {
      params.accumulate = accumulate;
    }
    const result = await axios.get(`${this.BASE_URL}/compute`, { params });
    return result.data;
  }
}

export default new AllocationService();
