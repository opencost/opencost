package external

// Merge returns a new map containing all entries from base plus any entries
// from external whose keys are not already present in base. When the same key
// exists in both, the base value wins.
func Merge(base, external map[string]string) map[string]string {
	if len(external) == 0 {
		return base
	}
	out := make(map[string]string, len(base)+len(external))
	for k, v := range external {
		out[k] = v
	}
	for k, v := range base {
		out[k] = v // base overwrites external on conflict
	}
	return out
}
