package cloud

// Azure simply falls back to the CustomProvider for most calls. TODO: Implement this provider
type Azure struct {
	*CustomProvider
}

// DownloadPricingData uses provided azure "best guesses" for pricing
func (a *Azure) DownloadPricingData() error {
	if a.CustomProvider.Pricing == nil {
		m := make(map[string]*NodePrice)
		a.CustomProvider.Pricing = m
	}
	p, err := GetDefaultPricingData("azure.json")
	if err != nil {
		return err
	}
	a.CustomProvider.Pricing["default"] = &NodePrice{
		CPU: p.CPU,
		RAM: p.RAM,
	}
	a.CustomProvider.Pricing["default,spot"] = &NodePrice{
		CPU: p.SpotCPU,
		RAM: p.SpotRAM,
	}
	return nil
}
