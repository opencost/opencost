package clustercache

func GetLoadBalancerIngressAddress(service *Service) []string {
	var addresses []string
	for _, loadBalancerIngress := range service.Status.LoadBalancer.Ingress {
		address := loadBalancerIngress.IP
		// Some cloud providers use hostname rather than IP
		if address == "" {
			address = loadBalancerIngress.Hostname
		}
		addresses = append(addresses, address)

	}
	return addresses
}
