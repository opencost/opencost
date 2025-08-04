package otc

import (
	"fmt"
	"strings"
)

// Builds query string for serviceName[0]=ecs&serviceName[1]=memo&...
func buildServiceNameQueryParam(serviceNames []string) string {
	var sb strings.Builder
	for i, name := range serviceNames {
		sb.WriteString(fmt.Sprintf("serviceName[%d]=%s", i, name))
		if i < len(serviceNames)-1 {
			sb.WriteString("&")
		}
	}
	return sb.String()
}
