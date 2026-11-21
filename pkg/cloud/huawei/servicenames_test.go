package huawei

import "testing"

func TestServiceDisplayName(t *testing.T) {
	cases := map[string]string{
		"hws.service.type.ec2":        "ECS",
		"hws.service.type.vpn":        "VPN",
		"HWS.SERVICE.TYPE.CCE":        "CCE",
		"hws.service.type.modelarts":  "ModelArts (MaaS)",
		"hws.service.type.newservice": "NEWSERVICE",
		"Elastic Cloud Server":        "Elastic Cloud Server",
		"":                            "",
	}
	for in, want := range cases {
		if got := serviceDisplayName(in); got != want {
			t.Errorf("serviceDisplayName(%q) = %q, want %q", in, got, want)
		}
	}
}
