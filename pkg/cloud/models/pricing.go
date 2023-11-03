package models

type PricingMetadata struct {
	Currency string   `json:"currency"`
	Source   string   `json:"source"`
	Warnings []string `json:"warnings,omitempty"`
}
