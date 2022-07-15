package kubecost

import "encoding"

// ETLSet is an interface which represents the basic data block of an ETL. It is keyed by its Window
type ETLSet interface {
	ConstructSet() ETLSet
	CloneSet() ETLSet
	IsEmpty() bool
	GetWindow() Window

	// Representations
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}
