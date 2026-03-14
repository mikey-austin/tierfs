package metrics

import (
	dto "github.com/prometheus/client_model/go"
)

// Gather returns the current snapshot of all registered metrics.
// It is primarily used in tests to assert counter and gauge values.
func (r *Registry) Gather() ([]*dto.MetricFamily, error) {
	return r.prom.Gather()
}
