package lbsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
)

// ErrNoConnectors is returned when there are no connectors added to the
// balancer.
var ErrNoConnectors = errors.New("lbsql: no available connectors")

var _ driver.Driver = &Balancer{}
var _ driver.Connector = &Balancer{}
var _ driver.DriverContext = &Balancer{}

// Balancer is a driver.Connector that randomly picks between the connectors
// that have been added to it when establishing connections.
type Balancer struct {
	mu struct {
		sync.Mutex

		connectors map[string]driver.Connector
	}
}

// NewBalancer returns a Balancer.
func NewBalancer() *Balancer {
	b := &Balancer{}
	b.mu.connectors = map[string]driver.Connector{}
	return b
}

// Add adds a driver.Connector to the balancer.
func (b *Balancer) Add(name string, c driver.Connector) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.mu.connectors[name] = c
}

// Remove removes a connector from the balancer.
func (b *Balancer) Remove(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.mu.connectors, name)
}

// ConnectorNames returns a list of all the names of connectors currently in the
// balancer.
func (b *Balancer) ConnectorNames() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	var names []string
	for name := range b.mu.connectors {
		names = append(names, name)
	}
	return names
}

// randomConnectors returns the connectors in a random order.
func (b *Balancer) randomConnectors() []driver.Connector {
	b.mu.Lock()
	defer b.mu.Unlock()

	var connectors []driver.Connector
	for _, c := range b.mu.connectors {
		connectors = append(connectors, c)
	}

	return connectors
}

// Connect connects to a random driver.Connector. If the connection fails it
// retries all the available connectors until one succeeds, or the context is
// canceled.
func (b *Balancer) Connect(ctx context.Context) (driver.Conn, error) {
	connectors := b.randomConnectors()

	if len(connectors) == 0 {
		return nil, ErrNoConnectors
	}

	var conn driver.Conn
	var err error
	for _, c := range connectors {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		conn, err = c.Connect(ctx)
		if err == nil {
			return conn, nil
		}
	}
	return nil, err
}

// Open is a thin wrapper around Connect.
func (b *Balancer) Open(_ string) (driver.Conn, error) {
	return b.Connect(context.Background())
}

// Driver returns the balancer.
func (b *Balancer) Driver() driver.Driver {
	return b
}

// OpenConnector returns the balancer.
func (b *Balancer) OpenConnector(name string) (driver.Connector, error) {
	return b, nil
}
