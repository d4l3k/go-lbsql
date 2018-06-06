package lbsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/cenkalti/backoff"
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

// randomConnector returns a random connector.
func (b *Balancer) randomConnector() (driver.Connector, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, c := range b.mu.connectors {
		return c, nil
	}
	return nil, ErrNoConnectors
}

// Connect connects to a random driver.Connector. If the connection fails it
// retries using exponential backoff until one succeeds, it times out, or the
// context is canceled.
func (b *Balancer) Connect(ctx context.Context) (driver.Conn, error) {
	eb := backoff.NewExponentialBackOff()
	ctxb := backoff.WithContext(eb, ctx)

	var conn driver.Conn
	if err := backoff.Retry(func() error {
		c, err := b.randomConnector()
		if err != nil {
			return backoff.Permanent(err)
		}

		conn, err = c.Connect(ctx)
		return err
	}, ctxb); err != nil {
		return nil, err
	}
	return conn, nil
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
