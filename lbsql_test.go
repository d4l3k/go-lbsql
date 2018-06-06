package lbsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
)

type testConnector struct{}

func (testConnector) Connect(context.Context) (driver.Conn, error) { return nil, nil }
func (testConnector) Driver() driver.Driver                        { return nil }

type errConnector struct{}

func (errConnector) Connect(context.Context) (driver.Conn, error) { return nil, errors.New("err") }
func (errConnector) Driver() driver.Driver                        { return nil }

func TestBalancer(t *testing.T) {
	b := NewBalancer()
	if _, err := b.Connect(context.Background()); err != ErrNoConnectors {
		t.Fatalf("expected %+v; got %+v", ErrNoConnectors, err)
	}

	foo := testConnector{}
	b.Add("foo", foo)
	b.Add("bar", nil)
	if len(b.mu.connectors) != 2 {
		t.Fatalf("expected 2 connectors")
	}
	b.Remove("bar")
	if len(b.mu.connectors) != 1 {
		t.Fatalf("expected 1 connectors")
	}

	connector, err := b.randomConnector()
	if err != nil {
		t.Fatal(err)
	}
	if connector != foo {
		t.Fatalf("expected randomConnector = foo")
	}

	b.Add("err", errConnector{})

	if _, err := b.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
}
