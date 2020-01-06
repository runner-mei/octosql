package aggregate

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cube2222/octosql"
	"github.com/cube2222/octosql/streaming/storage"
)

type Count struct {
}

var currentCountPrefix = []byte("$current_count$")

func (agg *Count) AddValue(ctx context.Context, tx storage.StateTransaction, key octosql.Value, value octosql.Value) error {
	currentCountStorage := storage.NewValueState(tx.WithPrefix(currentCountPrefix))

	var currentCount octosql.Value
	err := currentCountStorage.Get(&currentCount)
	if err == storage.ErrKeyNotFound {
		currentCount = octosql.MakeInt(0)
	} else if err != nil {
		return errors.Wrap(err, "couldn't get current count from storage")
	}

	currentCount = octosql.MakeInt(currentCount.AsInt() + 1)

	err = currentCountStorage.Set(&currentCount)
	if err != nil {
		return errors.Wrap(err, "couldn't set current count in storage")
	}

	return nil
}

func (agg *Count) GetValue(ctx context.Context, tx storage.StateTransaction, key octosql.Value) (octosql.Value, error) {
	currentCountStorage := storage.NewValueState(tx.WithPrefix(currentCountPrefix))

	var currentCount octosql.Value
	err := currentCountStorage.Get(&currentCount)
	if err == storage.ErrKeyNotFound {
		return octosql.MakeInt(0), nil
	} else if err != nil {
		return octosql.ZeroValue(), errors.Wrap(err, "couldn't get current count from storage")
	}

	return currentCount, nil
}

func (agg *Count) String() string {
	return "count"
}