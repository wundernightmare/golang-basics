package cache_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tracehubmmp/golang-basics/services/tasks/internal/cache"
	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

func TestNone_EveryReadIsALoad(t *testing.T) {
	t.Parallel()
	var c cache.None
	ctx := context.Background()
	c.Warm(ctx, domain.Task{ID: "a"})
	loads := 0
	for range 2 {
		task, hit, err := c.Get(ctx, "a", func(context.Context) (domain.Task, error) {
			loads++
			return domain.Task{ID: "a", Title: "from the store"}, nil
		})
		require.NoError(t, err)
		assert.False(t, hit)
		assert.Equal(t, "from the store", task.Title)
	}
	assert.Equal(t, 2, loads, "nothing is kept")

	_, _, err := c.Get(ctx, "b", func(context.Context) (domain.Task, error) { return domain.Task{}, domain.ErrNotFound })
	require.ErrorIs(t, err, domain.ErrNotFound)
	assert.NoError(t, c.Invalidate(ctx, "a"))
}
