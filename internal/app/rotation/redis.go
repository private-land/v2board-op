package rotation

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

// RedisWake retains stream cursors across XREAD calls. Streams contain wakeups,
// not work claims; trimming or losing messages is repaired through the API.
type RedisWake struct {
	Client    *redis.Client
	keys, ids []string
}

func (w *RedisWake) Snapshot(ctx context.Context, keys []string) error {
	if len(w.keys) > 0 {
		return w.Client.Ping(ctx).Err()
	}
	ids := make([]string, len(keys))
	for i, k := range keys {
		entries, err := w.Client.XRevRangeN(ctx, k, "+", "-", 1).Result()
		if err != nil {
			return err
		}
		ids[i] = "0-0"
		if len(entries) > 0 {
			ids[i] = entries[0].ID
		}
	}
	w.keys = append([]string(nil), keys...)
	w.ids = ids
	return nil
}
func (w *RedisWake) Wait(ctx context.Context) error {
	if len(w.keys) == 0 {
		return redis.Nil
	}
	streams := append(append([]string(nil), w.keys...), w.ids...)
	results, err := w.Client.XRead(ctx, &redis.XReadArgs{Streams: streams, Count: 100, Block: time.Second}).Result()
	if err == redis.Nil {
		return errNoWake
	}
	if err != nil {
		return err
	}
	for _, s := range results {
		for i, k := range w.keys {
			if s.Stream == k && len(s.Messages) > 0 {
				w.ids[i] = s.Messages[len(s.Messages)-1].ID
			}
		}
	}
	return nil
}
