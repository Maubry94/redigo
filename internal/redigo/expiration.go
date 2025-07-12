package redigo

import (
	"redigo/internal/redigo/types"
	"time"

	"github.com/samber/lo"
)

func (database *RedigoDB) GetTtl(key string) (int64, bool) {
	database.storeMutex.Lock()
	defer database.storeMutex.Unlock()

	return lo.Ternary(
		lo.HasKey(database.store, key),
		func() (int64, bool) {
			expirationTime, hasExpiry := database.expirationKeys[key]

			return lo.Ternary(
				hasExpiry,
				func() (int64, bool) {
					now := time.Now().Unix()
					remainingTime := expirationTime - now

					return lo.Ternary(
						remainingTime > 0,
						func() (int64, bool) {
							return remainingTime, true
						},
						func() (int64, bool) {
							database.SafeRemoveKey(key)
							return -1, false
						},
					)()
				},
				func() (int64, bool) {
					return 0, true
				},
			)()
		},
		func() (int64, bool) {
			return -1, false
		},
	)()
}

func (database *RedigoDB) handleTtlRestoration(command types.Command) {
	shouldProcess := lo.Ternary(
		command.Ttl != nil && *command.Ttl > 0,
		true,
		false,
	)

	if !shouldProcess {
		return
	}

	now := time.Now().Unix()
	expirationTime := command.Timestamp + *command.Ttl

	action := lo.Ternary(
		expirationTime > now,
		func() {
			database.expirationKeys[command.Key] = expirationTime
		},
		func() {
			database.UnsafeRemoveKey(command.Key)
		},
	)

	action()
}

func (database *RedigoDB) applyExpiration(key string, commandTimestamp, seconds int64) {
	now := time.Now().Unix()
	elapsedTime := now - commandTimestamp
	remainingTime := seconds - elapsedTime

	if remainingTime > 0 {
		database.expirationKeys[key] = now + remainingTime
	} else {
		database.UnsafeRemoveKey(key)
	}
}

func (database *RedigoDB) StartDataExpirationListener() {
	ticker := time.NewTicker(database.envs.DataExpirationInterval)

	cleanupHandler := func() {
		now := time.Now().Unix()

		database.storeMutex.Lock()

		expiredKeys := lo.FilterMap(
			lo.Entries(database.expirationKeys),
			func(entry lo.Entry[string, int64], _ int) (string, bool) {
				return entry.Key, now > entry.Value
			},
		)

		lo.ForEach(expiredKeys, func(key string, _ int) {
			database.SafeRemoveKey(key)
		})

		database.storeMutex.Unlock()

		commands := lo.Map(expiredKeys, func(key string, _ int) types.Command {
			return types.Command{
				Name:      "DELETE",
				Key:       key,
				Value:     types.CommandValue{},
				Timestamp: now,
			}
		})

		lo.ForEach(commands, func(command types.Command, _ int) {
			database.AddCommandsToAofBuffer(command)
		})
	}

	for range ticker.C {
		cleanupHandler()
	}
}
