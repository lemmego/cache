package cache

import (
	"fmt"

	"github.com/lemmego/api/app"
	"github.com/spf13/cobra"
)

// clearCommand empties the cache.
func clearCommand(a app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "cache:clear",
		Short: "Empty the cache",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, cfg, err := resolveForCommand(a)
			if err != nil {
				return err
			}

			// A memory cache lives in one process, and this is a different
			// process from the server. Clearing it here would do nothing at
			// all, which is worth saying out loud rather than reporting
			// success.
			if cfg.Driver == "memory" {
				return fmt.Errorf(
					"the cache driver is %q, which lives in the server's own memory; "+
						"this command runs in a separate process and cannot reach it. "+
						"Restart the server to clear it, or switch to the file or redis driver",
					cfg.Driver)
			}

			if err := c.Flush(cmd.Context()); err != nil {
				return err
			}
			cmd.Printf("Cache cleared (%s).\n", cfg.Driver)
			return nil
		},
	}
}

// forgetCommand removes one key.
func forgetCommand(a app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "cache:forget <key>",
		Short: "Remove one key from the cache",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, _, err := resolveForCommand(a)
			if err != nil {
				return err
			}

			forgotten, err := c.Forget(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if !forgotten {
				cmd.Printf("%q was not in the cache.\n", args[0])
				return nil
			}
			cmd.Printf("Forgot %q.\n", args[0])
			return nil
		},
	}
}

// pruneCommand reclaims expired entries on stores that need an explicit sweep.
func pruneCommand(a app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "cache:prune",
		Short: "Reclaim expired cache entries",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, cfg, err := resolveForCommand(a)
			if err != nil {
				return err
			}

			pruner, ok := c.Store().(Pruner)
			if !ok {
				cmd.Printf("The %s driver reclaims expired entries on its own; there is nothing to prune.\n", cfg.Driver)
				return nil
			}
			if err := pruner.Prune(cmd.Context()); err != nil {
				return err
			}
			cmd.Printf("Pruned expired entries (%s).\n", cfg.Driver)
			return nil
		},
	}
}

// resolveForCommand finds the cache a command should act on.
func resolveForCommand(a app.App) (*Cache, *Config, error) {
	c, ok := app.Lookup[*Cache](a)
	if !ok {
		return nil, nil, fmt.Errorf("cache: no cache is registered; add &cache.Provider{} to bootstrap/providers.go")
	}
	cfg, ok := app.Lookup[*Config](a)
	if !ok {
		cfg = DefaultConfig()
	}
	return c, cfg, nil
}
