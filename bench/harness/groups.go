package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// runGroups traite `harness groups delete`.
func runGroups(ctx context.Context, args []string) error {
	if len(args) < 1 || args[0] != "delete" {
		return errors.New("usage: harness groups delete --group <g> [--bootstrap <host:port>]")
	}
	fs, bootstrap := newFlagSet("groups delete")
	group := fs.String("group", "", "consumer group to delete")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("--group is required")
	}

	cl, err := newClient(ctx, *bootstrap)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	resps, err := adm.DeleteGroups(ctx, *group)
	if err != nil {
		return fmt.Errorf("deleting group %s: %w", *group, err)
	}
	r, ok := resps[*group]
	if !ok {
		return fmt.Errorf("deleting group %s: no response", *group)
	}
	switch {
	case r.Err == nil:
		logger.Printf("group %s deleted", *group)
		return nil
	case errors.Is(r.Err, kerr.GroupIDNotFound):
		logger.Printf("group %s unknown, nothing to delete", *group)
		return nil
	case errors.Is(r.Err, kerr.NonEmptyGroup):
		return fmt.Errorf("group %s still has active members: stop the consumers first", *group)
	default:
		return fmt.Errorf("deleting group %s: %w", *group, r.Err)
	}
}
