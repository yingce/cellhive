package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
)

func cmdQueueConsumer(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: cellhive queues consumer list|add|remove <namespace> <queue> [worker]")
	}
	verb, ns, queue := args[0], args[1], args[2]
	base := "/client/v4/accounts/" + url.PathEscape(ns) + "/queues/" + url.PathEscape(queue) + "/consumers"
	switch verb {
	case "list":
		if len(args) != 3 {
			return fmt.Errorf("usage: cellhive queues consumer list <namespace> <queue>")
		}
		out, err := adminCall(http.MethodGet, base, nil)
		if err != nil {
			return err
		}
		printJSON(out)
		return nil
	case "remove":
		if len(args) != 4 {
			return fmt.Errorf("usage: cellhive queues consumer remove <namespace> <queue> <worker>")
		}
		out, err := adminCall(http.MethodDelete, base+"/"+url.PathEscape(args[3]), nil)
		if err != nil {
			return err
		}
		printJSON(out)
		return nil
	case "add":
		if len(args) < 4 {
			return fmt.Errorf("usage: cellhive queues consumer add <namespace> <queue> <worker> [--batch-size N] [--max-retries N] [--max-concurrency N] [--dead-letter-queue name]")
		}
		fs := flag.NewFlagSet("queues consumer add", flag.ContinueOnError)
		batchSize := fs.Int("batch-size", 0, "max messages per batch")
		maxRetries := fs.Int("max-retries", 0, "maximum retries")
		maxConcurrency := fs.Int("max-concurrency", 0, "maximum concurrent invocations")
		deadLetter := fs.String("dead-letter-queue", "", "dead letter queue")
		if err := fs.Parse(args[4:]); err != nil {
			return err
		}
		if len(fs.Args()) != 0 || *batchSize < 0 || *maxRetries < 0 || *maxConcurrency < 0 {
			return fmt.Errorf("invalid consumer options")
		}
		body := map[string]any{"type": "worker", "script_name": args[3], "dead_letter_queue": *deadLetter,
			"settings": map[string]int{"batch_size": *batchSize, "max_retries": *maxRetries, "max_concurrency": *maxConcurrency}}
		out, err := adminCall(http.MethodPost, base, body)
		if err != nil {
			return err
		}
		printJSON(out)
		return nil
	}
	return fmt.Errorf("usage: cellhive queues consumer list|add|remove <namespace> <queue> [worker]")
}

func cmdQueuePurge(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: cellhive queues purge <namespace> <queue>")
	}
	path := "/client/v4/accounts/" + url.PathEscape(args[0]) + "/queues/" + url.PathEscape(args[1]) + "/purge"
	out, err := adminCall(http.MethodPost, path, nil)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}
