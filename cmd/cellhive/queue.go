package main

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/spf13/cobra"
)

func newQueueCommand() *cobra.Command {
	queue := &cobra.Command{
		Use:   "queue",
		Short: "manage queues (registry, status, consumer, purge)",
	}
	queue.AddCommand(newKindRegistryLeaves("queue")...)
	queue.AddCommand(
		newQueueStatusCommand(),
		newQueueReplayDLQCommand(),
		newQueueConsumerCommand(),
		newQueuePurgeCommand(),
	)
	return queue
}

func newQueueStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <namespace> <queue>",
		Short: "show queue depth/visible/leased counts",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return queueStatusCore(args[0], args[1])
		},
	}
}

func newQueueReplayDLQCommand() *cobra.Command {
	var limit int
	cmd := &cobra.Command{
		Use:     "replay-dlq <namespace> <queue>",
		Aliases: []string{"replay"},
		Short:   "replay dead-letter messages",
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return queueReplayCore(args[0], args[1], limit)
		},
	}
	cmd.Flags().IntVar(&limit, "limit", 100, "max messages to replay")
	return cmd
}

func newQueuePurgeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "purge <namespace> <queue>",
		Short: "purge all messages in a queue",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmdQueuePurge(args)
		},
	}
}

func newQueueConsumerCommand() *cobra.Command {
	consumer := &cobra.Command{
		Use:   "consumer",
		Short: "manage queue consumers",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("usage: cellhive queue consumer list|add|remove <namespace> <queue> [worker]")
		},
	}
	consumer.AddCommand(
		&cobra.Command{
			Use:   "list <namespace> <queue>",
			Short: "list queue consumers",
			Args:  cobra.ExactArgs(2),
			RunE: func(cmd *cobra.Command, args []string) error {
				return cmdQueueConsumer([]string{"list", args[0], args[1]})
			},
		},
		newQueueConsumerAddCommand(),
		&cobra.Command{
			Use:   "remove <namespace> <queue> <worker>",
			Short: "remove a queue consumer",
			Args:  cobra.ExactArgs(3),
			RunE: func(cmd *cobra.Command, args []string) error {
				return cmdQueueConsumer([]string{"remove", args[0], args[1], args[2]})
			},
		},
	)
	return consumer
}

func newQueueConsumerAddCommand() *cobra.Command {
	var batchSize, maxRetries, maxConcurrency int
	var deadLetter string
	cmd := &cobra.Command{
		Use:   "add <namespace> <queue> <worker>",
		Short: "add or update a queue consumer",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			legacy := []string{"add", args[0], args[1], args[2],
				"--batch-size", strconv.Itoa(batchSize),
				"--max-retries", strconv.Itoa(maxRetries),
				"--max-concurrency", strconv.Itoa(maxConcurrency)}
			if deadLetter != "" {
				legacy = append(legacy, "--dead-letter-queue", deadLetter)
			}
			return cmdQueueConsumer(legacy)
		},
	}
	cmd.Flags().IntVar(&batchSize, "batch-size", 0, "max messages per batch")
	cmd.Flags().IntVar(&maxRetries, "max-retries", 0, "maximum retries")
	cmd.Flags().IntVar(&maxConcurrency, "max-concurrency", 0, "maximum concurrent invocations")
	cmd.Flags().StringVar(&deadLetter, "dead-letter-queue", "", "dead letter queue")
	return cmd
}

func queueStatusCore(namespace, queue string) error {
	q := url.Values{"namespace": {namespace}, "queue": {queue}}
	data, err := adminCall(http.MethodGet, "/v1/control/queue/status?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}

func queueReplayCore(namespace, queue string, limit int) error {
	q := url.Values{"namespace": {namespace}, "queue": {queue}, "limit": {strconv.Itoa(limit)}}
	data, err := adminCall(http.MethodPost, "/v1/control/queue/replay-dlq?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	printJSON(data)
	return nil
}
