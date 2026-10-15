package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
)

func workflowCLIPath(ns, name string) string {
	return "/client/v4/accounts/" + url.PathEscape(ns) + "/workflows/" + url.PathEscape(name) + "/instances"
}

func cmdWorkflowTrigger(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: cellhive workflows trigger <namespace> <workflow> [--params JSON] [--id ID]")
	}
	fs := flag.NewFlagSet("workflows trigger", flag.ContinueOnError)
	params := fs.String("params", "null", "JSON instance parameters")
	id := fs.String("id", "", "optional instance ID")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if len(fs.Args()) != 0 || !json.Valid([]byte(*params)) {
		return fmt.Errorf("workflow params must be valid JSON")
	}
	out, err := adminCall(http.MethodPost, workflowCLIPath(args[0], args[1]), map[string]any{"instance_id": *id, "params": json.RawMessage(*params)})
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func cmdWorkflowInstances(args []string) error {
	if len(args) < 3 {
		return fmt.Errorf("usage: cellhive workflows instances list|describe|pause|resume|terminate|restart|send-event|delete <namespace> <workflow> [id]")
	}
	verb, base := args[0], workflowCLIPath(args[1], args[2])
	var method, path string
	var body any
	switch verb {
	case "list":
		if len(args) != 3 {
			return fmt.Errorf("usage: cellhive workflows instances list <namespace> <workflow>")
		}
		method, path = http.MethodGet, base
	case "describe", "pause", "resume", "terminate", "restart", "delete":
		if len(args) != 4 {
			return fmt.Errorf("usage: cellhive workflows instances %s <namespace> <workflow> <id>", verb)
		}
		id := url.PathEscape(args[3])
		switch verb {
		case "describe":
			method, path = http.MethodGet, base+"/"+id
		case "delete":
			method, path = http.MethodPost, base+"/batch/delete"
			body = map[string]any{"instances": []string{args[3]}}
		default:
			method, path = http.MethodPatch, base+"/"+id+"/status"
			body = map[string]string{"status": verb}
		}
	case "send-event":
		if len(args) != 6 || !json.Valid([]byte(args[5])) {
			return fmt.Errorf("usage: cellhive workflows instances send-event <namespace> <workflow> <id> <type> <JSON>")
		}
		method, path = http.MethodPost, base+"/"+url.PathEscape(args[3])+"/events/"+url.PathEscape(args[4])
		body = json.RawMessage(args[5])
	default:
		return fmt.Errorf("unsupported workflow instance command %q", verb)
	}
	out, err := adminCall(method, path, body)
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}
