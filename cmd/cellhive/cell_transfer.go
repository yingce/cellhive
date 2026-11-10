package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"cellhive/internal/cell"
	"github.com/spf13/cobra"
)

func newCellCommand() *cobra.Command {
	command := &cobra.Command{Use: "cell", Short: "transfer complete cell SQLite databases"}
	var output string
	export := &cobra.Command{Use: "export <namespace/class/id>", Short: "export a complete SQLite database", Args: cobra.ExactArgs(1)}
	export.Flags().StringVar(&output, "output", "", "destination SQLite file (must not exist)")
	export.RunE = func(cmd *cobra.Command, args []string) error {
		if output == "" {
			return fmt.Errorf("--output is required")
		}
		if _, err := cell.ParseScope(args[0]); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(cmd.Context(), http.MethodGet, adminURL()+"/v1/control/cell/export?scope="+url.QueryEscape(args[0]), nil)
		if err != nil {
			return err
		}
		if err = authorizeAdmin(req); err != nil {
			return err
		}
		response, err := (&http.Client{Timeout: 60 * time.Minute}).Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			b, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			return fmt.Errorf("export HTTP %d: %s", response.StatusCode, b)
		}
		// A sibling temporary file and exclusive link prevent partial exports or
		// accidental replacement of an existing user file.
		f, err := os.CreateTemp(filepath.Dir(output), ".cellhive-export-*")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		defer f.Close()
		if _, err = io.Copy(f, response.Body); err != nil {
			return err
		}
		if err = f.Sync(); err != nil {
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		if err = os.Link(f.Name(), output); err != nil {
			return err
		}
		dir, err := os.Open(filepath.Dir(output))
		if err != nil {
			return err
		}
		defer dir.Close()
		return dir.Sync()
	}
	imp := &cobra.Command{Use: "import <namespace/class/id> <sqlite-file>", Short: "import SQLite into a new cell (existing data is rejected)", Args: cobra.ExactArgs(2)}
	imp.RunE = func(cmd *cobra.Command, args []string) error {
		if _, err := cell.ParseScope(args[0]); err != nil {
			return err
		}
		if wal, err := os.Stat(args[1] + "-wal"); err == nil && wal.Size() > 0 {
			return fmt.Errorf("SQLite file has a nonempty WAL; export a consistent standalone SQLite file before importing")
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
		f, err := os.Open(args[1])
		if err != nil {
			return err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(cmd.Context(), http.MethodPost, adminURL()+"/v1/control/cell/import?scope="+url.QueryEscape(args[0]), f)
		if err != nil {
			return err
		}
		req.ContentLength = st.Size()
		req.Header.Set("Content-Type", "application/vnd.sqlite3")
		if err = authorizeAdmin(req); err != nil {
			return err
		}
		data, err := doJSONClient(req, &http.Client{Timeout: 60 * time.Minute})
		if err != nil {
			return err
		}
		printJSON(data)
		return nil
	}
	command.AddCommand(export, imp)
	return command
}
