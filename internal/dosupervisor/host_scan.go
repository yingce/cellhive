package dosupervisor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// scanHost deliberately does not discover directories. Only callers that know
// workerd's flat layout may opt in; arbitrary sidecars require legacy discovery.
func (s *Supervisor) scanHost(hostID, hash string) ([]string, []string, []byte, *manifestDoc, error) {
	if hash == "" || strings.ContainsAny(hash, ". /\\\x00") {
		return nil, nil, nil, nil, fmt.Errorf("dosupervisor: invalid host hash %q", hash)
	}
	root := filepath.Clean(s.HostStorageDir)
	rel, err := filepath.Rel(s.Dir, root)
	if err != nil || (rel != "." && rel != "cellhive-do-host") {
		return nil, nil, nil, nil, fmt.Errorf("dosupervisor: unsupported host storage root %q", root)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() {
		return nil, nil, nil, nil, fmt.Errorf("dosupervisor: invalid host storage root %q: %v", root, err)
	}
	other := s.Dir
	if rel == "." {
		other = filepath.Join(s.Dir, "cellhive-do-host")
	}
	facetPath := filepath.Join(root, hash+".facets")
	info, err = os.Lstat(facetPath)
	if err != nil || !info.Mode().IsRegular() {
		return nil, nil, nil, nil, fmt.Errorf("dosupervisor: invalid facet index %q: %v", facetPath, err)
	}
	data, err := os.ReadFile(facetPath)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	names, err := ParseFacets(data)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	bases := []string{hash + ".facets", hash + ".sqlite"}
	for i := range names {
		bases = append(bases, hash+"."+strconv.Itoa(i+1)+".sqlite")
	}
	var paths, rels []string
	host := s.bindingForHost(hostID)
	doc := &manifestDoc{
		Hosts:  map[string]hostBinding{hostID: host},
		Facets: map[string][]string{hash: names},
	}
	for i, base := range bases {
		if _, err := os.Lstat(filepath.Join(other, base)); !errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil, nil, fmt.Errorf("dosupervisor: ambiguous host layout %q: %v", base, err)
		}
		if base == hash+".facets" {
			continue
		}
		path := filepath.Join(root, base)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue // A registered facet need not have created SQLite state yet.
		}
		if err != nil || !info.Mode().IsRegular() {
			return nil, nil, nil, nil, fmt.Errorf("dosupervisor: invalid host SQLite %q: %v", path, err)
		}
		paths = append(paths, path)
		rels = append(rels, filepath.ToSlash(filepath.Join(rel, base)))
		if i >= 2 {
			if object, ok := s.objectForFacet(rels[len(rels)-1], hash, i-1, names[i-2], host); ok {
				doc.Objects = append(doc.Objects, object)
			}
		}
	}
	s.facetMu.Lock()
	if s.facetNames == nil {
		s.facetNames = map[string][]string{}
	}
	s.facetNames[hash] = names
	s.facetMu.Unlock()
	return paths, []string{facetPath}, data, doc, nil
}
