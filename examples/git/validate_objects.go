package main

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// Check every new object, including objects the incoming refs do not reach.
// Direct edges suffice once all objects in the durable catalog were validated.
func verifyObjects(st storer.EncodedObjectStorer, ids []plumbing.Hash) error {
	check := func(id plumbing.Hash, kind plumbing.ObjectType) error {
		_, err := st.EncodedObject(kind, id)
		return err
	}
	for _, id := range ids {
		o, err := st.EncodedObject(plumbing.AnyObject, id)
		if err != nil {
			return err
		}
		if o.Type() == plumbing.CommitObject || o.Type() == plumbing.TagObject {
			if err := validateObjectHeaders(o); err != nil {
				return err
			}
		}
		decoded, err := object.DecodeObject(st, o)
		if err != nil {
			return err
		}
		switch v := decoded.(type) {
		case *object.Commit:
			if err := check(v.TreeHash, plumbing.TreeObject); err != nil {
				return err
			}
			for _, p := range v.ParentHashes {
				if err := check(p, plumbing.CommitObject); err != nil {
					return err
				}
			}
		case *object.Tree:
			if err := v.Validate(); err != nil {
				return err
			}
			for _, entry := range v.Entries {
				kind := plumbing.BlobObject
				switch entry.Mode {
				case filemode.Submodule:
					continue
				case filemode.Dir:
					kind = plumbing.TreeObject
				}
				if err := check(entry.Hash, kind); err != nil {
					return err
				}
			}
		case *object.Tag:
			if err := check(v.Target, v.TargetType); err != nil {
				return err
			}
		case *object.Blob:
		default:
			return fmt.Errorf("invalid object kind")
		}
	}
	return nil
}

// go-git normalizes signatures and permits absent identities during decoding.
// Validate the required raw headers without constraining later extension headers.
var identityHeader = regexp.MustCompile(`^[^<>\x00\n]* <[^<>\x00\n]*> [ \t]*(0|[1-9][0-9]*) [+-][0-9]{4}\n$`)

func validIdentityHeader(line string) bool {
	match := identityHeader.FindStringSubmatch(line)
	if match == nil {
		return false
	}
	_, err := strconv.ParseUint(match[1], 10, 63)
	return err == nil
}

func validateObjectHeaders(o plumbing.EncodedObject) error {
	r, err := o.Reader()
	if err != nil {
		return err
	}
	defer r.Close()
	reader := bufio.NewReader(r)
	fields := []string{"tree ", "author ", "committer "}
	if o.Type() == plumbing.TagObject {
		fields = []string{"object ", "type ", "tag "}
	}
	for _, field := range fields {
		line, err := reader.ReadString('\n')
		for field == "author " && err == nil && strings.HasPrefix(line, "parent ") {
			line, err = reader.ReadString('\n')
		}
		if err != nil {
			return err
		}
		value, ok := strings.CutPrefix(line, field)
		if !ok || ((field == "author " || field == "committer ") && !validIdentityHeader(value)) {
			return fmt.Errorf("%s has invalid %sheader", o.Type(), field)
		}
	}
	if o.Type() == plumbing.TagObject {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		// Historical tags may omit tagger; validate it when present.
		if identity, ok := strings.CutPrefix(line, "tagger "); ok && !validIdentityHeader(identity) {
			return fmt.Errorf("tag has invalid tagger header")
		}
	}
	return nil
}
