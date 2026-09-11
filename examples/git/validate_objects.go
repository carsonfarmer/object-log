package main

import (
	"bufio"
	"fmt"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"io"
	"regexp"
	"strconv"
	"strings"
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
		switch o.Type() {
		case plumbing.CommitObject:
			if err := validateObjectHeaders(o); err != nil {
				return err
			}
			c, e := object.DecodeCommit(st, o)
			if e != nil {
				return e
			}
			if e = check(c.TreeHash, plumbing.TreeObject); e != nil {
				return e
			}
			for _, p := range c.ParentHashes {
				if e = check(p, plumbing.CommitObject); e != nil {
					return e
				}
			}
		case plumbing.TreeObject:
			tree, e := object.DecodeTree(st, o)
			if e != nil {
				return e
			}
			if e = tree.Validate(); e != nil {
				return e
			}
			for _, entry := range tree.Entries {
				kind := plumbing.BlobObject
				switch entry.Mode {
				case filemode.Submodule:
					continue
				case filemode.Dir:
					kind = plumbing.TreeObject
				case filemode.Regular, filemode.Deprecated, filemode.Executable, filemode.Symlink:
				default:
					return fmt.Errorf("invalid tree mode")
				}
				if e = check(entry.Hash, kind); e != nil {
					return e
				}
			}
		case plumbing.TagObject:
			if err := validateObjectHeaders(o); err != nil {
				return err
			}
			tag, e := object.DecodeTag(st, o)
			if e != nil {
				return e
			}
			if e = check(tag.Target, tag.TargetType); e != nil {
				return e
			}
		case plumbing.BlobObject:
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
	fields := []string{"tree "}
	if o.Type() == plumbing.TagObject {
		fields = []string{"object ", "type ", "tag "}
	}
	for _, field := range fields {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if !strings.HasPrefix(line, field) {
			return fmt.Errorf("%s missing %sheader", o.Type(), field)
		}
	}
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	if o.Type() == plumbing.TagObject {
		// Historical tags may omit tagger; validate it when present.
		if identity, ok := strings.CutPrefix(line, "tagger "); ok && !validIdentityHeader(identity) {
			return fmt.Errorf("tag has invalid tagger header")
		}
		return nil
	}
	for err == nil && strings.HasPrefix(line, "parent ") {
		line, err = reader.ReadString('\n')
	}
	for _, header := range []string{"author ", "committer "} {
		if err != nil {
			return err
		}
		identity, ok := strings.CutPrefix(line, header)
		if !ok || !validIdentityHeader(identity) {
			return fmt.Errorf("commit has invalid %sheader", header)
		}
		if header == "author " {
			line, err = reader.ReadString('\n')
		}
	}
	return nil
}
