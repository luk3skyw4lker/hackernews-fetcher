package model

// Walk visits n and its descendants in display order: the node, then its
// comments (each with its subtree), then its poll options.
func Walk(n *Node, fn func(*Node)) {
	if n == nil {
		return
	}
	fn(n)
	for _, c := range n.Children {
		Walk(c, fn)
	}
	for _, p := range n.Parts {
		Walk(p, fn)
	}
}

// ComputeIntegrity derives a story's Integrity from its tree.
func ComputeIntegrity(root *Node) Integrity {
	in := Integrity{Complete: true}
	if root == nil {
		in.Complete = false
		return in
	}
	in.ReportedDescendants = root.Descendants
	Walk(root, func(n *Node) {
		if n.Depth > in.MaxDepth {
			in.MaxDepth = n.Depth
		}
		if n.Status == StatusMissing || n.Status == StatusError {
			in.Complete = false
		}
		switch n.Relation {
		case RelationPart:
			in.PartsFetched++
		case RelationKid:
			in.FetchedComments++
			switch n.Status {
			case StatusOK:
				in.OK++
			case StatusDeleted:
				in.Deleted++
			case StatusDead:
				in.Dead++
			case StatusMissing:
				in.Missing++
			case StatusError:
				in.Error++
			}
		}
	})
	return in
}

// CollectMissing lists every non-ok node of a tree in display order.
func CollectMissing(root *Node) []MissingItem {
	out := []MissingItem{}
	Walk(root, func(n *Node) {
		if n.Status == StatusOK {
			return
		}
		out = append(out, MissingItem{
			ID: n.ID, Status: n.Status, Relation: n.Relation, ParentID: n.ParentID,
			Depth: n.Depth, Path: n.Path, ErrorReason: n.ErrorReason,
		})
	})
	return out
}

// ComputeStats derives snapshot-wide counters from its stories and authors.
func ComputeStats(stories []*Story, authors map[string]*Author) Stats {
	var st Stats
	for _, s := range stories {
		Walk(s.Tree, func(n *Node) {
			st.ItemsFetched++
			switch n.Status {
			case StatusMissing:
				st.MissingItems++
			case StatusError:
				st.FailedItems++
			}
		})
	}
	st.AuthorsFetched = len(authors)
	return st
}

// DeriveStatus returns completed when nothing errored, partial otherwise.
func DeriveStatus(st Stats, authors map[string]*Author) SnapshotStatus {
	if st.FailedItems > 0 {
		return SnapshotPartial
	}
	for _, a := range authors {
		if a.Status == StatusError {
			return SnapshotPartial
		}
	}
	return SnapshotCompleted
}

// AuthorsOf returns the subset of authors referenced by the given trees.
func AuthorsOf(all map[string]*Author, roots ...*Node) map[string]*Author {
	out := map[string]*Author{}
	for _, r := range roots {
		Walk(r, func(n *Node) {
			if n.By == "" {
				return
			}
			if a, ok := all[n.By]; ok {
				out[n.By] = a
			}
		})
	}
	return out
}

// ItemsBy returns the ids of every node authored by username, in display order.
func ItemsBy(stories []*Story, username string) []int64 {
	ids := []int64{}
	for _, s := range stories {
		Walk(s.Tree, func(n *Node) {
			if n.By == username {
				ids = append(ids, n.ID)
			}
		})
	}
	return ids
}

// Limit returns a shallow copy of s with only the first n stories and the
// authors those stories reference. n <= 0 or n >= len(stories) returns s.
func (s *Snapshot) Limit(n int) *Snapshot {
	if n <= 0 || n >= len(s.Stories) {
		return s
	}
	cp := *s
	cp.Stories = s.Stories[:n]
	roots := make([]*Node, 0, n)
	for _, st := range cp.Stories {
		roots = append(roots, st.Tree)
	}
	cp.Authors = AuthorsOf(s.Authors, roots...)
	return &cp
}
