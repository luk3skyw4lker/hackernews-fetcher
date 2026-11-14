package store

import "github.com/luk3skyw4lker/hackernews-fetcher/internal/model"

// buildTrees turns flat item rows into nested trees in O(n) and returns the
// roots keyed by story id. Rows must be ordered so that a parent precedes its
// children and siblings appear in position order (loadItems does this).
func buildTrees(nodes []*model.Node) map[int64]*model.Node {
	byID := make(map[int64]*model.Node, len(nodes))
	roots := map[int64]*model.Node{}
	for _, n := range nodes {
		byID[n.ID] = n
		if n.Relation == model.RelationRoot || n.ParentID == nil {
			roots[n.ID] = n
			continue
		}
		p := byID[*n.ParentID]
		if p == nil {
			continue // orphan: cannot happen with rows written by SaveSnapshot
		}
		if n.Relation == model.RelationPart {
			p.Parts = append(p.Parts, n)
		} else {
			p.Children = append(p.Children, n)
		}
	}
	return roots
}
