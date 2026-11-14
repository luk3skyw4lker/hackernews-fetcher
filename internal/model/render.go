package model

import (
	"fmt"
	"html"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	tagRe   = regexp.MustCompile(`<[^>]*>`)
	spaceRe = regexp.MustCompile(`\s+`)
)

// PlainText strips HTML tags, unescapes entities and collapses whitespace.
func PlainText(s string) string {
	s = strings.NewReplacer("<p>", " ", "<P>", " ").Replace(s)
	s = html.UnescapeString(tagRe.ReplaceAllString(s, ""))
	return strings.TrimSpace(spaceRe.ReplaceAllString(s, " "))
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// RenderText writes a human-readable, indented view of a snapshot.
func RenderText(w io.Writer, s *Snapshot) error {
	p := &printer{w: w}
	header := "Snapshot"
	if s.ID != 0 {
		header += fmt.Sprintf(" #%d", s.ID)
	}
	at := s.StartedAt
	if s.FinishedAt != nil {
		at = *s.FinishedAt
	}
	p.f("%s @ %s [%s] — %d items, %d authors, %d missing, %d failed\n",
		header, at.UTC().Format(time.RFC3339), s.Status,
		s.Stats.ItemsFetched, s.Stats.AuthorsFetched, s.Stats.MissingItems, s.Stats.FailedItems)
	if s.Error != "" {
		p.f("  ! %s\n", s.Error)
	}
	for _, sk := range s.SkippedIDs {
		p.f("  (skipped #%d at HN rank %d: %s)\n", sk.ID, sk.HNRank, sk.Reason)
	}
	for _, st := range s.Stories {
		p.f("\n")
		renderStory(p, st)
	}
	if len(s.Authors) > 0 {
		p.f("\nAuthors (%d):\n", len(s.Authors))
		names := make([]string, 0, len(s.Authors))
		for n := range s.Authors {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			a := s.Authors[n]
			if a.Status != StatusOK {
				p.f("  %s [%s]%s\n", n, a.Status, reasonSuffix(a.ErrorReason))
				continue
			}
			created := ""
			if a.Created != nil {
				created = ", since " + a.Created.UTC().Format("2006-01-02")
			}
			karma := 0
			if a.Karma != nil {
				karma = *a.Karma
			}
			p.f("  %s (karma %d%s)\n", n, karma, created)
		}
	}
	return p.err
}

func renderStory(p *printer, st *Story) {
	t := st.Tree
	in := st.Integrity
	desc := "?"
	if in.ReportedDescendants != nil {
		desc = fmt.Sprint(*in.ReportedDescendants)
	}
	completeness := "complete"
	if !in.Complete {
		completeness = fmt.Sprintf("INCOMPLETE: %d missing, %d error", len(filter(st.MissingItems, StatusMissing)), len(filter(st.MissingItems, StatusError)))
	}
	title := PlainText(t.Title)
	if t.Status != StatusOK {
		title = "[" + string(t.Status) + "]"
	}
	score := 0
	if t.Score != nil {
		score = *t.Score
	}
	p.f("[%d] #%d %q by %s (%s, score %d, HN rank %d)\n", st.Rank, t.ID, title, orDash(t.By), orDash(t.Type), score, st.HNRank)
	p.f("    comments: HN reports %s, fetched %d (ok %d, deleted %d, dead %d, missing %d, error %d), max depth %d — %s\n",
		desc, in.FetchedComments, in.OK, in.Deleted, in.Dead, in.Missing, in.Error, in.MaxDepth, completeness)
	if len(t.Parts) > 0 {
		p.f("    poll options:\n")
		for _, part := range t.Parts {
			p.f("      - %s\n", nodeLine(part))
		}
	}
	renderChildren(p, t.Children, "    ")
}

func renderChildren(p *printer, kids []*Node, prefix string) {
	for i, k := range kids {
		last := i == len(kids)-1
		branch, next := "├─ ", "│  "
		if last {
			branch, next = "└─ ", "   "
		}
		p.f("%s%s%s\n", prefix, branch, nodeLine(k))
		renderChildren(p, k.Children, prefix+next)
	}
}

func nodeLine(n *Node) string {
	switch n.Status {
	case StatusMissing:
		return fmt.Sprintf("#%d [missing]", n.ID)
	case StatusError:
		return fmt.Sprintf("#%d [error%s]", n.ID, reasonSuffix(n.ErrorReason))
	case StatusDeleted:
		return fmt.Sprintf("#%d [deleted]", n.ID)
	}
	body := PlainText(n.Text)
	if n.Relation == RelationPart && n.Score != nil {
		body += fmt.Sprintf(" (%d votes)", *n.Score)
	}
	tag := ""
	if n.Status == StatusDead {
		tag = " [dead]"
		if body == "[dead]" { // HN's placeholder text; the tag already says it
			body = ""
		}
	}
	line := fmt.Sprintf("#%d %s%s", n.ID, orDash(n.By), tag)
	if body != "" {
		line += ": " + truncate(body, 100)
	}
	return line
}

func reasonSuffix(r string) string {
	if r == "" {
		return ""
	}
	return ": " + r
}

func filter(items []MissingItem, s NodeStatus) []MissingItem {
	var out []MissingItem
	for _, it := range items {
		if it.Status == s {
			out = append(out, it)
		}
	}
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

type printer struct {
	w   io.Writer
	err error
}

func (p *printer) f(format string, args ...any) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintf(p.w, format, args...)
}
