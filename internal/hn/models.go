package hn

// Item mirrors the HackerNews item resource. Only `id` is guaranteed; every
// other field may be absent. Unknown fields are ignored on decode.
type Item struct {
	ID          int64   `json:"id"`
	Deleted     bool    `json:"deleted"`
	Type        string  `json:"type"`
	By          string  `json:"by"`
	Time        int64   `json:"time"`
	Text        string  `json:"text"`
	Dead        bool    `json:"dead"`
	Parent      int64   `json:"parent"`
	Poll        int64   `json:"poll"`
	Kids        []int64 `json:"kids"`
	URL         string  `json:"url"`
	Score       *int    `json:"score"`
	Title       string  `json:"title"`
	Parts       []int64 `json:"parts"`
	Descendants *int    `json:"descendants"`
}

// User mirrors the HackerNews user resource.
type User struct {
	ID        string  `json:"id"`
	Created   int64   `json:"created"`
	Karma     int     `json:"karma"`
	About     string  `json:"about"`
	Submitted []int64 `json:"submitted"`
}
