package server

import (
	"bytes"
	"errors"
	"strconv"

	"github.com/gofiber/fiber/v3"

	"github.com/luk3skyw4lker/hackernews-fetcher/internal/model"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/refresher"
	"github.com/luk3skyw4lker/hackernews-fetcher/internal/store"
)

// errorBody is the single error shape: {"error":{"code":"...","message":"..."}}.
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type httpError struct {
	status  int
	code    string
	message string
}

func (e *httpError) Error() string { return e.message }

func apiError(status int, code, message string) error {
	return &httpError{status: status, code: code, message: message}
}

func badRequest(message string) error {
	return apiError(fiber.StatusBadRequest, "bad_request", message)
}

func (s *Server) errorHandler(c fiber.Ctx, err error) error {
	var body errorBody
	status := fiber.StatusInternalServerError
	var he *httpError
	var fe *fiber.Error
	switch {
	case errors.As(err, &he):
		status, body.Error.Code, body.Error.Message = he.status, he.code, he.message
	case errors.As(err, &fe):
		status, body.Error.Code, body.Error.Message = fe.Code, codeFor(fe.Code), fe.Message
	default:
		s.log.Error("request failed", "path", c.Path(), "err", err)
		body.Error.Code, body.Error.Message = "internal", "internal server error"
	}
	return c.Status(status).JSON(body)
}

func codeFor(status int) string {
	switch status {
	case fiber.StatusNotFound:
		return "not_found"
	case fiber.StatusMethodNotAllowed:
		return "method_not_allowed"
	case fiber.StatusBadRequest:
		return "bad_request"
	default:
		if status >= 500 {
			return "internal"
		}
		return "error"
	}
}

// intQuery parses an optional integer query parameter within [lo, hi].
func intQuery(c fiber.Ctx, key string, def, lo, hi int64) (int64, error) {
	raw := c.Query(key)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < lo || v > hi {
		return 0, badRequest(key + " must be an integer between " + strconv.FormatInt(lo, 10) + " and " + strconv.FormatInt(hi, 10))
	}
	return v, nil
}

func intParam(c fiber.Ctx, key string) (int64, error) {
	v, err := strconv.ParseInt(c.Params(key), 10, 64)
	if err != nil || v <= 0 {
		return 0, badRequest(key + " must be a positive integer")
	}
	return v, nil
}

func format(c fiber.Ctx) (string, error) {
	f := c.Query("format", "json")
	if f != "json" && f != "text" {
		return "", badRequest("format must be json or text")
	}
	return f, nil
}

// sendSnapshot writes snap as JSON or plain text, honoring ?limit.
func sendSnapshot(c fiber.Ctx, snap *model.Snapshot, cached []byte) error {
	f, err := format(c)
	if err != nil {
		return err
	}
	limit, err := intQuery(c, "limit", 0, 1, int64(max(len(snap.Stories), 1)))
	if err != nil {
		return err
	}
	if limit > 0 {
		snap = snap.Limit(int(limit))
		cached = nil
	}
	if f == "text" {
		var buf bytes.Buffer
		if err := model.RenderText(&buf, snap); err != nil {
			return err
		}
		c.Set(fiber.HeaderContentType, fiber.MIMETextPlainCharsetUTF8)
		return c.Send(buf.Bytes())
	}
	if cached != nil {
		c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSONCharsetUTF8)
		return c.Send(cached)
	}
	return c.JSON(snap)
}

func (s *Server) topStories(c fiber.Ctx) error {
	latest := s.d.Cache.Get()
	if latest == nil {
		return apiError(fiber.StatusServiceUnavailable, "no_snapshot", "no snapshot is available yet; the first crawl is still running")
	}
	return sendSnapshot(c, latest.Snapshot, latest.JSON)
}

func (s *Server) listSnapshots(c fiber.Ctx) error {
	limit, err := intQuery(c, "limit", 20, 1, 100)
	if err != nil {
		return err
	}
	before, err := intQuery(c, "before_id", 0, 1, 1<<62)
	if err != nil {
		return err
	}
	status := model.SnapshotStatus(c.Query("status"))
	switch status {
	case "", model.SnapshotRunning, model.SnapshotCompleted, model.SnapshotPartial, model.SnapshotFailed:
	default:
		return badRequest("status must be one of running, completed, partial, failed")
	}
	list, err := s.d.Store.ListSnapshots(c.Context(), store.ListOptions{Limit: int(limit), BeforeID: before, Status: status})
	if err != nil {
		return err
	}
	resp := fiber.Map{"snapshots": list}
	if len(list) == int(limit) {
		resp["next_before_id"] = list[len(list)-1].ID
	}
	return c.JSON(resp)
}

// loadSnapshot serves the cached snapshot when the id matches, else hits
// the database.
func (s *Server) loadSnapshot(c fiber.Ctx) (*model.Snapshot, []byte, error) {
	id, err := intParam(c, "id")
	if err != nil {
		return nil, nil, err
	}
	if latest := s.d.Cache.Get(); latest != nil && latest.Snapshot.ID == id {
		return latest.Snapshot, latest.JSON, nil
	}
	snap, err := s.d.Store.Snapshot(c.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil, apiError(fiber.StatusNotFound, "not_found", "snapshot "+strconv.FormatInt(id, 10)+" not found")
	}
	return snap, nil, err
}

func (s *Server) snapshot(c fiber.Ctx) error {
	snap, cached, err := s.loadSnapshot(c)
	if err != nil {
		return err
	}
	return sendSnapshot(c, snap, cached)
}

func (s *Server) story(c fiber.Ctx) error {
	storyID, err := intParam(c, "story_id")
	if err != nil {
		return err
	}
	snap, _, err := s.loadSnapshot(c)
	if err != nil {
		return err
	}
	for _, st := range snap.Stories {
		if st.Tree.ID == storyID {
			return c.JSON(fiber.Map{
				"snapshot_id": snap.ID,
				"status":      snap.Status,
				"story":       st,
				"authors":     model.AuthorsOf(snap.Authors, st.Tree),
			})
		}
	}
	return apiError(fiber.StatusNotFound, "not_found", "story "+strconv.FormatInt(storyID, 10)+" is not in snapshot "+strconv.FormatInt(snap.ID, 10))
}

func (s *Server) author(c fiber.Ctx) error {
	latest := s.d.Cache.Get()
	if latest == nil {
		return apiError(fiber.StatusServiceUnavailable, "no_snapshot", "no snapshot is available yet")
	}
	name := c.Params("username")
	a, ok := latest.Snapshot.Authors[name]
	if !ok {
		return apiError(fiber.StatusNotFound, "not_found", "author "+strconv.Quote(name)+" has no posts in the latest snapshot")
	}
	return c.JSON(fiber.Map{
		"snapshot_id": latest.Snapshot.ID,
		"author":      a,
		"item_ids":    model.ItemsBy(latest.Snapshot.Stories, name),
	})
}

func (s *Server) refresh(c fiber.Ctx) error {
	if s.d.Refresher == nil {
		return apiError(fiber.StatusServiceUnavailable, "refresh_disabled", refresher.ErrDisabled.Error())
	}
	switch err := s.d.Refresher.Trigger(); {
	case errors.Is(err, refresher.ErrBusy):
		return apiError(fiber.StatusConflict, "crawl_running", err.Error())
	case errors.Is(err, refresher.ErrDisabled):
		return apiError(fiber.StatusServiceUnavailable, "refresh_disabled", err.Error())
	case err != nil:
		return err
	}
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{"status": "accepted", "message": "crawl scheduled"})
}
