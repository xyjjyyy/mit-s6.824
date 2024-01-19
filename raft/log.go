package raft

import (
	"context"
	"log/slog"
)

type NilHandler struct{}

func (*NilHandler) Enabled(_ context.Context, level slog.Level) bool { return false }
func (*NilHandler) Handle(_ context.Context, r slog.Record) error    { return nil }
func (nh *NilHandler) WithAttrs(attrs []slog.Attr) slog.Handler      { return nh }
func (nh *NilHandler) WithGroup(name string) slog.Handler            { return nh }
