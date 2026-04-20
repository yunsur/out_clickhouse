package main

/*
#include <stdlib.h>

struct flb_plugin_proxy_context {
	void *remote_context;
};

struct flbgo_output_plugin {
	void *_;
	void *api;
	void *o_ins;
	struct flb_plugin_proxy_context *context;
};
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

var ctxRegistry sync.Map

func setPluginContext(plugin unsafe.Pointer, p *ClickHousePlugin) (func(), error) {
	out := (*C.struct_flbgo_output_plugin)(plugin)
	if out == nil || out.context == nil {
		return nil, fmt.Errorf("plugin proxy context is nil; fluent-bit ABI mismatch (< 1.9?)")
	}

	token := C.malloc(1)
	if token == nil {
		return nil, fmt.Errorf("malloc failed for context token")
	}
	*(*C.char)(token) = 0
	key := uintptr(token)
	ctxRegistry.Store(key, p)

	out.context.remote_context = token

	return func() {
		ctxRegistry.Delete(key)
		C.free(token)
	}, nil
}

func pluginOutputInstance(plugin unsafe.Pointer) unsafe.Pointer {
	out := (*C.struct_flbgo_output_plugin)(plugin)
	if out == nil {
		return nil
	}
	return out.o_ins
}

func getPluginContext(ctxPtr unsafe.Pointer) (*ClickHousePlugin, bool) {
	if ctxPtr == nil {
		return nil, false
	}
	v, ok := ctxRegistry.Load(uintptr(ctxPtr))
	if !ok {
		return nil, false
	}
	p, ok := v.(*ClickHousePlugin)
	return p, ok
}
