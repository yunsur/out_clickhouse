package main

/*
#cgo linux LDFLAGS: -ldl
#include <stdlib.h>
#include <dlfcn.h>
#include <pthread.h>

typedef void (*fb_log_fn_t)(void *, const char *, ...);

static fb_log_fn_t info_fn = NULL;
static fb_log_fn_t warn_fn = NULL;
static fb_log_fn_t error_fn = NULL;
static fb_log_fn_t debug_fn = NULL;
static fb_log_fn_t trace_fn = NULL;
static pthread_once_t log_fn_once = PTHREAD_ONCE_INIT;

static void resolve_log_fns(void)
{
    info_fn = (fb_log_fn_t) dlsym(RTLD_DEFAULT, "flb_plg_info");
    warn_fn = (fb_log_fn_t) dlsym(RTLD_DEFAULT, "flb_plg_warn");
    error_fn = (fb_log_fn_t) dlsym(RTLD_DEFAULT, "flb_plg_error");
    debug_fn = (fb_log_fn_t) dlsym(RTLD_DEFAULT, "flb_plg_debug");
    trace_fn = (fb_log_fn_t) dlsym(RTLD_DEFAULT, "flb_plg_trace");
}

static int fb_internal_log(void *o_ins, int level, const char *msg)
{
    fb_log_fn_t fn = NULL;

    if (o_ins == NULL || msg == NULL) {
        return 0;
    }

    pthread_once(&log_fn_once, resolve_log_fns);

    switch (level) {
    case 0:
        fn = info_fn;
        break;
    case 1:
        fn = warn_fn;
        break;
    case 2:
        fn = error_fn;
        break;
    case 3:
        fn = debug_fn;
        break;
    default:
        fn = trace_fn;
        break;
    }

    if (fn == NULL) {
        return 0;
    }
    fn(o_ins, "%s", msg);
    return 1;
}
*/
import "C"

import (
	"fmt"
	"runtime/debug"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

const maxFlushPayloadBytes = 64 * 1024 * 1024

func payloadTooLarge(length C.int) bool {
	return length > C.int(maxFlushPayloadBytes)
}

func emitFluentBitLog(instance unsafe.Pointer, level int, message string) bool {
	if instance == nil {
		return false
	}
	if message == "" {
		return true
	}

	msg := C.CString(message)
	defer C.free(unsafe.Pointer(msg))
	return C.fb_internal_log(instance, C.int(level), msg) == 1
}

func safeExportCall(name string, fallback int, fn func() int) (code int) {
	code = fallback
	defer func() {
		if recovered := recover(); recovered != nil {
			logErrorf("%s panic recovered: %v\n%s", name, recovered, debug.Stack())
		}
	}()
	return fn()
}

func safeExportVoid(name string, fn func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logErrorf("%s panic recovered: %v\n%s", name, recovered, debug.Stack())
		}
	}()
	fn()
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return safeExportCall("register", output.FLB_ERROR, func() int {
		logInfof("register called; requires fluent-bit with FLBPluginFlushCtx support")
		return output.FLBPluginRegister(def, "clickhouse", "clickhouse output instances.")
	})
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	return safeExportCall("init", output.FLB_ERROR, func() int {
		instance := pluginOutputInstance(plugin)

		getConfig := func(key string, defaults ...string) string {
			value := output.FLBPluginConfigKey(plugin, key)
			if len(value) > 0 {
				return value
			}
			if len(defaults) > 0 {
				return defaults[0]
			}
			return ""
		}

		p, err := NewPlugin(getConfig)
		if err != nil {
			logPluginfForInstance(instance, pluginLogLevelError, "ERROR", "init plugin failed: %v", err)
			return output.FLB_ERROR
		}
		p.logInstance = instance

		if err := p.Init(); err != nil {
			logPluginfForInstance(instance, pluginLogLevelError, "ERROR", "init plugin failed: %v", err)
			return output.FLB_ERROR
		}

		release, err := setPluginContext(plugin, p)
		if err != nil {
			logPluginfForInstance(instance, pluginLogLevelError, "ERROR", "init plugin failed: %v", err)
			p.Exit()
			return output.FLB_ERROR
		}
		p.contextRelease = release
		return output.FLB_OK
	})
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	return safeExportCall("flush", output.FLB_ERROR, func() int {
		logWarnf("non-Ctx flush called; ctx-aware fluent-bit is required")
		return output.FLB_ERROR
	})
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctxPtr, data unsafe.Pointer, length C.int, tag *C.char) int {
	return safeExportCall("flush_ctx", output.FLB_ERROR, func() int {
		p, ok := getPluginContext(ctxPtr)
		if !ok {
			logErrorf("flush failed: context not found")
			return output.FLB_ERROR
		}
		if length < 0 {
			p.logErrorf("flush failed: invalid payload length=%d", int(length))
			return output.FLB_ERROR
		}
		if data == nil && length > 0 {
			p.logErrorf("flush failed: payload pointer is nil with length=%d", int(length))
			return output.FLB_ERROR
		}
		if payloadTooLarge(length) {
			p.logErrorf("flush failed: payload too large length=%d limit=%d", int(length), maxFlushPayloadBytes)
			p.observeDroppedRows(output.FLB_ERROR, "payload_limit", 1)
			return output.FLB_ERROR
		}
		tagValue := ""
		if tag != nil {
			tagValue = C.GoString(tag)
		}
		payload := C.GoBytes(data, length)
		return p.BatchInsertPayload(tagValue, payload)
	})
}

//export FLBPluginExit
func FLBPluginExit() int {
	return safeExportCall("exit", output.FLB_ERROR, func() int {
		logWarnf("exit called without ctx; draining all registered instances")
		ctxRegistry.Range(func(key, value any) bool {
			p, ok := value.(*ClickHousePlugin)
			if !ok || p == nil {
				ctxRegistry.Delete(key)
				return true
			}
			p.logWarnf("exit called without ctx; draining instance")
			p.Exit()
			p.releaseContextOnce()
			return true
		})
		return output.FLB_OK
	})
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctxPtr unsafe.Pointer) int {
	return safeExportCall("exit_ctx", output.FLB_ERROR, func() int {
		p, ok := getPluginContext(ctxPtr)
		if !ok {
			logErrorf("exit failed: context not found")
			return output.FLB_ERROR
		}
		p.Exit()
		p.releaseContextOnce()
		return output.FLB_OK
	})
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	safeExportVoid("unregister", func() {
		logInfof("unregister called")
		output.FLBPluginUnregister(def)
	})
}

func main() {
}

func pluginFromContext(ctxPtr unsafe.Pointer) (*ClickHousePlugin, error) {
	if ctxPtr == nil {
		return nil, fmt.Errorf("plugin context pointer is nil")
	}
	p, ok := getPluginContext(ctxPtr)
	if !ok {
		return nil, fmt.Errorf("plugin context is nil")
	}
	return p, nil
}

func pluginFromContextValue(value any) (*ClickHousePlugin, error) {
	if value == nil {
		return nil, fmt.Errorf("plugin context is nil")
	}
	p, ok := value.(*ClickHousePlugin)
	if !ok {
		return nil, fmt.Errorf("plugin context type %T is invalid", value)
	}
	return p, nil
}
