//
// registry.go
//
package workerpool

import (
    "fmt"
    "sync"
)


var (
    defaultRegistry = NewRegistry()
)


type Registry struct {
    m sync.Map
}


//
// Get the default registry.
//
// Version:
//   - 2026-02-03: Created new.
//
func DefaultRegistry() *Registry {
    return defaultRegistry
}


//
// Get a typed *Pool[T] by name.
//
// Version:
//   - 2026-02-03: Created new.
//
func GetPool[T any](r *Registry, name string) (*Pool[T], bool) {
    if r == nil || name == "" {
        return nil, false
    }
    v, ok := r.m.Load(name)
    if !ok {
        return nil, false
    }
    p, ok := v.(*Pool[T])
    return p, ok
}


//
// NewRegistry creates a new Registry.
//
// Version:
//   - 2026-02-03: Created new.
//
func NewRegistry() *Registry {
    return &Registry{}
}


//
// Registers an instance with a unique name.
//
// Version:
//   - 2026-02-03: Created new.
//
func (r *Registry) Register(name string, v any) error {
    // Guard.
    if r == nil {
        return fmt.Errorf("failed to register: registry=null")
    }
    if name == "" {
        return fmt.Errorf("failed to register: name=null")
    }
    if v == nil {
        return fmt.Errorf("failed to register: value=null")
    }

    _, loaded := r.m.LoadOrStore(name, v)
    if loaded {
        return fmt.Errorf("failed to register because the name is already registered: name=%q", name)
    }
    return nil
}


//
// Deletes a registered instance by name.
//
// Version:
//   - 2026-02-03: Created new.
//
func (r *Registry) Delete(name string) {
    if r == nil || name == "" {
        return
    }
    r.m.Delete(name)
}




