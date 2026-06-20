package fortify

import "context"

// Execute runs fn through the supplied Composer and returns the typed result.
//
// It is the free-function spelling of policy.Execute shown in the spec:
//
//	result, err := fortify.Execute[MyOutput](ctx, policy, func(ctx context.Context) (MyOutput, error) {
//	    return llmClient.Complete(ctx, prompt)
//	})
//
// Because the Go Composer is already generic over its result type T, this is a
// thin convenience wrapper over (*Composer[T]).Execute that lets call sites
// read as fortify.Execute[T](...) when that style is preferred. The two forms
// are equivalent; pick whichever reads better at the call site.
func Execute[T any](ctx context.Context, policy *Composer[T], fn func(context.Context) (T, error)) (T, error) {
	return policy.Execute(ctx, fn)
}
