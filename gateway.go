package berserk

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// The gateway is the authenticated public edge. It nests the native
// /<package>.<Service>/<Method> gRPC surface under a path prefix
// (DefaultGRPCPathPrefix) and strips that prefix before matching, so a
// client reaching Berserk through the gateway must prepend it to every
// method it invokes. Identity is resolved by the gateway from the bearer
// token; clients send no identity headers.

func prefixedMethod(prefix, method string) string {
	if prefix == "" || strings.HasPrefix(method, prefix+"/") {
		return method
	}
	return prefix + method
}

func withBearer(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// gatewayStreamInterceptor routes a streaming RPC through the gateway: it
// rewrites the method path under the gateway prefix and attaches the
// bearer token the gateway resolves to an identity.
func gatewayStreamInterceptor(token, prefix string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(withBearer(ctx, token), desc, cc, prefixedMethod(prefix, method), opts...)
	}
}

// gatewayUnaryInterceptor is the unary counterpart of
// gatewayStreamInterceptor.
func gatewayUnaryInterceptor(token, prefix string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(withBearer(ctx, token), prefixedMethod(prefix, method), req, reply, cc, opts...)
	}
}
