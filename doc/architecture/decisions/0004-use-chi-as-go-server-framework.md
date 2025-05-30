# 4. use chi as go server framework

Date: 2025-05-07

## Status

Accepted

## Context

For the new VSA control plane that is being developed using Go, which will handle OpenAPI-generated code, several server frameworks were considered. Each framework offers different features and is suited for various use cases. The primary options evaluated were:

1. **net/http (Go standard library)**
   - **Features**: Built-in, lightweight, no extra dependencies, high performance with minimal abstraction.
   - **Best For**: Performance-focused applications, projects using go-ogen.

2. **chi (Lightweight router)**
   - **Features**: Minimalistic and efficient, middleware support (CORS, logging, authentication, etc.), compatible with oapi-codegen and go-ogen.
   - **Best For**: Microservices and APIs needing structured middleware support.

3. **echo (Feature-rich framework)**
   - **Features**: High performance with middleware support, automatic JSON binding and validation, works well with oapi-codegen and openapi-generator.
   - **Best For**: API-heavy applications requiring built-in request parsing, teams familiar with echo.

4. **gin (Fast and popular)**
   - **Features**: High-performance HTTP router, middleware support with easy JSON handling, compatible with oapi-codegen and openapi-generator.
   - **Best For**: High-performance REST APIs, applications needing structured middleware.

## Decision
We chose `chi` as the Go server framework for the new control plane due to the following reasons:

1. **Well-Optimized Router Engine**: `chi` is designed to be a minimalistic and efficient router, making it an excellent choice for the control plane, where performance and simplicity are crucial.

2. **Efficient Handling of Context and Parameters**: `chi` provides efficient handling of context and parameters, which is essential for building a scalable and maintainable control plane.

3. **Better Support for Middlewares**: `chi` offers robust middleware support, allowing for easy integration of essential features such as CORS, logging, authentication, and more. This makes it highly suitable for structured middleware support in the control plane.

## Consequences
- **Positive**:
  - Improved performance and efficiency due to `chi`'s optimized router engine.
  - Enhanced maintainability and scalability through efficient context and parameter handling.
  - Simplified integration of middleware, resulting in cleaner and more modular code.

- **Negative**:
  - Requires developers to be familiar with the `chi` framework and its middleware ecosystem.
  - May involve a learning curve for teams accustomed to other frameworks like `echo` or `gin`.

## Alternatives Considered
- **net/http**: While highly performant and lightweight, it requires manual implementation of middleware, which can be cumbersome for larger projects.
- **echo**: Although feature-rich and suitable for API-heavy applications, it may introduce unnecessary complexity for the control plane.
- **gin**: Popular and high-performance, but `chi` was preferred for its minimalistic approach and efficient middleware support.
