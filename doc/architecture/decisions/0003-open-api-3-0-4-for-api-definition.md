# 3. open API 3.0.4 for API definition

Date: 2025-05-05

## Status

Accepted

## Context

As we develop and maintain our APIs for the new control plane, we need a specification format that is both robust and future-proof. Our options are Swagger 2.0 and OpenAPI 3.x. This decision record evaluates the key differences between Swagger 2.0 and OpenAPI 3.x and provides a rationale for our choice. After evaluating the available versions of OpenAPI 3.x, we have decided to adopt OpenAPI 3.0.4.

## Comparison

### Callback Support
- **Swagger 2.0:** Does not support callback definitions.
- **OpenAPI 3.x:** Supports callback definitions, allowing for more complex and interactive API designs.

### Content Type Support
- **Swagger 2.0:** Limited support for content types per endpoint.
- **OpenAPI 3.x:** Supports multiple content types per endpoint, providing greater flexibility in handling different data formats.

### Future Compatibility
- **Swagger 2.0:** No longer actively developed, which limits its future compatibility and support.
- **OpenAPI 3.x:** Actively maintained and aligns with modern API standards, ensuring long-term support and compatibility.

### Request Body Handling
- **Swagger 2.0:** Request bodies are part of parameters, which can lead to less structured and harder-to-maintain definitions.
- **OpenAPI 3.x:** Uses a dedicated `requestBody` component for better structure and clarity in request definitions.

### Schema Organization
- **Swagger 2.0:** Uses separate definitions for parameters and responses, which can lead to redundancy and complexity.
- **OpenAPI 3.x:** Unifies schema definitions under `components`, promoting better reusability and organization.

### Security Definitions
- **Swagger 2.0:** Uses `securityDefinitions` for defining security schemes.
- **OpenAPI 3.x:** Introduces `securitySchemes` with enhancements for OAuth 2.0, improving security definition capabilities.

### Tooling Support in Go
- **Swagger 2.0:** Mature tooling support (e.g., go-swagger) with full server generation capabilities.
- **OpenAPI 3.x:** Tooling support is still developing (e.g., oapi-codegen) and may require manual implementation.

## Decision

We have decided to adopt OpenAPI 3.0.4 for our API specifications. The primary reasons for this decision are:

1. **Future-Proofing:** OpenAPI 3.x is actively maintained and aligns with modern API standards, ensuring our API specifications remain relevant and supported in the long term.
2. **Enhanced Features:** The support for callback definitions (though we do not use this currently), multiple content types per endpoint, and improved security schemes provide greater flexibility and robustness in our API design.
3. **Better Structure and Reusability:** The dedicated `requestBody` component and unified schema organization under `components` lead to more structured and maintainable API definitions.
4. **Improved Security:** Enhanced security schemes with better support for OAuth 2.0 offer more robust security definitions.
5. **Better Code Generation Support:** OpenAPI 3.0.4 offers better code generation support, which is essential for our development process.

While the tooling support for OpenAPI 3.x in Go is still evolving, we believe the long-term benefits of adopting OpenAPI 3.x outweigh the current limitations. We will monitor the development of tooling support and contribute to its improvement where possible.

### Selected OpenAPI Version: 3.0.4

After careful consideration, we have decided to adopt OpenAPI 3.0.4 for our API specifications. The primary reasons for this decision are:

1. **Better Code Generation Support:** OpenAPI 3.0.4 has better code generation support compared to OpenAPI 3.1, which is crucial for our development workflow and automation.
2. **Minimal Feature Loss:** The only features we lose in OpenAPI 3.0.4 compared to OpenAPI 3.1 are type arrays and reusable examples. These features are not critical for our current API needs and can be worked around if necessary.
3. **Active Maintenance and Modern Practices:** OpenAPI 3.x, including version 3.0.4, is actively maintained and aligns with modern API standards and practices, ensuring our API specifications remain relevant and supported in the long term.

## Consequences

- Developers will need to familiarize themselves with the new specification format and its features.
- We may encounter some initial challenges with tooling support, but we expect this to improve over time.

