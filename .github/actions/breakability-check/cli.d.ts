#!/usr/bin/env node
/**
 * CLI for local breakability analysis.
 *
 * Usage:
 *   npx ts-node src/cli.ts --package axios --from 0.21.1 --to 0.21.4 --repo .
 *   npx ts-node src/cli.ts --package express --from 4.18.2 --to 5.0.0
 *
 * Options:
 *   --package, -p    Package name (required)
 *   --from, -f       From version (required)
 *   --to, -t         To version (required)
 *   --repo, -r       Path to consuming repository (default: cwd)
 *   --ecosystem, -e  Ecosystem (default: npm)
 *   --json           Output raw JSON instead of formatted comment
 *   --help, -h       Show help
 */
export {};
