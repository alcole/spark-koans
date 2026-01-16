# Contributing to PySpark Koans

Thank you for your interest in contributing to PySpark Koans!

## Development Setup

### Option 1: Using DevContainer (Recommended)

The easiest way to get started is using the DevContainer configuration:

1. Install [VS Code](https://code.visualstudio.com/) and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Open this repository in VS Code
3. Click "Reopen in Container" when prompted
4. Wait for the container to build and dependencies to install

Everything will be set up automatically, including:
- Node.js 20
- All npm dependencies
- ESLint and code formatting
- Pre-commit hooks

### Option 2: Manual Setup

If you prefer a local setup:

1. Install Node.js 20.x
2. Navigate to the `next-app/` directory:
   ```bash
   cd next-app
   ```
3. Install dependencies:
   ```bash
   npm install
   ```

## Pre-commit Hook

A pre-commit hook is configured to automatically run ESLint on staged files before each commit. This helps maintain code quality and catch issues early.

The hook will:
- Run ESLint on all staged `.js` and `.jsx` files
- Automatically fix issues where possible
- Re-stage fixed files
- Prevent commits if there are unfixable linting errors

To manually run linting:
```bash
cd next-app
npm run lint
```

## Running the Development Server

```bash
cd next-app
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

## Project Structure

See [CLAUDE.md](./CLAUDE.md) for detailed information about the project architecture and structure.

## Adding New Koans

See the "Adding New Koans" section in [CLAUDE.md](./CLAUDE.md).

## Running Tests

Currently, the project uses ESLint for code quality. Future contributions might include:
- Unit tests for React components
- Integration tests for the PySpark shim
- E2E tests for koan completion flows

## Pull Request Process

1. Create a new branch from `main` for your changes
2. Make your changes and ensure they pass linting (`npm run lint`)
3. Commit your changes (the pre-commit hook will run automatically)
4. Push your branch and create a pull request
5. Wait for CI checks to pass
6. Request review from maintainers

## Code Style

- This project uses ESLint with Next.js defaults
- Code is automatically formatted on save in the DevContainer
- Follow existing patterns in the codebase
- Keep functions focused and avoid over-engineering
