# Development Container Setup

This repository includes a devcontainer configuration for consistent development environments.

## Using the devcontainer

### VS Code

1. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Open the repository in VS Code
3. Click "Reopen in Container" when prompted (or use Command Palette: "Dev Containers: Reopen in Container")

The container will automatically:
- Install Node.js 20
- Install npm dependencies from `next-app/`
- Configure ESLint and Prettier
- Set up auto-formatting on save

### Other IDEs

The devcontainer uses the standard [Development Containers specification](https://containers.dev/), so it should work with any compatible IDE or tool.

## Features

- **Node.js 20**: Matches the version used in CI/CD
- **Auto-install dependencies**: Runs `npm install` in the `next-app/` directory on container creation
- **Port forwarding**: Port 3000 is automatically forwarded for the Next.js dev server
- **VS Code extensions**: Pre-configured with ESLint, Prettier, and Tailwind CSS extensions
- **Auto-formatting**: Code is formatted on save with ESLint fixes applied automatically

## Manual Setup (without devcontainer)

If you prefer not to use the devcontainer:

1. Ensure you have Node.js 20.x installed
2. Navigate to the `next-app/` directory
3. Run `npm install` to install dependencies
4. The pre-commit hook will be automatically available (it's tracked in `.git/hooks/`)
