# Scripts Directory

This directory contains utility scripts for the Knowledge Graph POC project.

## Available Scripts

### master.sh

Master startup script that handles all pre-flight checks and starts the application.

**Usage:**
```bash
# Start the application
./scripts/master.sh

# Run checks only (don't start server)
./scripts/master.sh --check

# Build then start
./scripts/master.sh --build

# Start on different port
./scripts/master.sh --port 3001

# Show help
./scripts/master.sh --help
```

**What it does:**
1. ✅ Checks Node.js version (requires 18+)
2. ✅ Checks npm installation
3. ✅ Verifies .env.local file exists
4. ✅ Checks if dependencies are installed
5. ✅ Ensures port 3000 is available
6. ✅ Starts the development server

**Features:**
- Automatic dependency installation if needed
- Port conflict detection and resolution
- Environment file validation
- Colored output for better readability
- Error handling with clear messages

## Example Usage

### Basic Startup
```bash
./scripts/master.sh
```

### Check Everything First
```bash
./scripts/master.sh --check
```

### Build Then Start
```bash
./scripts/master.sh --build
```

## Troubleshooting

### Permission Denied
```bash
chmod +x scripts/master.sh
```

### Port Already in Use
The script will automatically try to free port 3000. If it fails:
```bash
# Manually kill process on port 3000
lsof -ti:3000 | xargs kill -9
```

### Missing Dependencies
The script will automatically run `npm install` if needed.

### Missing .env.local
The script will create .env.local from .env.example if it exists.

## Integration with Testing

After running E2E tests, you can immediately start the app:
```bash
# Tests complete
npm run test:e2e

# Start app for manual testing
./scripts/master.sh
```

The script ensures everything is ready before starting, so you can begin testing immediately.

