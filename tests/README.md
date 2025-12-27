# E2E Testing with Playwright

## Overview

This directory contains end-to-end tests using Playwright that test the Knowledge Graph POC application from a user's perspective.

## Test Structure

```
tests/
└── e2e/
    ├── knowledge-graph.spec.ts          # Basic UI and functionality tests
    ├── knowledge-graph-full-flow.spec.ts # Complete user workflow test
    └── screenshots/                      # Test screenshots (auto-generated)
```

## Running Tests

### Run all tests
```bash
npm run test:e2e
```

### Run tests with UI mode (interactive)
```bash
npm run test:e2e:ui
```

### Run tests in headed mode (see browser)
```bash
npm run test:e2e:headed
```

### Run tests in debug mode
```bash
npm run test:e2e:debug
```

## Test Data

Tests use the data from `data/data.txt` which contains:
- Orion Logistics company information
- Shipment and customer entities
- Distribution centers and carriers
- Relationships between entities

## Test Scenarios

### Basic Tests (`knowledge-graph.spec.ts`)
- Application loading and UI elements
- Tab navigation
- Text input and processing
- Graph visualization
- Settings panel
- Graph controls

### Full Workflow Test (`knowledge-graph-full-flow.spec.ts`)
- Complete user journey:
  1. Open application
  2. Process test data
  3. Verify extraction
  4. Check graph visualization
  5. Interact with controls

## Prerequisites

1. **Environment Variables:** Ensure `.env.local` is configured with:
   - SurrealDB credentials
   - Azure OpenAI credentials

2. **Application Running:** Tests automatically start the dev server, but you can also run it manually:
   ```bash
   npm run dev
   ```

3. **Playwright Browsers:** Installed automatically on first run

## Test Execution

Tests will:
1. Start the Next.js dev server automatically
2. Open browser and navigate to the application
3. Execute test scenarios
4. Take screenshots on failures
5. Generate HTML report

## Viewing Results

After tests complete:
```bash
# View HTML report
npx playwright show-report
```

## Writing New Tests

Follow the existing test structure:

```typescript
import { test, expect } from '@playwright/test';

test('my test', async ({ page }) => {
  await page.goto('/');
  // Your test code
});
```

## Troubleshooting

### Tests fail to start
- Check if port 3000 is available
- Verify environment variables are set
- Check application builds successfully

### Tests timeout
- Increase timeout in test if processing takes long
- Check Azure OpenAI API is responding
- Verify SurrealDB connection

### Browser not found
```bash
npx playwright install
```

