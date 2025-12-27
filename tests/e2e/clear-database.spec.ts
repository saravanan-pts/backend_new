import { test, expect } from '@playwright/test';

const API_BASE_URL = process.env.PLAYWRIGHT_TEST_BASE_URL || 'http://localhost:3111';

test.describe('Database Cleanup', () => {
  test('should clear all data from SurrealDB', async ({ request }) => {
    // Call the clear API endpoint
    const response = await request.delete(`${API_BASE_URL}/api/clear`);

    // Check that the request was successful
    expect(response.status()).toBe(200);

    const result = await response.json();

    // Verify response structure
    expect(result).toHaveProperty('success', true);
    expect(result).toHaveProperty('message');
    expect(result).toHaveProperty('deleted');
    expect(result.deleted).toHaveProperty('entitiesDeleted');
    expect(result.deleted).toHaveProperty('relationshipsDeleted');
    expect(result.deleted).toHaveProperty('documentsDeleted');

    // Log the result for visibility
    console.log('Database cleared successfully:');
    console.log(`  - Entities deleted: ${result.deleted.entitiesDeleted}`);
    console.log(`  - Relationships deleted: ${result.deleted.relationshipsDeleted}`);
    console.log(`  - Documents deleted: ${result.deleted.documentsDeleted}`);

    // Verify all counts are numbers (they should be >= 0)
    expect(typeof result.deleted.entitiesDeleted).toBe('number');
    expect(typeof result.deleted.relationshipsDeleted).toBe('number');
    expect(typeof result.deleted.documentsDeleted).toBe('number');
  });

  test('should clear all data using POST method (alternative)', async ({ request }) => {
    // Call the clear API endpoint using POST method
    const response = await request.post(`${API_BASE_URL}/api/clear`);

    // Check that the request was successful
    expect(response.status()).toBe(200);

    const result = await response.json();

    // Verify response structure
    expect(result).toHaveProperty('success', true);
    expect(result).toHaveProperty('message');
    expect(result).toHaveProperty('deleted');
  });

  test('should handle multiple clear operations', async ({ request }) => {
    // Clear once
    const response1 = await request.delete(`${API_BASE_URL}/api/clear`);
    expect(response1.status()).toBe(200);
    
    const result1 = await response1.json();
    expect(result1.success).toBe(true);

    // Clear again (should work even with empty database)
    const response2 = await request.delete(`${API_BASE_URL}/api/clear`);
    expect(response2.status()).toBe(200);
    
    const result2 = await response2.json();
    expect(result2.success).toBe(true);

    // Second clear should report 0 deletions
    expect(result2.deleted.entitiesDeleted).toBe(0);
    expect(result2.deleted.relationshipsDeleted).toBe(0);
    expect(result2.deleted.documentsDeleted).toBe(0);
  });
});

