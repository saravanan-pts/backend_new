const { Surreal } = require('surrealdb.js');
const fs = require('fs');
const path = require('path');

// Load .env.local
const envPath = path.join(__dirname, '..', '.env.local');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  envContent.split('\n').forEach(line => {
    const match = line.match(/^([^=]+)=(.*)$/);
    if (match) {
      process.env[match[1].trim()] = match[2].trim();
    }
  });
}

async function listTables() {
  const db = new Surreal();
  try {
    const url = process.env.SURREALDB_URL || '';
    const user = process.env.SURREALDB_USER || 'root';
    const pass = process.env.SURREALDB_PASS || 'root';
    const ns = process.env.SURREALDB_NAMESPACE || 'test';
    const dbName = process.env.SURREALDB_DATABASE || 'test';

    console.log('Connecting to SurrealDB...');
    await db.connect(url);
    await db.signin({ user, pass });
    await db.use({ ns, db: dbName });
    
    console.log('\n=== Querying database info ===\n');
    
    // Query to get all tables
    const result = await db.query('INFO FOR DB');
    
    if (result && result[0] && result[0].result) {
      const dbInfo = result[0].result;
      
      console.log('Database Tables:');
      console.log('================');
      
      if (dbInfo.tables) {
        const tables = Object.keys(dbInfo.tables);
        console.log(`\nTotal tables: ${tables.length}\n`);
        
        tables.forEach((table, index) => {
          console.log(`${index + 1}. ${table}`);
        });
        
        console.log('\n=== Knowledge Graph Tables ===');
        console.log('\nThe knowledge graph data is stored in these 3 tables:');
        console.log('1. entity - Stores graph nodes (entities)');
        console.log('2. relationship - Stores graph edges (relationships between entities)');
        console.log('3. document - Stores uploaded documents and metadata');
        
        if (tables.includes('entity')) {
          console.log('\n✅ entity table exists');
        }
        if (tables.includes('relationship')) {
          console.log('✅ relationship table exists');
        }
        if (tables.includes('document')) {
          console.log('✅ document table exists');
        }
      } else {
        console.log('No tables found in database info');
        console.log('Full result:', JSON.stringify(result, null, 2));
      }
    } else {
      console.log('Unexpected result format:', JSON.stringify(result, null, 2));
    }
  } catch (error) {
    console.error('Error:', error.message);
    console.error(error.stack);
  } finally {
    await db.close();
  }
}

listTables();

