const { Surreal } = require('surrealdb.js');
require('dotenv').config({ path: '.env.local' });

async function checkData() {
  const db = new Surreal();
  
  try {
    const url = process.env.NEXT_PUBLIC_SURREALDB_URL;
    const namespace = process.env.NEXT_PUBLIC_SURREALDB_NAMESPACE;
    const database = process.env.NEXT_PUBLIC_SURREALDB_DATABASE;
    const username = process.env.NEXT_PUBLIC_SURREALDB_USERNAME;
    const password = process.env.NEXT_PUBLIC_SURREALDB_PASSWORD;
    
    console.log('Connecting to SurrealDB...');
    await db.connect(url);
    
    if (username && password) {
      await db.signin({ user: username, pass: password });
    }
    
    await db.use({ ns: namespace, db: database });
    
    console.log('\n=== Checking for existing data ===\n');
    
    // Check documents
    const documents = await db.query('SELECT * FROM document');
    console.log(`Documents: ${Array.isArray(documents[0]?.result) ? documents[0].result.length : 0}`);
    if (documents[0]?.result?.length > 0) {
      documents[0].result.forEach((doc, i) => {
        console.log(`  ${i + 1}. ${doc.filename || doc.id} (${doc.entityCount || 0} entities, ${doc.relationshipCount || 0} relationships)`);
      });
    }
    
    // Check entities
    const entities = await db.query('SELECT * FROM entity');
    console.log(`\nEntities: ${Array.isArray(entities[0]?.result) ? entities[0].result.length : 0}`);
    
    // Check relationships
    const relationships = await db.query('SELECT * FROM relationship');
    console.log(`Relationships: ${Array.isArray(relationships[0]?.result) ? relationships[0].result.length : 0}`);
    
    await db.close();
    console.log('\n✅ Check complete!');
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

checkData();
