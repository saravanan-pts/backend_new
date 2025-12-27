# Knowledge Graph POC

An interactive knowledge graph visualization application built with Next.js, Cytoscape.js, SurrealDB Cloud, and Azure OpenAI for intelligent entity and relationship extraction from various file formats.

## Features

- **Interactive Graph Visualization**: Powered by Cytoscape.js with multiple layout algorithms (Cola, Dagre, Circle, Grid)
- **Multi-format Document Processing**: Extract entities and relationships from text, PDF, CSV, and DOCX files
- **AI-Powered Extraction**: Uses Azure OpenAI (GPT-4) to intelligently identify entities and relationships
- **Graph Database Storage**: SurrealDB Cloud for efficient graph storage and querying
- **Real-time Updates**: Live graph updates as new data is processed
- **Entity Management**: Create, edit, delete entities and relationships
- **Search & Filter**: Search entities and filter graph by entity types
- **Export/Import**: Export graph data as JSON or PNG, import existing graphs

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Next.js Frontend                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Cytoscape  │  │  File Upload │  │  Node Detail │     │
│  │  Visualization│  │   Component  │  │    Panel     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│           │                 │                  │            │
│           └─────────────────┼──────────────────┘            │
│                             │                               │
│                    ┌────────▼────────┐                      │
│                    │  Zustand Store  │                      │
│                    └────────┬────────┘                      │
└─────────────────────────────┼───────────────────────────────┘
                              │
                    ┌─────────▼──────────┐
                    │  Next.js API Routes│
                    └─────────┬─────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
┌───────▼────────┐   ┌────────▼────────┐   ┌───────▼────────┐
│  SurrealDB     │   │  Azure OpenAI   │   │  Document      │
│  Cloud (WSS)   │   │  Service        │   │  Processors    │
└────────────────┘   └─────────────────┘   └────────────────┘
```

## Prerequisites

- Node.js 18+ and npm
- SurrealDB Cloud account and instance
- Azure OpenAI resource with GPT-4 deployment
- Environment variables configured (see below)

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd surrealdb-poc-v1
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Configure environment variables**
   ```bash
   cp .env.example .env.local
   ```
   
   Edit `.env.local` with your credentials:
   - SurrealDB Cloud WSS URL, namespace, database, and JWT token
   - Azure OpenAI endpoint, API key, and deployment name

4. **Run the development server**
   ```bash
   npm run dev
   ```

5. **Open your browser**
   Navigate to [http://localhost:3111](http://localhost:3111)

## Environment Variables

### SurrealDB Configuration
- `NEXT_PUBLIC_SURREALDB_URL`: WebSocket URL for SurrealDB Cloud instance
- `NEXT_PUBLIC_SURREALDB_NAMESPACE`: Namespace (e.g., "demo")
- `NEXT_PUBLIC_SURREALDB_DATABASE`: Database name (e.g., "surreal_deal_store")
- `SURREALDB_TOKEN`: JWT authentication token (server-side only)

### Azure OpenAI Configuration
- `AZURE_OPENAI_ENDPOINT`: Azure OpenAI resource endpoint
- `AZURE_OPENAI_API_KEY`: Azure OpenAI API key
- `AZURE_OPENAI_DEPLOYMENT_NAME`: Deployment name for GPT-4

### Application Configuration
- `NEXT_PUBLIC_APP_URL`: Application URL (default: http://localhost:3111)
- `MAX_FILE_SIZE_MB`: Maximum file size in MB (default: 10)
- `MAX_CHUNK_SIZE_TOKENS`: Maximum tokens per chunk for processing (default: 8000)

## Usage

### Uploading Files

1. Click on the **Upload** tab
2. Drag and drop files or click to select
3. Supported formats: TXT, PDF, CSV, DOCX (max 10MB)
4. Files are automatically processed and entities/relationships extracted

### Direct Text Input

1. Click on the **Input** tab
2. Paste or type text into the textarea
3. Click **Process Text**
4. Review extracted entities and relationships before committing

### Viewing Graph

- **Zoom**: Use mouse wheel or zoom controls
- **Pan**: Click and drag the background
- **Select Node**: Click on a node to view details
- **Filter**: Use the filter button to show/hide entity types
- **Search**: Use the search bar to find entities

### Managing Entities

- **View Details**: Click on a node to see properties and relationships
- **Edit Properties**: Click the edit icon in the details panel
- **Delete Entity**: Click delete button (removes connected relationships)
- **Add Relationship**: Use the details panel (coming soon)

### Export/Import

- **Export JSON**: Download graph data as JSON
- **Export PNG**: Export graph visualization as PNG
- **Import**: Load previously exported graph data

## Project Structure

```
/app
  /api
    /process          # File processing API route
  page.tsx            # Main dashboard
  layout.tsx          # Root layout
  globals.css         # Global styles
/components
  GraphVisualization.tsx  # Cytoscape.js graph component
  GraphControls.tsx       # Graph control toolbar
  NodeDetailPanel.tsx     # Entity details panel
  FileUpload.tsx          # File upload interface
  TextInput.tsx           # Text input component
  SettingsPanel.tsx       # Settings and configuration
  ErrorBoundary.tsx       # Error boundary component
/lib
  surrealdb-client.ts     # SurrealDB connection service
  schema.ts                # Graph schema definitions
  store.ts                 # Zustand state management
  test-data.ts             # Test data utilities
/services
  graph-operations.ts      # Graph CRUD operations
  azure-openai.ts          # Azure OpenAI integration
  document-processor.ts    # Document processing pipeline
/hooks
  useGraph.ts              # Graph operations hook
  useSurrealDB.ts          # Database connection hook
  useFileProcessor.ts      # File processing hook
  useEntityExtraction.ts   # Entity extraction hook
/types
  index.ts                 # TypeScript type definitions
```

## API Documentation

### POST `/api/process`

Process a file or text to extract entities and relationships.

**Request:**
- `file` (FormData): File to process (optional)
- `text` (FormData): Text to process (optional)

**Response:**
```json
{
  "success": true,
  "document": { ... },
  "entities": [ ... ],
  "relationships": [ ... ],
  "stats": {
    "entityCount": 10,
    "relationshipCount": 15
  }
}
```

### DELETE `/api/clear`

Clear all data from the SurrealDB database (entities, relationships, and documents).

**⚠️ Warning:** This permanently deletes all data from the database!

**Request:**
```bash
curl -X DELETE http://localhost:3111/api/clear
```

**Alternative using POST:**
```bash
curl -X POST http://localhost:3111/api/clear
```

**Response:**
```json
{
  "success": true,
  "message": "All data cleared successfully",
  "deleted": {
    "entitiesDeleted": 33,
    "relationshipsDeleted": 35,
    "documentsDeleted": 20
  }
}
```

## Entity Types

- **Person**: Individual people
- **Organization**: Companies, institutions, groups
- **Location**: Places, cities, countries
- **Concept**: Abstract ideas, topics
- **Event**: Occurrences, happenings
- **Technology**: Technologies, tools, systems

## Relationship Types

- **RELATED_TO**: General relationship
- **PART_OF**: Entity is part of another
- **WORKS_AT**: Person works at organization
- **LOCATED_IN**: Entity is located in a place
- **MENTIONS**: Entity mentions or references another
- **CREATED_BY**: Entity was created by another

## Troubleshooting

### Connection Issues

- **SurrealDB not connecting**: Check your WSS URL and JWT token
- **Azure OpenAI errors**: Verify endpoint, API key, and deployment name
- **Health check fails**: Ensure network connectivity and credentials

### Processing Issues

- **Large files timeout**: Increase `maxDuration` in API route or reduce file size
- **Extraction fails**: Check Azure OpenAI quota and deployment status
- **CSV parsing errors**: Ensure CSV has proper headers

### Graph Visualization

- **Graph not loading**: Check browser console for errors
- **Nodes not appearing**: Verify entities were created in database
- **Layout issues**: Try different layout algorithms in controls

## Development

### Running Tests

Test data utilities are available in `lib/test-data.ts`:

```typescript
import { populateTestData, clearTestData } from "@/lib/test-data";

// Populate with sample data
await populateTestData();

// Clear all data
await clearTestData();
```

### Building for Production

```bash
npm run build
npm start
```

## Future Enhancements

- [ ] Real-time collaborative editing
- [ ] Advanced graph algorithms (shortest path, centrality)
- [ ] Batch processing for multiple files
- [ ] Custom entity and relationship types
- [ ] Graph analytics and insights
- [ ] Export to other formats (GraphML, GEXF)
- [ ] Integration with more document sources
- [ ] Advanced search with filters
- [ ] Relationship strength visualization
- [ ] Timeline view for temporal relationships

## License

[Your License Here]

## Contributing

[Contributing Guidelines Here]
