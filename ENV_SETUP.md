# Environment Variables Setup Guide

## Required Environment Variables

Create a `.env.local` file in the root directory with the following variables:

### SurrealDB Configuration

```bash
# SurrealDB Cloud WebSocket URL
NEXT_PUBLIC_SURREALDB_URL=wss://your-instance.surreal.cloud

# SurrealDB Namespace
NEXT_PUBLIC_SURREALDB_NAMESPACE=demo

# SurrealDB Database Name
NEXT_PUBLIC_SURREALDB_DATABASE=surreal_deal_store

# SurrealDB JWT Token (server-side only, keep secret!)
SURREALDB_TOKEN=your-jwt-token-here
```

**How to get SurrealDB credentials:**
1. Sign up for SurrealDB Cloud at https://surrealdb.com/cloud
2. Create a new instance
3. Copy the WebSocket URL (wss://...)
4. Generate a JWT token in the SurrealDB Cloud dashboard
5. Use the namespace and database names from your instance

### Azure OpenAI Configuration

```bash
# Azure OpenAI Endpoint
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/

# Azure OpenAI API Key
AZURE_OPENAI_API_KEY=your-api-key-here

# Azure OpenAI Deployment Name (e.g., gpt-4)
AZURE_OPENAI_DEPLOYMENT_NAME=your-deployment-name
```

**How to get Azure OpenAI credentials:**
1. Create an Azure OpenAI resource in Azure Portal
2. Deploy a GPT-4 model
3. Copy the endpoint URL
4. Get the API key from the Azure Portal
5. Note your deployment name

### Application Configuration (Optional)

```bash
# Application URL (default: http://localhost:3000)
NEXT_PUBLIC_APP_URL=http://localhost:3000

# Maximum file size in MB (default: 10)
MAX_FILE_SIZE_MB=10

# Maximum tokens per chunk for processing (default: 8000)
MAX_CHUNK_SIZE_TOKENS=8000
```

## Setup Steps

1. **Copy the example file:**
   ```bash
   cp .env.example .env.local
   ```

2. **Edit `.env.local` with your actual credentials**

3. **Never commit `.env.local` to version control** (it's already in `.gitignore`)

4. **Restart the development server** after making changes:
   ```bash
   npm run dev
   ```

## Verification

After setting up environment variables, you can verify the connection:

1. Start the application: `npm run dev`
2. Open http://localhost:3000
3. Check the connection status indicator in the header (green = connected, red = disconnected)
4. Go to Settings tab to see connection details

## Troubleshooting

### SurrealDB Connection Issues
- Verify the WebSocket URL starts with `wss://`
- Check that the JWT token is valid and not expired
- Ensure the namespace and database names are correct
- Check network connectivity to SurrealDB Cloud

### Azure OpenAI Issues
- Verify the endpoint URL is correct
- Check that the API key is valid
- Ensure the deployment name matches your Azure deployment
- Verify you have quota available for the model

### File Processing Issues
- Check that file size is within limits (default 10MB)
- Verify Azure OpenAI credentials are correct
- Check browser console for detailed error messages

