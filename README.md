
```
fastapi_project/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   │
│   ├── api/
│   │   ├── __init__.py
│   │   ├── process.py
│   │   ├── health.py
│   │   └── clear.py
│   │
│   ├── db/
│   │   ├── __init__.py
│   │   └── cosmos_client.py
│   │
│   ├── repositories/
│   │   ├── __init__.py
│   │   └── graph_repository.py
│   │
│   ├── services/
│   │   ├── __init__.py
│   │   ├── document_processor.py
│   │   ├── openai_extractor.py
│   │   └── graph_service.py
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── chunking.py
│   │   └── json_sanitizer.py
│   │
│   ├── schemas/
│   │   └── (optional future pydantic schemas)
│   │
│   └── entities/
│       └── (optional future domain models)
│
├── tests/
│   └── (optional pytest files)
│
├── .env
├── requirements.txt
├── README.md
└── run.sh (optional helper script)
```

---

## **Short explanation for README**

You can add this section under the structure:

### **Key Directories**

| Directory          | Purpose                                           |
| ------------------ | ------------------------------------------------- |
| `app/api`          | FastAPI HTTP endpoints                            |
| `app/services`     | Business logic (LLM extraction, graph operations) |
| `app/repositories` | Gremlin CRUD operations                           |
| `app/db`           | CosmosDB Gremlin client                           |
| `app/utils`        | Helpers such as chunking + JSON cleanup           |
| `app/config.py`    | Centralized environment config                    |
| `.env`             | Azure + Cosmos credentials                        |
| `requirements.txt` | Python dependencies                               |

---

## **Summary of Data Flow**

Add this diagram to README:

```
            [CSV/TXT Upload]
                     ↓
         FastAPI → process.py
                     ↓
           DocumentProcessor
     (chunk → LLM extract → normalize)
                     ↓
         GraphService (business rules)
                     ↓
       GraphRepository (Gremlin CRUD)
                     ↓
     Azure Cosmos DB Gremlin Graph
```
