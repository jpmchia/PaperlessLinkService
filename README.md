# Paperless Link Service

A Go microservice for Paperless-ngx integration features, including custom field values aggregation and querying.

## Features

- Aggregates unique values from custom fields across all documents
- Supports dynamic list fields (Named Entities, Topics, etc.)
- Provides value counts for filter display
- Search functionality for finding values
- Supports PostgreSQL, MySQL/MariaDB, and SQLite databases

## API Endpoints

### GET `/api/custom-field-values/{fieldId}/`

Get all unique values for a specific custom field.

**Response:**
```json
{
  "field_id": 123,
  "field_name": "Topics",
  "values": [
    {
      "id": "val-12345",
      "label": "Finance",
      "count": 45
    },
    {
      "id": "val-67890",
      "label": "Legal",
      "count": 32
    }
  ],
  "total_documents": 500
}
```

### GET `/api/custom-field-values/{fieldId}/search/?q={query}`

Search for values matching a query string.

**Query Parameters:**
- `q` (required): Search query string

**Response:**
```json
[
  {
    "id": "val-12345",
    "label": "Finance",
    "count": 45
  }
]
```

### POST `/api/custom-field-values/{fieldId}/counts/`

Get value counts with optional filter rules applied.

**Request Body:**
```json
{
  "filter_rules": []
}
```

**Response:**
```json
[
  {
    "id": "val-12345",
    "label": "Finance",
    "count": 45
  }
]
```

### GET `/health`

Health check endpoint.

## Configuration

Copy `.env.example` to `.env` and configure:

```env
PORT=8080
DB_ENGINE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=paperless
DB_USER=paperless
DB_PASS=paperless
DB_SSL_MODE=prefer
```

For SQLite:
```env
DB_ENGINE=sqlite
DB_PATH=/path/to/db.sqlite3
```

## Building

```bash
go build -o custom-field-values-service
```

## Running

```bash
./custom-field-values-service
```

Or with environment variables:
```bash
PORT=8080 DB_HOST=localhost DB_NAME=paperless ./custom-field-values-service
```

## Docker

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o custom-field-values-service

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/custom-field-values-service .
CMD ["./custom-field-values-service"]
```

## Database Schema

The service expects the following tables:
- `documents_customfield` - Custom field definitions
- `documents_customfieldinstance` - Custom field values per document
- `documents_document` - Documents table

## Notes

- Values are aggregated from all non-deleted custom field instances
- Comma and colon separated values are parsed and counted individually
- Value IDs are generated using a simple hash function
- The service handles different data types (text, url, date, boolean, etc.)

