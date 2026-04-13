# Changelog

## v1.0.0 — Initial Release

### Connection Management
- Manual and URI-based MongoDB connection configuration (mongodb:// and mongodb+srv://)
- SCRAM authentication with AES-256-GCM encrypted password storage
- Multiple concurrent connections with live status indicators
- Connection test and validation before saving
- Duplicate, edit, and delete operations on saved connections

### Database & Collection Browser
- Expandable connection tree with lazy-loaded database and collection hierarchy
- Right-click context menus for creating and dropping databases and collections
- Database and collection statistics display (dbStats / collStats)

### Query Editor
- Find tab with filter, projection, sort, skip, limit, and maxTimeMs inputs
- Aggregation tab with 24-stage pipeline builder, operator picker, per-stage enable/disable, reordering, run-to-here, and 10 sample pipelines
- Keyboard shortcut (Cmd/Ctrl+Enter) for query execution

### Results Management
- Three view modes: Table (SQL-style), Tree (expandable documents), JSON (syntax-highlighted)
- Smart pagination with hasMore detection
- Document operations: insert, edit (with JSON validation and formatting), delete
- JSON export functionality
- Per-connection query history with one-click reload

### Index Management
- List, create, and drop indexes
- Full index options: unique, sparse, TTL, partial filters, background builds

### User Management
- List users per database with roles and authentication mechanisms
- Create users with role checkboxes or custom JSON
- Change passwords, grant/revoke roles, drop users

### UX
- Welcome screen with connection cards and quick actions
- Native menu bar with keyboard shortcuts
- Live connection log with timestamps
- Native macOS app icon
