# CMS Backend

A FastAPI-based backend for the Digix CMS platform — managing devices, videos, shops, groups, content approvals, announcements, and real-time WebSocket communication.

## Overview

The backend provides all REST APIs consumed by the CMS frontend and Android devices. It handles:

- **Devices** — registration, online status, storage reporting, expiration, layout config
- **Videos & Advertisements** — S3 upload, metadata, rotation, fit mode, content type
- **Shops & Groups** — location and device-group management
- **Links** — associations between devices, videos, shops, and groups (`device_video_shop_group`)
- **Content Approval** — request/review workflow for linking content to groups
- **Announcements** — global and targeted announcements with scheduling and expiration
- **WebSocket** — real-time push for pending approvals badge and announcements
- **Background Tasks** — scheduled device expiration, offline detection, storage cleanup

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        CMS Backend (Port 8005)                    │
├──────────────────────────────────────────────────────────────────┤
│  device_video_shop_group.py  — core linking, layout, downloads   │
│  client_requirements_api.py  — content approval workflow         │
│  announcement_api.py         — announcements CRUD + scheduling   │
│  websocket_routes.py         — WebSocket endpoints               │
│  websocket_manager.py        — connection manager + broadcast    │
│  background_tasks.py         — scheduled jobs (expiry, offline)  │
│  company_expiration_api.py   — company/tenant expiration         │
├──────────────────────────────────────────────────────────────────┤
│                     PostgreSQL Database                           │
│                     AWS S3 (Video/Image Storage)                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Key Features

### Device Layout & Grid
- `GET/POST /device/{mobile_id}/layout` — save and restore grid layout config per device
- `layout_config` JSON stores slot assignments, rotations, and sequential playback settings
- `POST /group/{gname}/sync-to-devices` — push a layout to all devices in a group

### Sequential Playback (Single Mode)
When `layout_config` slot 1 has `play_all_sequential: true`, the downloads endpoint returns all videos in `sequential_videos` order instead of just the single slot video. Android plays them in sequence, looping forever.

### Content Approval Workflow
- Users submit content change requests (`link_content`, `video_assign`, `video_remove`)
- Admins approve or reject via `POST /content-changes/{id}/review`
- On approval, `device_video_shop_group` is synced so Recent Links updates immediately
- WebSocket pushes updated pending count to all connected admins

### Real-time WebSocket
- `WS /ws/{tenant_id}` — persistent connection per tenant
- Broadcasts: `pending_approvals` count, `announcement` events

### Android Device Downloads
- `GET /device/{mobile_id}/videos/downloads` — returns presigned S3 URLs for all assigned content, ordered by `grid_position` from `layout_config`
- Supports single, sequential, and all grid layout modes (`split_h`, `split_v`, `grid_3`, `grid_4`, `grid_1x4`)

---

## API Reference (Main Endpoints)

### Links
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/link` | POST | Create device-video-shop-group link |
| `/links` | GET | List all links |
| `/link/{id}/settings` | PUT | Update grid_position, rotation, resolution |
| `/link/device-to-group` | POST | Link device to group (inherits all videos) |

### Device
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/device/{mobile_id}/layout` | GET/POST | Get/save grid layout config |
| `/device/{mobile_id}/videos/downloads` | GET | Presigned URLs for device content |
| `/device/{mobile_id}/resolution` | GET/POST | Device screen resolution |
| `/device/{mobile_id}/online` | POST | Update online status |
| `/device/{mobile_id}/storage` | POST | Report storage usage |

### Content Approval
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/content-changes` | POST | Submit a content change request |
| `/content-changes` | GET | List requests (filter by status) |
| `/content-changes/{id}/review` | POST | Approve or reject a request |
| `/company/approval-settings` | GET/PUT | Toggle approval requirement |

### Announcements
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/announcements` | GET/POST | List / create announcements |
| `/announcements/{id}` | PUT/DELETE | Update / delete announcement |
| `/announcements/active` | GET | Get currently active announcements |

### WebSocket
| Endpoint | Description |
|----------|-------------|
| `WS /ws/{tenant_id}` | Real-time push channel per tenant |

---

## Installation

### Prerequisites
- Python 3.9+
- PostgreSQL 12+
- AWS account with S3 access

### Setup

```bash
git clone <repository-url>
cd cms-backend

python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install -r requirement.txt
```

### Environment Variables

Create a `.env` file:

```env
# PostgreSQL
PGHOST=localhost
PGPORT=5432
PGDATABASE=dgx
PGUSER=app_user
PGPASSWORD=your_password
PG_MIN_CONN=1
PG_MAX_CONN=10

# AWS S3
S3_BUCKET=your-bucket-name
S3_PREFIX=videos
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
PRESIGN_EXPIRES=3600

# App
PORT=8005
SECRET_KEY=your_secret_key
```

---

## Running

### Development
```bash
uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --reload
```

### Production
```bash
uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --workers 4
```

Interactive API docs available at `http://localhost:8005/docs`

---

## Database Schema (Main Tables)

| Table | Description |
|-------|-------------|
| `device` | Device registration, status, storage, expiration |
| `video` | Video metadata and S3 references |
| `advertisement` | Image/ad metadata and S3 references |
| `shop` | Shop/location entities |
| `group` | Device groups |
| `device_video_shop_group` | Core link table (device ↔ video ↔ shop ↔ group) |
| `device_assignment` | Device ↔ group ↔ shop assignment |
| `group_video` | Group ↔ video membership |
| `group_advertisement` | Group ↔ advertisement membership |
| `device_layout` | Per-device grid layout config |
| `content_change_request` | Approval workflow requests |
| `announcement` | Global/targeted announcements |
| `company` | Tenant/company records |

---

## Deployment

CI/CD is configured via `.github/workflows/deploy-backend.yml`. Pushes to `staging` trigger automatic deployment.

---

## Branch Strategy

| Branch | Purpose |
|--------|---------|
| `staging` | Main integration branch — all PRs target here |
| `main` | Production-stable |
| `fix/*` | Bug fix branches |
| `feat/*` | Feature branches |
