# DGX Backend

A microservices-based backend system for managing devices, videos, shops, and groups with PostgreSQL and AWS S3 integration. Built with FastAPI.

## Overview

This backend provides RESTful APIs for a device management and video distribution system. It consists of multiple FastAPI services that can run independently or together, managing:

- **Devices** - Mobile/IoT device registration and status tracking
- **Videos** - Video file uploads to S3 with metadata management
- **Shops** - Shop/location management
- **Groups** - Device grouping functionality
- **Links** - Associations between devices, videos, shops, and groups

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Services                              │
├──────────────┬──────────────┬──────────────┬───────────────────┤
│ device.py    │ video.py     │ shop.py      │ group.py          │
│ Port: 8000   │ Port: 8003   │ Port: 8002   │ Port: 8001        │
├──────────────┴──────────────┴──────────────┴───────────────────┤
│           device_video_shop_group.py (Combined Service)         │
│                         Port: 8005                               │
├─────────────────────────────────────────────────────────────────┤
│                     PostgreSQL Database                          │
│                     AWS S3 (Video Storage)                       │
└─────────────────────────────────────────────────────────────────┘
```

## Services

### Device Service (`device.py`)
Manages device registration and status tracking.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/db_health` | GET | Database health check |
| `/insert_device` | POST | Register a new device |
| `/device/{mobile_id}` | GET | Get device by mobile ID |
| `/devices` | GET | List devices (paginated, searchable) |
| `/device/{mobile_id}` | PUT | Update device |
| `/device/{mobile_id}` | DELETE | Delete device |

### Video Service (`video_service.py`)
Handles video uploads to S3 and metadata management.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/upload_video` | POST | Upload video to S3 |
| `/video/{video_name}` | GET | Get video metadata + presigned URL |
| `/video/{video_name}/presign` | GET | Generate presigned download URL |
| `/videos` | GET | List videos (paginated) |
| `/video/{video_name}` | PUT | Update video metadata |
| `/video/{video_name}/rotation` | POST | Set video rotation |
| `/video/{video_name}/fit_mode` | POST | Set video fit mode |
| `/video/{video_name}` | DELETE | Delete video record |

### Shop Service (`shop_service.py`)
Manages shop/location entities.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/insert_shop` | POST | Create a new shop |
| `/shop/{shop_name}` | GET | Get shop by name |
| `/shops` | GET | List shops (paginated) |
| `/shop/{shop_name}` | PUT | Update shop |
| `/shop/{shop_name}` | DELETE | Delete shop |

### Group Service (`group.py`)
Manages device groups.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/insert_group` | POST | Create a new group |
| `/group/{gname}` | GET | Get group by name |
| `/groups` | GET | List groups (paginated) |
| `/group/{gname}` | PUT | Update group |
| `/group/{gname}` | DELETE | Delete group |

### Combined Service (`device_video_shop_group.py`)
Main unified service providing linking functionality and extended features.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/link` | POST | Create device-video-shop-group link |
| `/links` | GET | List all links |
| `/links/by_device/{mobile_id}` | GET | Get links for a device |
| `/links/by_group/{gname}` | GET | Get links for a group |
| `/device/{mobile_id}/presigned_urls` | GET | Get presigned URLs for device's videos |
| `/device/{mobile_id}/online` | POST | Update device online status |
| `/device/{mobile_id}/temperature` | POST | Update device temperature |
| `/device/{mobile_id}/logs` | GET | Get device logs |
| `/device/{mobile_id}/logs/download` | GET | Download device logs as CSV |
| `/devices/logs/summary` | GET | Get logs summary for all devices |
| `/group/{gname}/videos` | PUT | Update videos for a group |
| `/admin/mark_offline_devices` | POST | Mark stale devices as offline |

## Installation

### Prerequisites
- Python 3.9+
- PostgreSQL 12+
- AWS Account with S3 access

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd dgx-backend
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   venv\Scripts\activate     # Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirement.txt
   ```

4. **Configure environment variables**
   
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
   S3_PREFIX=myvideos
   AWS_REGION=us-east-1
   PRESIGN_EXPIRES=3600
   
   # Optional
   PORT=8005
   ```

5. **Initialize the database**
   
   The services auto-create tables on startup, or run migrations manually.

## Running the Services

### Development Mode

Run individual services:
```bash
# Device Service
uvicorn device:app --host 0.0.0.0 --port 8000 --reload

# Video Service
uvicorn video_service:app --host 0.0.0.0 --port 8003 --reload

# Shop Service
uvicorn shop_service:app --host 0.0.0.0 --port 8002 --reload

# Group Service
uvicorn group:app --host 0.0.0.0 --port 8001 --reload

# Combined Service (recommended)
uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --reload
```

Or run directly:
```bash
python device_video_shop_group.py
```

### Production Mode

```bash
uvicorn device_video_shop_group:app --host 0.0.0.0 --port 8005 --workers 4
```

## Database Schema

The system uses the following main tables:

- `device` - Device registration and status
- `video` - Video metadata and S3 references
- `shop` - Shop information
- `group` - Group definitions
- `device_video_shop_group` - Link table connecting all entities
- `device_logs` - Device telemetry logs

## Cron Jobs

### Mark Offline Devices

Use `mark_devices_offline.sh` to mark devices as offline if they haven't reported recently:

```bash
# Add to crontab to run every minute
* * * * * /path/to/mark_devices_offline.sh
```

Or call the API endpoint:
```bash
curl -X POST http://localhost:8005/admin/mark_offline_devices
```

## Video Properties

Videos support the following configurable properties:

| Property | Values | Default | Description |
|----------|--------|---------|-------------|
| `rotation` | 0, 90, 180, 270 | 0 | Display rotation in degrees |
| `fit_mode` | cover, contain, fill, none | cover | CSS object-fit behavior |
| `content_type` | video, image, html, pdf | video | Media type |
| `display_duration` | seconds | 10 | Display duration for slideshows |

## Dependencies

```
fastapi==0.109.0
uvicorn[standard]==0.27.0
psycopg2-binary==2.9.9
boto3==1.34.0
python-dotenv==1.0.0
pydantic==2.5.0
```

## API Documentation

Once running, access the interactive API documentation:

- Swagger UI: `http://localhost:8005/docs`
- ReDoc: `http://localhost:8005/redoc`

## Error Handling

The API uses standard HTTP status codes:

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad request / validation error |
| 404 | Resource not found |
| 409 | Conflict (e.g., deleting linked device) |
| 500 | Internal server error |
| 503 | Database connection pool exhausted |

## Security Notes

- CORS is configured to allow all origins (`*`) - restrict in production
- Database credentials should be properly secured
- S3 buckets should have appropriate access policies
- Consider adding authentication/authorization for production use

## License

[Add your license here]
