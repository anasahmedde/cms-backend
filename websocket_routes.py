# websocket_routes.py
# Location: cms-backend-staging/websocket_routes.py
# WebSocket Endpoints for Real-time Device Status

from typing import Optional
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from starlette.websockets import WebSocketState

from websocket_manager import ws_manager, DeviceStatus, WebSocketMessage
from database import AsyncDatabasePool
from tenant_context import active_sessions

router = APIRouter()


def validate_token(token: str) -> Optional[dict]:
    """
    Validate WebSocket authentication token.
    Returns session info if valid, None if invalid.
    """
    if not token:
        return None
    
    if token.startswith("Bearer "):
        token = token[7:]
    
    session = active_sessions.get(token)
    if not session:
        return None
    
    if datetime.now() > session.get("expires_at", datetime.min):
        return None
    
    return session


@router.websocket("/ws/devices")
async def websocket_device_status(
    websocket: WebSocket,
    token: str = Query(None, description="Authentication token")
):
    """
    WebSocket endpoint for real-time device status updates.
    
    Connection URL: ws://server:8005/ws/devices?token=YOUR_TOKEN
    
    After connecting, send subscription messages:
    - {"type": "subscribe", "all": true} - Subscribe to all devices
    - {"type": "subscribe", "devices": ["device1", "device2"]} - Specific devices
    - {"type": "unsubscribe", "devices": ["device1"]} - Unsubscribe
    - {"type": "ping"} - Keepalive
    
    You will receive:
    - {"type": "device_list", "data": {...}} - Initial device list
    - {"type": "device_status", "data": {...}} - Status updates
    - {"type": "device_online", "data": {...}} - Device came online
    - {"type": "device_offline", "data": {...}} - Device went offline
    - {"type": "device_temperature", "data": {...}} - Temperature updates
    - {"type": "heartbeat", "data": {...}} - Server heartbeat (every 30s)
    """
    # Validate token
    session = validate_token(token)
    if not session:
        await websocket.close(code=4001, reason="Invalid or expired token")
        return
    
    user_id = session.get("user_id")
    tenant_id = session.get("active_tenant_id") or session.get("tenant_id")
    is_platform_user = session.get("user_type") == "platform"
    
    # Connect
    await ws_manager.connect(
        websocket=websocket,
        tenant_id=tenant_id,
        user_id=user_id,
        is_platform_user=is_platform_user
    )
    
    try:
        # Send initial device list
        await send_initial_device_list(websocket, tenant_id, is_platform_user)
        
        # Listen for messages
        while True:
            message = await websocket.receive_text()
            await ws_manager.handle_message(websocket, message)
            
    except WebSocketDisconnect:
        print(f"[WS] Client disconnected: user={user_id}")
    except Exception as e:
        print(f"[WS] Error: {e}")
    finally:
        await ws_manager.disconnect(websocket)


async def send_initial_device_list(
    websocket: WebSocket,
    tenant_id: Optional[int],
    is_platform_user: bool
):
    """Send the initial device list when client connects."""
    try:
        async with AsyncDatabasePool.connection() as conn:
            if is_platform_user and tenant_id is None:
                rows = await conn.fetch("""
                    SELECT d.mobile_id, d.device_name, d.is_online, d.is_active,
                           d.temperature, d.last_online_at, d.download_status,
                           d.daily_count, d.monthly_count, d.tenant_id,
                           c.name as company_name
                    FROM public.device d
                    LEFT JOIN public.company c ON c.id = d.tenant_id
                    ORDER BY d.is_online DESC, d.last_online_at DESC NULLS LAST
                    LIMIT 500
                """)
            else:
                rows = await conn.fetch("""
                    SELECT d.mobile_id, d.device_name, d.is_online, d.is_active,
                           d.temperature, d.last_online_at, d.download_status,
                           d.daily_count, d.monthly_count, d.tenant_id,
                           NULL as company_name
                    FROM public.device d
                    WHERE d.tenant_id = $1
                    ORDER BY d.is_online DESC, d.last_online_at DESC NULLS LAST
                    LIMIT 500
                """, tenant_id)
        
        devices = []
        for row in rows:
            devices.append({
                "mobile_id": row["mobile_id"],
                "device_name": row["device_name"],
                "is_online": row["is_online"],
                "is_active": row["is_active"],
                "temperature": row["temperature"],
                "last_online_at": row["last_online_at"].isoformat() if row["last_online_at"] else None,
                "download_status": row["download_status"],
                "daily_count": row["daily_count"],
                "monthly_count": row["monthly_count"],
                "tenant_id": row["tenant_id"],
                "company_name": row["company_name"],
            })
        
        message = WebSocketMessage(
            type="device_list",
            data={
                "devices": devices,
                "count": len(devices)
            }
        )
        
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_text(message.to_json())
            
    except Exception as e:
        print(f"[WS] Error sending initial device list: {e}")


@router.get("/ws/stats")
async def get_websocket_stats():
    """Get WebSocket connection statistics."""
    return ws_manager.get_stats()


# ===========================================
# Helper functions - call from API endpoints
# ===========================================

async def notify_device_online(tenant_id: int, mobile_id: str, device_name: Optional[str] = None):
    """Call this when a device comes online."""
    await ws_manager.broadcast_device_online(tenant_id, mobile_id, device_name)


async def notify_device_offline(tenant_id: int, mobile_id: str, device_name: Optional[str] = None):
    """Call this when a device goes offline."""
    await ws_manager.broadcast_device_offline(tenant_id, mobile_id, device_name)


async def notify_device_status(tenant_id: int, status: DeviceStatus):
    """Call this when device status changes."""
    await ws_manager.broadcast_device_status(tenant_id, status)


async def notify_device_temperature(tenant_id: int, mobile_id: str, temperature: float):
    """Call this when temperature is updated."""
    await ws_manager.broadcast_device_temperature(tenant_id, mobile_id, temperature)


async def notify_download_progress(tenant_id: int, mobile_id: str, progress: dict):
    """Call this when download progress updates."""
    await ws_manager.broadcast_download_progress(tenant_id, mobile_id, progress)


async def notify_announcement(announcement: dict):
    """Call this when a new announcement is published - broadcasts to ALL users."""
    await ws_manager.broadcast_announcement(announcement)


async def notify_announcement_cleared():
    """Call this when announcement is cleared - broadcasts to ALL users."""
    await ws_manager.broadcast_announcement_cleared()


async def notify_pending_approvals(tenant_id: int, pending_count: int):
    """
    Push updated pending approval count to all connections of a tenant.
    Call this after any approval request is created, approved, or rejected.
    """
    await ws_manager.broadcast_pending_approvals(tenant_id, pending_count)
