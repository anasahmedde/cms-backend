# websocket_manager.py
# Location: cms-backend-staging/websocket_manager.py
# Real-time Device Status WebSocket Manager

import asyncio
import json
from datetime import datetime
from typing import Dict, Set, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

from fastapi import WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState


class MessageType(str, Enum):
    """Types of WebSocket messages."""
    # Server -> Client
    DEVICE_STATUS = "device_status"
    DEVICE_LIST = "device_list"
    DEVICE_ONLINE = "device_online"
    DEVICE_OFFLINE = "device_offline"
    DEVICE_TEMPERATURE = "device_temperature"
    DEVICE_DOWNLOAD = "device_download"
    HEARTBEAT = "heartbeat"
    ERROR = "error"
    # Platform announcements
    ANNOUNCEMENT = "announcement"
    ANNOUNCEMENT_CLEARED = "announcement_cleared"
    
    # Content approval
    PENDING_APPROVALS = "pending_approvals"

    # Client -> Server
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    PING = "ping"


@dataclass
class DeviceStatus:
    """Real-time device status."""
    mobile_id: str
    device_name: Optional[str] = None
    is_online: bool = False
    is_active: bool = True
    temperature: Optional[float] = None
    last_online_at: Optional[str] = None
    download_status: bool = False
    download_progress: Optional[dict] = None
    daily_count: int = 0
    monthly_count: int = 0
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class WebSocketMessage:
    """Standard WebSocket message format."""
    type: str
    data: Any
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))


class ConnectionManager:
    """
    Manages WebSocket connections for real-time device status updates.
    
    Features:
    - Per-tenant isolation
    - Subscription-based updates
    - Automatic reconnection handling
    - Heartbeat to detect stale connections
    """
    
    def __init__(self):
        # {tenant_id: {websocket: {"subscriptions": set(), "user_id": int}}}
        self._connections: Dict[int, Dict[WebSocket, dict]] = {}
        self._global_connections: Dict[WebSocket, dict] = {}
        self._lock = asyncio.Lock()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
    
    async def start(self):
        """Start the connection manager background tasks."""
        self._running = True
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        print("[WS] Connection manager started")
    
    async def stop(self):
        """Stop the connection manager."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        async with self._lock:
            for tenant_connections in self._connections.values():
                for ws in list(tenant_connections.keys()):
                    await self._safe_close(ws)
            for ws in list(self._global_connections.keys()):
                await self._safe_close(ws)
            
            self._connections.clear()
            self._global_connections.clear()
        
        print("[WS] Connection manager stopped")
    
    async def connect(
        self,
        websocket: WebSocket,
        tenant_id: Optional[int],
        user_id: int,
        is_platform_user: bool = False
    ):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        
        connection_info = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "is_platform_user": is_platform_user,
            "subscriptions": set(),
            "watch_all": False,
            "connected_at": datetime.now().isoformat(),
        }
        
        async with self._lock:
            if is_platform_user and tenant_id is None:
                self._global_connections[websocket] = connection_info
            else:
                if tenant_id not in self._connections:
                    self._connections[tenant_id] = {}
                self._connections[tenant_id][websocket] = connection_info
        
        print(f"[WS] New connection: user={user_id}, tenant={tenant_id}, platform={is_platform_user}")
        
        await self._send(websocket, WebSocketMessage(
            type="connected",
            data={
                "user_id": user_id,
                "tenant_id": tenant_id,
                "message": "Connected to device status stream"
            }
        ))
    
    async def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        async with self._lock:
            if websocket in self._global_connections:
                del self._global_connections[websocket]
                print("[WS] Global connection removed")
                return
            
            for tenant_id, connections in list(self._connections.items()):
                if websocket in connections:
                    del connections[websocket]
                    if not connections:
                        del self._connections[tenant_id]
                    print(f"[WS] Tenant {tenant_id} connection removed")
                    return
    
    async def handle_message(self, websocket: WebSocket, message: str):
        """Handle incoming WebSocket message from client."""
        try:
            data = json.loads(message)
            msg_type = data.get("type")
            
            if msg_type == "subscribe":
                await self._handle_subscribe(websocket, data)
            elif msg_type == "unsubscribe":
                await self._handle_unsubscribe(websocket, data)
            elif msg_type == "ping":
                await self._send(websocket, WebSocketMessage(type="pong", data={}))
            else:
                await self._send(websocket, WebSocketMessage(
                    type="error",
                    data={"message": f"Unknown message type: {msg_type}"}
                ))
        except json.JSONDecodeError:
            await self._send(websocket, WebSocketMessage(
                type="error",
                data={"message": "Invalid JSON"}
            ))
    
    async def _handle_subscribe(self, websocket: WebSocket, data: dict):
        """Handle subscription request."""
        async with self._lock:
            info = self._get_connection_info(websocket)
            if not info:
                return
            
            if data.get("all"):
                info["watch_all"] = True
                await self._send(websocket, WebSocketMessage(
                    type="subscribed",
                    data={"all": True, "message": "Subscribed to all devices"}
                ))
            elif "devices" in data:
                devices = set(data["devices"])
                info["subscriptions"].update(devices)
                await self._send(websocket, WebSocketMessage(
                    type="subscribed",
                    data={"devices": list(devices)}
                ))
    
    async def _handle_unsubscribe(self, websocket: WebSocket, data: dict):
        """Handle unsubscription request."""
        async with self._lock:
            info = self._get_connection_info(websocket)
            if not info:
                return
            
            if data.get("all"):
                info["watch_all"] = False
                info["subscriptions"].clear()
            elif "devices" in data:
                devices = set(data["devices"])
                info["subscriptions"] -= devices
            
            await self._send(websocket, WebSocketMessage(
                type="unsubscribed",
                data={"message": "Unsubscribed successfully"}
            ))
    
    # ===========================================
    # Broadcast Methods
    # ===========================================
    
    async def broadcast_device_status(self, tenant_id: int, device_status: DeviceStatus):
        """Broadcast device status update."""
        message = WebSocketMessage(
            type=MessageType.DEVICE_STATUS,
            data=device_status.to_dict()
        )
        await self._broadcast_to_tenant(tenant_id, device_status.mobile_id, message)
    
    async def broadcast_device_online(self, tenant_id: int, mobile_id: str, device_name: Optional[str] = None):
        """Broadcast when a device comes online."""
        message = WebSocketMessage(
            type=MessageType.DEVICE_ONLINE,
            data={
                "mobile_id": mobile_id,
                "device_name": device_name,
                "is_online": True,
                "timestamp": datetime.now().isoformat()
            }
        )
        await self._broadcast_to_tenant(tenant_id, mobile_id, message)
    
    async def broadcast_device_offline(self, tenant_id: int, mobile_id: str, device_name: Optional[str] = None):
        """Broadcast when a device goes offline."""
        message = WebSocketMessage(
            type=MessageType.DEVICE_OFFLINE,
            data={
                "mobile_id": mobile_id,
                "device_name": device_name,
                "is_online": False,
                "timestamp": datetime.now().isoformat()
            }
        )
        await self._broadcast_to_tenant(tenant_id, mobile_id, message)
    
    async def broadcast_device_temperature(self, tenant_id: int, mobile_id: str, temperature: float):
        """Broadcast temperature update."""
        message = WebSocketMessage(
            type=MessageType.DEVICE_TEMPERATURE,
            data={
                "mobile_id": mobile_id,
                "temperature": temperature,
                "timestamp": datetime.now().isoformat()
            }
        )
        await self._broadcast_to_tenant(tenant_id, mobile_id, message)
    
    async def broadcast_download_progress(self, tenant_id: int, mobile_id: str, progress: dict):
        """Broadcast download progress update."""
        message = WebSocketMessage(
            type=MessageType.DEVICE_DOWNLOAD,
            data={
                "mobile_id": mobile_id,
                "progress": progress,
                "timestamp": datetime.now().isoformat()
            }
        )
        await self._broadcast_to_tenant(tenant_id, mobile_id, message)
    
    async def broadcast_announcement(self, announcement: dict):
        """
        Broadcast a platform announcement.
        - target_type='all'     → sends to every connected user
        - target_type='company' → sends only to that tenant's connections
        """
        message = WebSocketMessage(
            type=MessageType.ANNOUNCEMENT,
            data=announcement
        )
        target_type = announcement.get("target_type", "all")
        target_company_id = announcement.get("target_company_id")

        if target_type == "company" and target_company_id:
            await self._broadcast_to_tenant_all(target_company_id, message)
        else:
            await self._broadcast_to_all(message)
    
    async def broadcast_announcement_cleared(self):
        """Broadcast that announcement has been cleared to ALL connected users."""
        message = WebSocketMessage(
            type=MessageType.ANNOUNCEMENT_CLEARED,
            data={"cleared": True, "timestamp": datetime.now().isoformat()}
        )
        await self._broadcast_to_all(message)

    async def broadcast_pending_approvals(self, tenant_id: int, pending_count: int):
        """
        Push the current pending approval count to all connections of a tenant.
        Called after any create/approve/reject so admins/managers see the badge
        update instantly without polling.
        """
        message = WebSocketMessage(
            type=MessageType.PENDING_APPROVALS,
            data={"pending_count": pending_count, "timestamp": datetime.now().isoformat()}
        )
        await self._broadcast_to_tenant_all(tenant_id, message)
    
    async def _broadcast_to_all(self, message: WebSocketMessage):
        """Send message to ALL connected users (for platform-wide announcements)."""
        tasks = []
        
        async with self._lock:
            for ws in list(self._global_connections.keys()):
                tasks.append(self._send(ws, message))
            
            for tenant_connections in self._connections.values():
                for ws in list(tenant_connections.keys()):
                    tasks.append(self._send(ws, message))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _broadcast_to_tenant_all(self, tenant_id: int, message: WebSocketMessage):
        """Send message to ALL connections of a specific tenant (for company-targeted announcements)."""
        tasks = []
        
        async with self._lock:
            if tenant_id in self._connections:
                for ws in list(self._connections[tenant_id].keys()):
                    tasks.append(self._send(ws, message))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    # ===========================================
    # Internal Methods
    # ===========================================
    
    async def _broadcast_to_tenant(self, tenant_id: int, mobile_id: Optional[str], message: WebSocketMessage):
        """Send message to relevant subscribers."""
        tasks = []
        
        async with self._lock:
            if tenant_id in self._connections:
                for ws, info in list(self._connections[tenant_id].items()):
                    if self._should_receive(info, mobile_id):
                        tasks.append(self._send(ws, message))
            
            for ws, info in list(self._global_connections.items()):
                if self._should_receive(info, mobile_id):
                    tasks.append(self._send(ws, message))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def _should_receive(self, connection_info: dict, mobile_id: Optional[str]) -> bool:
        """Check if a connection should receive an update."""
        if connection_info["watch_all"]:
            return True
        if mobile_id and mobile_id in connection_info["subscriptions"]:
            return True
        return False
    
    def _get_connection_info(self, websocket: WebSocket) -> Optional[dict]:
        """Get connection info for a websocket."""
        if websocket in self._global_connections:
            return self._global_connections[websocket]
        
        for connections in self._connections.values():
            if websocket in connections:
                return connections[websocket]
        
        return None
    
    async def _send(self, websocket: WebSocket, message: WebSocketMessage):
        """Safely send a message to a websocket."""
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_text(message.to_json())
        except Exception as e:
            print(f"[WS] Send error: {e}")
            await self.disconnect(websocket)
    
    async def _safe_close(self, websocket: WebSocket):
        """Safely close a websocket."""
        try:
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.close()
        except Exception:
            pass
    
    async def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while self._running:
            try:
                await asyncio.sleep(30)
                
                message = WebSocketMessage(
                    type=MessageType.HEARTBEAT,
                    data={"server_time": datetime.now().isoformat()}
                )
                
                async with self._lock:
                    all_websockets = list(self._global_connections.keys())
                    for connections in self._connections.values():
                        all_websockets.extend(connections.keys())
                
                for ws in all_websockets:
                    await self._send(ws, message)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[WS] Heartbeat error: {e}")
    
    def get_stats(self) -> dict:
        """Get connection statistics."""
        total_tenant_connections = sum(
            len(conns) for conns in self._connections.values()
        )
        
        return {
            "global_connections": len(self._global_connections),
            "tenant_connections": total_tenant_connections,
            "total_connections": len(self._global_connections) + total_tenant_connections,
            "tenants_with_connections": len(self._connections),
        }


# Global Instance
ws_manager = ConnectionManager()
