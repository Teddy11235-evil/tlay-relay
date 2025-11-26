#!/usr/bin/env python3
"""
Cloud Relay Server for Render Farm - Railway Deployment
"""
import asyncio
import websockets
import json
import logging
import os
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RenderRelay")

class RenderRelay:
    def __init__(self):
        self.controllers = set()
        self.nodes = set()
        self.node_info = {}  # node_id -> info
        self.command_queues = {}  # node_id -> command queue
        self.controller_nodes = {}  # controller_id -> node mapping
        
    async def register_controller(self, websocket):
        """Register a controller connection"""
        controller_id = str(uuid.uuid4())
        self.controllers.add(websocket)
        self.controller_nodes[controller_id] = websocket
        
        logger.info(f"Controller connected: {controller_id}")
        
        # Send welcome message with current node list
        welcome_msg = {
            'type': 'welcome',
            'controller_id': controller_id,
            'message': 'Connected to Render Farm Relay',
            'nodes': list(self.node_info.values())
        }
        await self.send_to_controller(websocket, welcome_msg)
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_controller_message(controller_id, websocket, data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from controller {controller_id}")
                    await self.send_to_controller(websocket, {
                        'type': 'error',
                        'message': 'Invalid JSON format'
                    })
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Controller disconnected: {controller_id}")
        except Exception as e:
            logger.error(f"Controller error {controller_id}: {e}")
        finally:
            self.controllers.remove(websocket)
            if controller_id in self.controller_nodes:
                del self.controller_nodes[controller_id]
            logger.info(f"Controller cleaned up: {controller_id}")
    
    async def register_node(self, websocket):
        """Register a node connection"""
        node_id = str(uuid.uuid4())
        self.nodes.add(websocket)
        self.command_queues[node_id] = asyncio.Queue()
        
        logger.info(f"Node connected: {node_id}")
        
        try:
            # Wait for registration message
            message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
            data = json.loads(message)
            
            if data.get('type') != 'register':
                await websocket.close(1008, "Expected registration message")
                return
                
            # Complete registration
            node_name = data.get('node_name', f'Node-{node_id[:8]}')
            self.node_info[node_id] = {
                'node_id': node_id,
                'node_name': node_name,
                'computer_name': data.get('computer_name', 'Unknown'),
                'system_info': data.get('system_info', {}),
                'connected_at': datetime.utcnow().isoformat() + 'Z',
                'last_heartbeat': datetime.utcnow().isoformat() + 'Z',
                'status': 'online',
                'ip_address': 'unknown'  # WebSocket doesn't expose IP easily
            }
            
            logger.info(f"Node registered: {node_name} ({node_id})")
            
            # Notify all controllers
            await self.notify_controllers('node_connected', self.node_info[node_id])
            
            # Send confirmation to node
            await websocket.send(json.dumps({
                'type': 'registered',
                'node_id': node_id,
                'message': 'Successfully registered with relay'
            }))
            
            # Start heartbeat monitoring
            heartbeat_task = asyncio.create_task(self.monitor_node_heartbeat(websocket, node_id))
            command_task = asyncio.create_task(self.process_node_commands(websocket, node_id))
            
            # Message processing loop
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_node_message(websocket, node_id, data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from node {node_id}")
                    
            # Cleanup
            heartbeat_task.cancel()
            command_task.cancel()
            
        except asyncio.TimeoutError:
            logger.warning(f"Node {node_id} registration timeout")
            await websocket.close(1008, "Registration timeout")
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Node connection closed during registration: {node_id}")
        except Exception as e:
            logger.error(f"Node registration error {node_id}: {e}")
            await websocket.close(1011, "Registration failed")
        finally:
            await self.cleanup_node(node_id, websocket)
    
    async def monitor_node_heartbeat(self, websocket, node_id):
        """Monitor node heartbeat and detect disconnections"""
        try:
            while True:
                await asyncio.sleep(60)  # Check every minute
                
                if node_id not in self.node_info:
                    break
                    
                last_heartbeat = datetime.fromisoformat(
                    self.node_info[node_id]['last_heartbeat'].replace('Z', '+00:00')
                )
                time_since_heartbeat = (datetime.utcnow() - last_heartbeat).total_seconds()
                
                if time_since_heartbeat > 120:  # 2 minutes without heartbeat
                    logger.warning(f"Node {node_id} heartbeat timeout")
                    await self.cleanup_node(node_id, websocket)
                    break
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Heartbeat monitor error for {node_id}: {e}")
    
    async def process_node_commands(self, websocket, node_id):
        """Process command queue for a node"""
        try:
            while True:
                if node_id not in self.command_queues:
                    break
                    
                command_data = await self.command_queues[node_id].get()
                
                if command_data.get('cancelled'):
                    continue
                    
                try:
                    await websocket.send(json.dumps({
                        'type': 'command',
                        'command': command_data['command'],
                        'command_id': command_data['command_id'],
                        'from_controller': command_data['from_controller']
                    }))
                    logger.info(f"Command sent to node {node_id}: {command_data['command']}")
                except websockets.exceptions.ConnectionClosed:
                    logger.warning(f"Node {node_id} disconnected during command send")
                    break
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Command processor error for {node_id}: {e}")
    
    async def handle_controller_message(self, controller_id, websocket, data):
        """Handle messages from controller"""
        msg_type = data.get('type')
        
        if msg_type == 'command':
            node_name = data.get('node_name')
            command = data.get('command')
            command_id = data.get('command_id', str(uuid.uuid4()))
            
            logger.info(f"Controller {controller_id} command for {node_name}: {command}")
            
            # Find node by name
            target_node_id = None
            target_websocket = None
            
            for node_id, info in self.node_info.items():
                if info.get('node_name') == node_name:
                    target_node_id = node_id
                    for node_ws in self.nodes:
                        if node_ws not in self.nodes:  # Check if still connected
                            continue
                        # We need to track node WebSocket associations better
                        # For simplicity, we'll use the command queue approach
                    break
            
            if target_node_id and target_node_id in self.command_queues:
                # Queue command for node
                await self.command_queues[target_node_id].put({
                    'command': command,
                    'command_id': command_id,
                    'from_controller': controller_id
                })
                
                await self.send_to_controller(websocket, {
                    'type': 'command_queued',
                    'command_id': command_id,
                    'node_name': node_name,
                    'message': 'Command queued for delivery'
                })
                
            else:
                await self.send_to_controller(websocket, {
                    'type': 'error',
                    'command_id': command_id,
                    'message': f'Node {node_name} not found or not connected'
                })
                
        elif msg_type == 'ping':
            await self.send_to_controller(websocket, {
                'type': 'pong',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
            
        elif msg_type == 'list_nodes':
            await self.send_to_controller(websocket, {
                'type': 'node_list',
                'nodes': list(self.node_info.values())
            })
    
    async def handle_node_message(self, websocket, node_id, data):
        """Handle messages from node"""
        msg_type = data.get('type')
        
        if msg_type == 'heartbeat':
            # Update node heartbeat
            if node_id in self.node_info:
                self.node_info[node_id]['last_heartbeat'] = datetime.utcnow().isoformat() + 'Z'
                self.node_info[node_id]['status'] = 'online'
                
        elif msg_type == 'command_response':
            # Forward response to appropriate controller
            controller_id = data.get('controller_id')
            command_id = data.get('command_id')
            response = data.get('response', {})
            
            if controller_id in self.controller_nodes:
                await self.send_to_controller(self.controller_nodes[controller_id], {
                    'type': 'command_response',
                    'command_id': command_id,
                    'node_name': self.node_info.get(node_id, {}).get('node_name'),
                    'response': response,
                    'success': data.get('success', True),
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                })
                
        elif msg_type == 'status_update':
            # Update node status information
            if node_id in self.node_info:
                self.node_info[node_id].update(data.get('status', {}))
                await self.notify_controllers('node_updated', self.node_info[node_id])
    
    async def send_to_controller(self, websocket, data):
        """Send data to specific controller"""
        try:
            await websocket.send(json.dumps(data))
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"Error sending to controller: {e}")
    
    async def notify_controllers(self, event_type, data):
        """Notify all controllers of an event"""
        message = {
            'type': event_type,
            'data': data,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
        disconnected_controllers = []
        for controller in self.controllers:
            try:
                await controller.send(json.dumps(message))
            except websockets.exceptions.ConnectionClosed:
                disconnected_controllers.append(controller)
            except Exception as e:
                logger.error(f"Error notifying controller: {e}")
                disconnected_controllers.append(controller)
        
        # Cleanup disconnected controllers
        for controller in disconnected_controllers:
            self.controllers.remove(controller)
            # Also remove from controller_nodes mapping
            for cid, ws in list(self.controller_nodes.items()):
                if ws == controller:
                    del self.controller_nodes[cid]
                    break
    
    async def cleanup_node(self, node_id, websocket):
        """Clean up node resources"""
        if websocket in self.nodes:
            self.nodes.remove(websocket)
            
        if node_id in self.node_info:
            node_name = self.node_info[node_id]['node_name']
            del self.node_info[node_id]
            logger.info(f"Node cleaned up: {node_name} ({node_id})")
            
            # Notify controllers
            await self.notify_controllers('node_disconnected', {
                'node_id': node_id,
                'node_name': node_name,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        
        if node_id in self.command_queues:
            # Cancel all pending commands for this node
            while not self.command_queues[node_id].empty():
                try:
                    command_data = self.command_queues[node_id].get_nowait()
                    command_data['cancelled'] = True
                except asyncio.QueueEmpty:
                    break
            del self.command_queues[node_id]

# Create global relay instance
relay = RenderRelay()

async def handler(websocket, path):
    """
    Main WebSocket connection handler
    """
    client_ip = websocket.remote_address[0] if websocket.remote_address else 'unknown'
    logger.info(f"New connection from {client_ip}")
    
    try:
        # First message should identify connection type
        message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
        data = json.loads(message)
        
        connection_type = data.get('type')
        
        if connection_type == 'controller':
            await relay.register_controller(websocket)
        elif connection_type == 'node':
            await relay.register_node(websocket)
        else:
            await websocket.close(1008, "Invalid connection type. Use 'controller' or 'node'")
            
    except asyncio.TimeoutError:
        logger.warning(f"Connection timeout from {client_ip}")
        await websocket.close(1008, "Connection timeout")
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON from {client_ip}")
        await websocket.close(1008, "Invalid JSON")
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed during handshake: {client_ip}")
    except Exception as e:
        logger.error(f"Connection error from {client_ip}: {e}")
        await websocket.close(1011, "Internal server error")

async def health_check():
    """Simple health check endpoint"""
    return {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'stats': {
            'controllers_connected': len(relay.controllers),
            'nodes_connected': len(relay.nodes),
            'total_nodes_registered': len(relay.node_info)
        }
    }

async def main():
    """Main application entry point"""
    # Get port from environment (Railway provides this)
    port = int(os.getenv('PORT', 8765))
    
    # Start WebSocket server
    async with websockets.serve(
        handler,
        "0.0.0.0",  # Listen on all interfaces
        port,
        ping_interval=20,
        ping_timeout=10,
        max_size=10 * 1024 * 1024  # 10MB max message size
    ):
        logger.info(f"🌈 Render Farm Cloud Relay Server started on port {port}")
        logger.info(f"🚀 Ready for controllers and nodes to connect!")
        logger.info(f"📊 Current stats: {len(relay.controllers)} controllers, {len(relay.nodes)} nodes")
        
        # Keep the server running
        await asyncio.Future()

if __name__ == "__main__":
    # Set event loop policy for Windows compatibility
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server fatal error: {e}")
        raise
