#!/usr/bin/env python3
"""
Koyeb-Optimized Relay Server
"""
import asyncio
import websockets
import json
import logging
import os
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("KoyebRelay")

class KoyebRelay:
    def __init__(self):
        self.controllers = set()
        self.nodes = {}
        self.node_info = {}
        
    async def handle_connection(self, websocket, path):
        client_ip = websocket.remote_address[0] if websocket.remote_address else 'unknown'
        logger.info(f"New connection from {client_ip}")
        
        try:
            # Wait for client identification
            message = await asyncio.wait_for(websocket.recv(), timeout=15.0)
            data = json.loads(message)
            
            client_type = data.get('type')
            if client_type == 'controller':
                await self.handle_controller(websocket, data)
            elif client_type == 'node':
                await self.handle_node(websocket, data)
            else:
                await websocket.close(1008, "Invalid client type")
                
        except asyncio.TimeoutError:
            logger.warning(f"Connection timeout from {client_ip}")
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON from {client_ip}")
        except Exception as e:
            logger.error(f"Connection error: {e}")

    async def handle_controller(self, websocket, data):
        controller_id = str(uuid.uuid4())
        self.controllers.add(websocket)
        
        logger.info(f"Controller connected: {controller_id}")
        
        # Send welcome with current nodes
        await websocket.send(json.dumps({
            'type': 'welcome',
            'controller_id': controller_id,
            'message': 'Connected to Koyeb Relay',
            'nodes': list(self.node_info.values()),
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }))
        
        try:
            async for message in websocket:
                try:
                    cmd_data = json.loads(message)
                    await self.process_controller_command(controller_id, cmd_data)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        'type': 'error',
                        'message': 'Invalid JSON'
                    }))
        except Exception as e:
            logger.error(f"Controller error: {e}")
        finally:
            self.controllers.discard(websocket)
            logger.info(f"Controller disconnected: {controller_id}")

    async def handle_node(self, websocket, data):
        node_id = str(uuid.uuid4())
        node_name = data.get('node_name', f'node-{node_id[:8]}')
        
        self.nodes[node_id] = websocket
        self.node_info[node_id] = {
            'node_id': node_id,
            'node_name': node_name,
            'computer_name': data.get('computer_name', 'Unknown'),
            'status': 'online',
            'connected_at': datetime.utcnow().isoformat() + 'Z',
            'last_heartbeat': datetime.utcnow().isoformat() + 'Z'
        }
        
        logger.info(f"Node registered: {node_name}")
        
        # Notify all controllers
        await self.broadcast_to_controllers({
            'type': 'node_connected',
            'node': self.node_info[node_id]
        })
        
        # Send confirmation to node
        await websocket.send(json.dumps({
            'type': 'registered',
            'node_id': node_id,
            'message': 'Successfully registered with Koyeb relay'
        }))
        
        try:
            async for message in websocket:
                try:
                    msg_data = json.loads(message)
                    await self.process_node_message(node_id, msg_data)
                except json.JSONDecodeError:
                    pass  # Ignore invalid JSON from nodes
                    
        except Exception as e:
            logger.error(f"Node error {node_name}: {e}")
        finally:
            await self.cleanup_node(node_id)

    async def process_controller_command(self, controller_id, data):
        if data.get('type') == 'command':
            node_name = data.get('node_name')
            command = data.get('command')
            command_id = data.get('command_id', str(uuid.uuid4()))
            
            logger.info(f"Command from {controller_id} to {node_name}: {command}")
            
            # Find target node
            target_node_id = None
            for node_id, info in self.node_info.items():
                if info['node_name'] == node_name:
                    target_node_id = node_id
                    break
            
            if target_node_id and target_node_id in self.nodes:
                await self.nodes[target_node_id].send(json.dumps({
                    'type': 'command',
                    'command': command,
                    'command_id': command_id,
                    'from_controller': controller_id
                }))
                
                # Send queued confirmation
                await self.send_to_controller(controller_id, {
                    'type': 'command_queued',
                    'command_id': command_id,
                    'node_name': node_name
                })
            else:
                await self.send_to_controller(controller_id, {
                    'type': 'error',
                    'command_id': command_id,
                    'message': f'Node {node_name} not found'
                })

    async def process_node_message(self, node_id, data):
        if data.get('type') == 'heartbeat':
            # Update heartbeat
            if node_id in self.node_info:
                self.node_info[node_id]['last_heartbeat'] = datetime.utcnow().isoformat() + 'Z'
                
        elif data.get('type') == 'command_response':
            # Forward response to controller
            controller_id = data.get('controller_id')
            await self.send_to_controller(controller_id, {
                'type': 'command_response',
                'node_name': self.node_info.get(node_id, {}).get('node_name'),
                'response': data.get('response', {}),
                'command_id': data.get('command_id'),
                'success': data.get('success', True)
            })

    async def send_to_controller(self, controller_id, message):
        """Send message to specific controller"""
        for controller in self.controllers:
            try:
                # We'd need to track controller IDs better in production
                await controller.send(json.dumps(message))
                break
            except:
                continue

    async def broadcast_to_controllers(self, message):
        """Broadcast message to all controllers"""
        disconnected = []
        for controller in self.controllers:
            try:
                await controller.send(json.dumps(message))
            except:
                disconnected.append(controller)
        
        for controller in disconnected:
            self.controllers.discard(controller)

    async def cleanup_node(self, node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]
            
        if node_id in self.node_info:
            node_name = self.node_info[node_id]['node_name']
            del self.node_info[node_id]
            logger.info(f"Node disconnected: {node_name}")
            
            await self.broadcast_to_controllers({
                'type': 'node_disconnected',
                'node_name': node_name
            })

async def main():
    # Koyeb provides PORT environment variable
    port = int(os.getenv('PORT', 8000))
    
    relay = KoyebRelay()
    
    # Start WebSocket server
    server = await websockets.serve(
        relay.handle_connection,
        "0.0.0.0",  # Important: Listen on all interfaces
        port,
        ping_interval=20,
        ping_timeout=10,
        max_size=10 * 1024 * 1024  # 10MB max message size
    )
    
    logger.info(f"🚀 Koyeb Relay Server running on port {port}")
    logger.info(f"📡 WebSocket URL: wss://your-app-name.koyeb.app")
    logger.info("💚 Ready for controllers and nodes!")
    
    # Run forever
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {e}")
        raise
