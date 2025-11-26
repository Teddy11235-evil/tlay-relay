#!/usr/bin/env python3
"""
Koyeb-Compatible Relay Server with HTTP Health Checks
"""
import asyncio
import websockets
import json
import logging
import os
from datetime import datetime
import uuid
from aiohttp import web

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("KoyebRelay")

class KoyebRelay:
    def __init__(self):
        self.controllers = set()
        self.nodes = {}
        self.node_info = {}
        
    async def handle_websocket(self, websocket, path):
        """Handle WebSocket connections"""
        client_ip = websocket.remote_address[0] if websocket.remote_address else 'unknown'
        logger.info(f"WebSocket connection from {client_ip}")
        
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
            logger.warning(f"WebSocket timeout from {client_ip}")
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON from {client_ip}")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")

    async def handle_controller(self, websocket, data):
        """Handle controller connection"""
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
        """Handle node connection"""
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
        """Process commands from controllers"""
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
        """Process messages from nodes"""
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
        """Clean up node resources"""
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

# HTTP routes for health checks
async def health_check(request):
    """Handle Koyeb health checks"""
    return web.Response(text="OK", status=200)

async def status(request):
    """Status page with relay information"""
    relay = request.app['relay']
    status_info = {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'stats': {
            'controllers_connected': len(relay.controllers),
            'nodes_connected': len(relay.nodes),
            'total_nodes_registered': len(relay.node_info)
        },
        'nodes': list(relay.node_info.values())
    }
    return web.json_response(status_info)

async def websocket_handler(request):
    """Handle WebSocket upgrade requests"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    relay = request.app['relay']
    await relay.handle_websocket(ws, request.path)
    
    return ws

async def create_app():
    """Create aiohttp application"""
    app = web.Application()
    
    # Store relay instance in app
    relay = KoyebRelay()
    app['relay'] = relay
    
    # Add routes
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_get('/status', status)
    app.router.add_get('/ws', websocket_handler)  # WebSocket endpoint
    
    return app

async def main():
    """Main application entry point"""
    port = int(os.getenv('PORT', 8000))
    
    # Create aiohttp app
    app = await create_app()
    
    # Create aiohttp runner
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Create TCP site
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"🚀 Koyeb Relay Server running on port {port}")
    logger.info("📡 WebSocket URL: wss://your-app-name.koyeb.app/ws")
    logger.info("🏥 Health check: https://your-app-name.koyeb.app/health")
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
