#!/usr/bin/env python3
"""
Koyeb-Compatible Relay Server - Pure HTTP
"""
from aiohttp import web
import json
import logging
import os
from datetime import datetime
import uuid
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("KoyebRelay")

class KoyebRelay:
    def __init__(self):
        self.controllers = {}
        self.nodes = {}
        self.command_queues = {}
        self.responses = {}
        self.heartbeat_timeout = 60
        
    async def handle_register_controller(self, request):
        """Register a new controller"""
        try:
            controller_id = str(uuid.uuid4())
            
            self.controllers[controller_id] = {
                'id': controller_id,
                'registered_at': datetime.utcnow().isoformat() + 'Z',
                'last_seen': datetime.utcnow().isoformat() + 'Z'
            }
            
            logger.info(f"Controller registered: {controller_id}")
            
            return web.json_response({
                'controller_id': controller_id,
                'nodes': list(self.nodes.values()),
                'message': 'Controller registered successfully'
            })
            
        except Exception as e:
            logger.error(f"Controller registration error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def handle_register_node(self, request):
        """Register a new node"""
        try:
            data = await request.json()
            node_id = str(uuid.uuid4())
            node_name = data.get('node_name', f'node-{node_id[:8]}')
            
            self.nodes[node_id] = {
                'node_id': node_id,
                'node_name': node_name,
                'computer_name': data.get('computer_name', 'Unknown'),
                'status': 'online',
                'registered_at': datetime.utcnow().isoformat() + 'Z',
                'last_heartbeat': datetime.utcnow().isoformat() + 'Z'
            }
            
            self.command_queues[node_id] = []
            
            logger.info(f"Node registered: {node_name}")
            
            return web.json_response({
                'node_id': node_id,
                'message': 'Node registered successfully'
            })
            
        except Exception as e:
            logger.error(f"Node registration error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def handle_node_heartbeat(self, request):
        """Handle node heartbeat and return pending commands"""
        try:
            data = await request.json()
            node_id = data.get('node_id')
            
            if not node_id:
                return web.json_response({'error': 'node_id required'}, status=400)
            
            if node_id in self.nodes:
                self.nodes[node_id]['last_heartbeat'] = datetime.utcnow().isoformat() + 'Z'
                self.nodes[node_id]['status'] = 'online'
                
                commands = []
                if node_id in self.command_queues:
                    commands = self.command_queues[node_id].copy()
                    self.command_queues[node_id] = []
                
                logger.info(f"Heartbeat from {self.nodes[node_id]['node_name']} - {len(commands)} commands")
                
                return web.json_response({
                    'commands': commands,
                    'message': 'Heartbeat received'
                })
            else:
                return web.json_response({'error': 'Node not found'}, status=404)
                
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def handle_send_command(self, request):
        """Send command to a node"""
        try:
            data = await request.json()
            node_name = data.get('node_name')
            command = data.get('command')
            command_id = data.get('command_id', str(uuid.uuid4()))
            
            if not node_name or not command:
                return web.json_response({'error': 'node_name and command required'}, status=400)
            
            target_node_id = None
            for node_id, node_info in self.nodes.items():
                if node_info['node_name'] == node_name:
                    target_node_id = node_id
                    break
            
            if target_node_id:
                command_data = {
                    'command_id': command_id,
                    'command': command,
                    'sent_at': datetime.utcnow().isoformat() + 'Z'
                }
                
                self.command_queues[target_node_id].append(command_data)
                
                logger.info(f"Command queued for {node_name}: {command}")
                
                return web.json_response({
                    'command_id': command_id,
                    'status': 'queued',
                    'message': f'Command queued for {node_name}'
                })
            else:
                return web.json_response({
                    'error': f'Node {node_name} not found'
                }, status=404)
                
        except Exception as e:
            logger.error(f"Send command error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def handle_command_response(self, request):
        """Receive command response from node"""
        try:
            data = await request.json()
            command_id = data.get('command_id')
            response = data.get('response', {})
            
            if not command_id:
                return web.json_response({'error': 'command_id required'}, status=400)
            
            self.responses[command_id] = {
                'response': response,
                'received_at': datetime.utcnow().isoformat() + 'Z'
            }
            
            logger.info(f"Response received for command {command_id}")
            
            return web.json_response({'status': 'received'})
            
        except Exception as e:
            logger.error(f"Command response error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def handle_get_response(self, request):
        """Get command response"""
        try:
            command_id = request.query.get('command_id')
            
            if not command_id:
                return web.json_response({'error': 'command_id required'}, status=400)
            
            if command_id in self.responses:
                response = self.responses[command_id]
                return web.json_response(response)
            else:
                return web.json_response({
                    'error': 'Response not found'
                }, status=404)
                
        except Exception as e:
            logger.error(f"Get response error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def handle_list_nodes(self, request):
        """List all connected nodes"""
        try:
            current_time = datetime.utcnow()
            inactive_nodes = []
            
            for node_id, node_info in self.nodes.items():
                last_heartbeat = datetime.fromisoformat(node_info['last_heartbeat'].replace('Z', '+00:00'))
                if (current_time - last_heartbeat).total_seconds() > self.heartbeat_timeout:
                    inactive_nodes.append(node_id)
                    logger.info(f"Node marked offline: {node_info['node_name']}")
            
            for node_id in inactive_nodes:
                self.nodes[node_id]['status'] = 'offline'
            
            return web.json_response({
                'nodes': list(self.nodes.values()),
                'total': len(self.nodes),
                'online': len([n for n in self.nodes.values() if n.get('status') == 'online'])
            })
            
        except Exception as e:
            logger.error(f"List nodes error: {e}")
            return web.json_response({'error': str(e)}, status=400)
    
    async def handle_health(self, request):
        """Health check endpoint"""
        return web.json_response({
            'status': 'healthy',
            'service': 'render-farm-relay',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'stats': {
                'nodes_connected': len(self.nodes),
                'nodes_online': len([n for n in self.nodes.values() if n.get('status') == 'online']),
                'controllers_connected': len(self.controllers),
                'pending_commands': sum(len(q) for q in self.command_queues.values())
            }
        })
    
    async def handle_root(self, request):
        """Root endpoint"""
        return web.json_response({
            'message': 'Render Farm Relay Server',
            'status': 'running',
            'endpoints': {
                'GET /health': 'Health check',
                'POST /controller/register': 'Register controller',
                'POST /node/register': 'Register node',
                'POST /node/heartbeat': 'Node heartbeat',
                'POST /command/send': 'Send command',
                'POST /command/response': 'Submit response',
                'GET /command/response': 'Get response',
                'GET /nodes': 'List nodes'
            }
        })

async def create_app():
    """Create aiohttp application"""
    app = web.Application()
    relay = KoyebRelay()
    
    app['relay'] = relay
    
    # Add routes
    app.router.add_get('/', relay.handle_root)
    app.router.add_get('/health', relay.handle_health)
    app.router.add_post('/controller/register', relay.handle_register_controller)
    app.router.add_post('/node/register', relay.handle_register_node)
    app.router.add_post('/node/heartbeat', relay.handle_node_heartbeat)
    app.router.add_post('/command/send', relay.handle_send_command)
    app.router.add_post('/command/response', relay.handle_command_response)
    app.router.add_get('/command/response', relay.handle_get_response)
    app.router.add_get('/nodes', relay.handle_list_nodes)
    
    return app

async def main():
    """Main application entry point"""
    port = int(os.getenv('PORT', 8080))
    
    app = await create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"🚀 Koyeb Relay Server running on port {port}")
    logger.info("💚 Ready for controllers and nodes!")
    
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
