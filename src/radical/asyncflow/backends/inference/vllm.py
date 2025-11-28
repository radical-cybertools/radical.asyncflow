"""
Dragon VLLM Inference Service
Service that runs on HPC compute nodes and is directly accessible
"""
import copy
import yaml
import time
import asyncio
import multiprocessing as mp
from typing import List, Dict, Any
import socket
import logging
from aiohttp import web
import threading
import functools

try:
    import dragon
    from ml_inference.dragon_inference_utils import DragonInference
except ImportError:  # pragma: no cover - environment without Dragon
    dragon = None
    DragonInference = None


logger = logging.getLogger(__name__)


class DragonVllmInferenceBackend:
    """
    DragonVllmInferenceBackend: VLLM inference service for HPC environments.

    Can operate in two modes:
    1. Service Mode (use_service=True): HTTP API enabled for remote access
    2. Direct Mode (use_service=False): Direct pipeline access only

    Key Features:
        - Async initialization with concurrent multi-service support
        - HTTP API for remote inference requests (REST + curl compatible) [Service Mode]
        - Direct Python API for in-process calls [Both Modes]
        - Node partitioning via offset parameter for multi-service deployments
        - Automatic GPU allocation and tensor parallelism support
        - Built-in health monitoring and request tracking

    Architecture:
        - Uses Dragon multiprocessing for distributed execution
        - aiohttp for non-blocking HTTP serving [Service Mode]
        - Thread pool execution for pipeline initialization
        - Queue-based communication with inference workers

    Usage Example (Service Mode):
        >>> service = DragonVllmInferenceBackend(
        ...     config_file="config.yaml",
        ...     model_name="/path/to/model",
        ...     num_nodes=1,
        ...     num_gpus=2,
        ...     port=8000,
        ...     use_service=True  # HTTP enabled
        ... )
        >>> await service.initialize()
        >>> # Access via HTTP from AsyncFlow tasks

    Usage Example (Direct Mode):
        >>> engine = DragonVllmInferenceBackend(
        ...     config_file="config.yaml",
        ...     model_name="/path/to/model",
        ...     num_nodes=1,
        ...     num_gpus=2,
        ...     use_service=False  # No HTTP
        ... )
        >>> await engine.initialize()
        >>> results = await engine.generate(["What is AI?"])  # Direct call

    HTTP Endpoints [Service Mode Only]:
        GET  /health   - Service health check and status
        POST /generate - Batch inference endpoint
            Request:  {"prompts": [...], "timeout": 300}
            Response: {"status": "success", "results": [...], "hostname": "..."}

    Parameters:
        config_file (str): Path to Dragon VLLM configuration YAML
        model_name (str): Path to the model weights/checkpoint
        offset (int): Node offset in allocation for partitioning (default: 0)
        num_nodes (int): Number of nodes to use (default: 1)
        num_gpus (int): Number of GPUs per node (default: 1)
        tp_size (int): Tensor parallel size (default: 1)
        port (int): HTTP server port (default: 8000) [Service Mode]
        use_service (bool): Enable HTTP server (default: True)

    Attributes:
        is_initialized (bool): Whether service is ready for inference
        hostname (str): Hostname where service is running
        pipeline (DragonInference): Underlying VLLM inference pipeline
        use_service (bool): Whether HTTP service is enabled
        
    Methods:
        initialize(): Async initialization - blocks until ready
        generate(prompts): Direct Python API for inference
        shutdown(): Graceful shutdown of service and resources
        get_endpoint(): Returns full HTTP endpoint URL [Service Mode]

    Notes:
        - Designed for HPC environments with Dragon runtime
        - Supports concurrent initialization of multiple services
        - Thread-safe for concurrent request handling
        - Requires Dragon multiprocessing: mp.set_start_method("dragon")
        - Service blocks on initialize() until pipeline is loaded and HTTP server is ready
    """

    def __init__(self, config_file: str, model_name: str, offset: int = 0,
                 num_nodes: int = 1, num_gpus: int = 1, tp_size: int = 1,
                 port: int = 8000, use_service: bool = True):

        if dragon is None:
            raise ImportError("Dragon is required for DragonVllmInferenceBackend.")
        
        if DragonInference is None:
            raise ImportError("DragonVllm is required for DragonVllmInferenceBackend.")

        self.config_file = config_file
        self.model_name = model_name
        self.num_nodes = num_nodes
        self.num_gpus = num_gpus
        self.tp_size = tp_size
        self.port = port
        self.offset = offset
        self.use_service = use_service

        self.inference_pipeline = None
        self.input_queue = None
        self.response_queue = None
        self.is_initialized = False
        self.hostname = socket.gethostname()
        
        # HTTP components (only used if use_service=True)
        self.app = None
        self.runner = None
        self.site = None
        self._server_task = None

    def update_config(self, base_config):
        """Update config with custom parameters"""
        config = copy.deepcopy(base_config)
        config['required']['model_name'] = self.model_name
        config['hardware']['num_nodes'] = self.num_nodes
        config['hardware']['num_gpus'] = self.num_gpus
        config['required']['tp_size'] = self.tp_size
        config['input_batching']['toggle_on'] = True
        config['input_batching']['type'] = 'pre-batch'
        config['guardrails']['toggle_on'] = False
        config['dynamic_inf_wrkr']['toggle_on'] = False
        return config

    async def initialize(self):
        """
        Initialize the VLLM pipeline and optionally HTTP server
        Blocks until service is ready
        """
        if self.is_initialized:
            logger.warning("Service already initialized")
            return self

        mode = "service" if self.use_service else "engine"
        logger.info(f"Initializing VLLM {mode} on {self.hostname}...")

        # Load config
        with open(self.config_file, 'r') as f:
            base_config = yaml.safe_load(f)

        config = self.update_config(base_config)

        # Create queues
        self.input_queue = mp.Queue()
        self.response_queue = mp.Queue()

        # Initialize pipeline
        logger.info("Initializing VLLM pipeline...")

        def _init_pipeline():
            self.inference_pipeline = DragonInference(config, self.num_nodes, self.offset, self.input_queue)
            self.inference_pipeline.initialize()

        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _init_pipeline)

        logger.info("VLLM pipeline initialized!")

        # Start HTTP server only if use_service=True
        if self.use_service:
            await self._start_http_server()
            logger.info(f"Service ready at http://{self.hostname}:{self.port}")
            logger.info(f"GET  http://{self.hostname}:{self.port}/health")
            logger.info(f"POST http://{self.hostname}:{self.port}/generate")
        else:
            logger.info("Engine ready (no HTTP server)")
            logger.info("Use engine.generate() for direct pipeline access")

        self.is_initialized = True
        return self
    
    async def _start_http_server(self):
        """Start the HTTP server (only called if use_service=True)"""
        self.app = web.Application()

        # Add routes
        self.app.router.add_get('/health', self._handle_health)
        self.app.router.add_post('/generate', self._handle_generate)

        # Start server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.hostname, self.port)
        await self.site.start()

        logger.info(f"HTTP server started on {self.hostname}:{self.port}")

    async def generate(self, prompts: List[str], timeout: int = 300) -> List[str]:
        """
        Generate responses for given prompts
        Works in both service and engine mode
        """
        if not self.is_initialized:
            raise RuntimeError("Not initialized. Call await initialize() first.")

        logger.info(f"Processing {len(prompts)} prompts...")
        start_time = time.time()
        
        # Send prompts to the pipeline
        self.inference_pipeline.query((prompts, self.response_queue))

        loop = asyncio.get_event_loop()
        results = []

        # Use executor to call blocking get() in separate thread
        for i in range(len(prompts)):
            get_func = functools.partial(self.response_queue.get, timeout=timeout)
            response = await loop.run_in_executor(None, get_func)

            # Extract text from response
            if isinstance(response, dict) and 'text' in response:
                text = response['text']
            elif isinstance(response, tuple) and len(response) > 0:
                text = str(response[0])
            else:
                text = str(response)

            results.append(text)
            logger.debug(f"Completed prompt {i+1}/{len(prompts)}")

        end_time = time.time()
        logger.info(f"Completed {len(prompts)} prompts in {end_time - start_time:.2f}s")
        
        return results
    
    async def shutdown(self):
        """Shutdown the service/engine"""
        if not self.is_initialized:
            return

        mode = "service" if self.use_service else "engine"
        logger.info(f"Shutting down {mode}...")
        
        # Stop HTTP server (only if use_service=True)
        if self.use_service:
            if self.site:
                await self.site.stop()
            if self.runner:
                await self.runner.cleanup()

        # Shutdown pipeline
        if self.inference_pipeline:
            self.inference_pipeline.destroy()
        if self.input_queue:
            self.input_queue.close()
        if self.response_queue:
            self.response_queue.close()

        self.is_initialized = False
        logger.info(f"{mode.capitalize()} shutdown complete")
    
    # HTTP Handlers (only used if use_service=True)
    async def _handle_health(self, request):
        """Health check endpoint"""
        return web.json_response({
            "status": "healthy",
            "hostname": self.hostname,
            "initialized": self.is_initialized,
            "endpoint": f"http://{self.hostname}:{self.port}",
            "mode": "service"
        })

    async def _handle_generate(self, request):
        """Generate responses via HTTP"""
        try:
            data = await request.json()
            prompts = data.get('prompts', [])
            timeout = data.get('timeout', 300)

            if not prompts:
                return web.json_response({
                    "status": "error",
                    "message": "No prompts provided"
                }, status=400)

            start_time = time.time()
            results = await self.generate(prompts, timeout)
            end_time = time.time()

            return web.json_response({
                "status": "success",
                "hostname": self.hostname,
                "results": results,
                "num_prompts": len(prompts),
                "total_time": end_time - start_time,
                "avg_time_per_prompt": (end_time - start_time) / len(prompts)
            })
            
        except Exception as e:
            logger.error(f"Generation error: {e}", exc_info=True)
            return web.json_response({
                "status": "error",
                "message": str(e),
                "hostname": self.hostname
            }, status=500)

    def get_endpoint(self):
        """
        Get the service endpoint URL
        Returns None if use_service=False
        """
        if not self.use_service:
            logger.warning("Engine mode: No HTTP endpoint available")
            return None
        return f"http://{self.hostname}:{self.port}"

    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.shutdown()
