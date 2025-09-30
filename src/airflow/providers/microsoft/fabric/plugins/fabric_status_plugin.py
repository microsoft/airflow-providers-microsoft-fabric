"""
Microsoft Fabric AKS Status Plugin for Apache Airflow

This plugin displays information about the underlying AKS system hosting Airflow.
Accessible at: /fabric-status

Shows a table with all pods containing:
- Namespace
- Pod Name  
- Status
- Active Since
- Node
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Blueprint, render_template_string
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access

logger = logging.getLogger(__name__)

# Check for Kubernetes library availability at module load time
try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException
    KUBERNETES_AVAILABLE = True
    logger.info("Kubernetes library is available")
except ImportError as e:
    KUBERNETES_AVAILABLE = False
    client = None
    config = None  
    ApiException = Exception
    logger.warning(f"Kubernetes library not available: {e}. Plugin will show error message.")

# Simple HTML template that should work without issues
FABRIC_STATUS_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Fabric AKS Status</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .error { color: red; padding: 10px; background: #ffe6e6; border-radius: 4px; }
        .success { color: green; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; table-layout: auto; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; word-wrap: break-word; }
        th { background-color: #f2f2f2; font-weight: bold; }
        .status-running { color: green; font-weight: bold; }
        .status-pending { color: orange; font-weight: bold; }
        .status-failed { color: red; font-weight: bold; }
        .status-succeeded { color: blue; font-weight: bold; }
        .status-unknown { color: gray; font-weight: bold; }
        .diagnostic-info { font-size: 0.9em; color: #666; }
        .diagnostic-column { width: 400px; min-width: 400px; max-width: 400px; }

        .resource-usage { font-size: 0.85em; color: #555; margin-top: 2px; }
        .container-list { font-size: 0.8em; color: #666; margin-top: 2px; font-style: italic; }
    </style>
</head>
<body>
    <h1>Microsoft Fabric AKS Status</h1>
    
    {% if error_message %}
    <div class="error">Error: {{ error_message }}</div>
    {% endif %}
    
    <p>Last updated: {{ last_updated }}</p>
    <button onclick="location.reload();">Refresh</button>
    
    {% if pods %}
    <table>
        <tr>
            <th>Namespace</th>
            <th>Node</th>
            <th>Pod Name</th>
            <th>Status</th>
            <th>Active Since</th>
            <th>CPU/Memory (per pod)</th>
            <th>Diagnostics</th>
        </tr>
        {% for pod in pods %}
        <tr>
            <td>{{ pod.namespace }}</td>
            <td>{{ pod.node_name or '-' }}</td>
            <td>
                <div>{{ pod.name }}</div>
                {% if pod.container_names %}
                <div class="container-list">
                    {{ pod.container_names|join(', ') }}
                </div>
                {% endif %}
            </td>
            <td><span class="status-{{ pod.status.lower() }}">{{ pod.status }}</span></td>
            <td>{{ pod.active_since_formatted }}</td>
            <td>
                {% if pod.cpu_usage or pod.memory_usage %}
                <div class="resource-usage">
                    <div>CPU: {{ pod.cpu_usage or 'N/A' }}</div>
                    <div>MEM: {{ pod.memory_usage or 'N/A' }}</div>
                </div>
                {% else %}
                N/A
                {% endif %}
            </td>
            <td class="diagnostic-column">
                {% if pod.diagnostic_info %}
                <span class="diagnostic-info">{{ pod.diagnostic_info }}</span>
                {% else %}
                -
                {% endif %}
            </td>
        </tr>
        {% endfor %}
    </table>
    <p>Total: {{ pods|length }} pods</p>
    {% else %}
    <p>No pod information available.</p>
    {% endif %}
</body>
</html>
"""


def format_time_ago(start_time: Optional[datetime]) -> str:
    """Format time difference as human-readable string (e.g., '6d', '3h ago')"""
    if not start_time:
        return "Unknown"
    
    now = datetime.now(start_time.tzinfo)
    diff = now - start_time
    
    days = diff.days
    hours, remainder = divmod(diff.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    if days > 0:
        if hours > 0:
            return f"{days}d {hours}h ago"
        return f"{days}d ago"
    elif hours > 0:
        if minutes > 30:  # Show minutes if significant
            return f"{hours}h {minutes}m ago"
        return f"{hours}h ago"
    elif minutes > 0:
        return f"{minutes}m ago"
    else:
        return "Just now"


def format_resource_usage(value: Optional[str], unit: str = "") -> str:
    """Format resource usage values"""
    if not value:
        return "-"
    
    try:
        # Handle different units (m for millicores, Ki/Mi/Gi for memory)
        if unit == "cpu":
            if value.endswith('m'):
                # Millicores
                millis = int(value[:-1])
                if millis >= 1000:
                    return f"{millis/1000:.1f}"
                return f"{millis}m"
            elif value.endswith('n'):
                # Nanocores
                nanos = int(value[:-1])
                return f"{nanos/1000000:.1f}m"
            else:
                # Cores
                return f"{float(value):.2f}"
        elif unit == "memory":
            if value.endswith('Ki'):
                kb = int(value[:-2])
                if kb >= 1024 * 1024:
                    return f"{kb/(1024*1024):.1f}Gi"
                elif kb >= 1024:
                    return f"{kb/1024:.1f}Mi"
                return f"{kb}Ki"
            elif value.endswith('Mi'):
                mb = int(value[:-2])
                if mb >= 1024:
                    return f"{mb/1024:.1f}Gi"
                return f"{mb}Mi"
            elif value.endswith('Gi'):
                return value
            else:
                # Bytes
                bytes_val = int(value)
                if bytes_val >= 1024**3:
                    return f"{bytes_val/(1024**3):.1f}Gi"
                elif bytes_val >= 1024**2:
                    return f"{bytes_val/(1024**2):.1f}Mi"
                elif bytes_val >= 1024:
                    return f"{bytes_val/1024:.1f}Ki"
                return f"{bytes_val}B"
        
        return value
    except (ValueError, AttributeError):
        return value or "-"


class PodInfo:
    """Simple data class to hold basic pod information"""
    
    def __init__(self, namespace: str, name: str, status: str, active_since_formatted: str, 
                 node_name: Optional[str] = None, diagnostic_info: Optional[str] = None, 
                 container_names: Optional[List[str]] = None, cpu_usage: Optional[str] = None,
                 memory_usage: Optional[str] = None):
        self.namespace = namespace
        self.name = name
        self.status = status
        self.active_since_formatted = active_since_formatted
        self.node_name = node_name or "Unknown"
        self.diagnostic_info = diagnostic_info
        self.container_names = container_names or []
        self.cpu_usage = cpu_usage
        self.memory_usage = memory_usage


def get_pod_diagnostic_info(pod) -> str:
    """Extract concise diagnostic information for non-running pods"""
    diagnostic_parts = []
    
    # Check container statuses first for most specific issues
    if pod.status.container_statuses:
        for container_status in pod.status.container_statuses:
            if not container_status.ready:
                # Check waiting state
                if container_status.state and container_status.state.waiting:
                    waiting = container_status.state.waiting
                    if waiting.reason:
                        # Provide concise error details
                        if waiting.reason == "ImagePullBackOff":
                            diagnostic_parts.append(f"[IMG] {container_status.name}: ImagePullBackOff")
                        elif waiting.reason == "ErrImagePull":
                            diagnostic_parts.append(f"[IMG] {container_status.name}: ErrImagePull")
                        elif waiting.reason == "ContainerCreating":
                            diagnostic_parts.append(f"[WAIT] {container_status.name}: Creating")
                        elif waiting.reason == "PodInitializing":
                            diagnostic_parts.append("[INIT] Pod initializing")
                        elif waiting.reason == "CreateContainerConfigError":
                            diagnostic_parts.append(f"[CFG] {container_status.name}: Config error")
                        else:
                            diagnostic_parts.append(f"{container_status.name}: {waiting.reason}")
                
                # Check terminated state
                elif container_status.state and container_status.state.terminated:
                    terminated = container_status.state.terminated
                    if terminated.reason:
                        exit_code = terminated.exit_code or "?"
                        if terminated.reason == "Error":
                            diagnostic_parts.append(f"[FAIL] {container_status.name}: Error (exit {exit_code})")
                        elif terminated.reason == "OOMKilled":
                            diagnostic_parts.append(f"[MEM] {container_status.name}: OOMKilled")
                        elif terminated.reason == "Completed":
                            diagnostic_parts.append(f"[OK] {container_status.name}: Completed")
                        else:
                            diagnostic_parts.append(f"{container_status.name}: {terminated.reason} (exit {exit_code})")
    
    # Check init container statuses
    if pod.status.init_container_statuses:
        for init_container in pod.status.init_container_statuses:
            if not init_container.ready and init_container.state and init_container.state.waiting:
                waiting = init_container.state.waiting
                if waiting.reason:
                    diagnostic_parts.append(f"[INIT] {init_container.name}: {waiting.reason}")
    
    # Check for scheduling issues
    if pod.status.phase == "Pending":
        for condition in (pod.status.conditions or []):
            if condition.type == "PodScheduled" and condition.status == "False" and condition.reason == "Unschedulable":
                if condition.message:
                    msg = condition.message.lower()
                    if "insufficient cpu" in msg:
                        diagnostic_parts.append("[CPU] Insufficient CPU")
                    elif "insufficient memory" in msg:
                        diagnostic_parts.append("[MEM] Insufficient memory")
                    elif "insufficient nvidia.com/gpu" in msg or "insufficient amd.com/gpu" in msg:
                        diagnostic_parts.append("[GPU] Insufficient GPU")
                    elif "no nodes available" in msg:
                        diagnostic_parts.append("[NODE] No nodes available")
                    elif "node(s) had taints" in msg:
                        diagnostic_parts.append("[TAINT] Node taints")
                    elif "didn't match node selector" in msg or "node(s) didn't match pod affinity" in msg:
                        diagnostic_parts.append("[SELECT] Node selector mismatch")
                    elif "node(s) had volume node affinity conflict" in msg:
                        diagnostic_parts.append("[VOL] Volume affinity conflict")
                    else:
                        diagnostic_parts.append("[SCHED] Unschedulable")
                else:
                    diagnostic_parts.append("[SCHED] Unschedulable")
    
    # Join and limit the total length - prioritize first 2-3 most important issues
    result = " | ".join(diagnostic_parts[:3])  # Take first 3 issues for conciseness
    return result[:200] + "..." if len(result) > 200 else result


def parse_memory_value(memory_str: str) -> int:
    """Parse Kubernetes memory string to bytes"""
    if not memory_str:
        return 0
    
    memory_str = memory_str.strip()
    
    # Handle different units
    if memory_str.endswith('Ki'):
        return int(memory_str[:-2]) * 1024
    elif memory_str.endswith('Mi'):
        return int(memory_str[:-2]) * 1024 * 1024
    elif memory_str.endswith('Gi'):
        return int(memory_str[:-2]) * 1024 * 1024 * 1024
    elif memory_str.endswith('Ti'):
        return int(memory_str[:-2]) * 1024 * 1024 * 1024 * 1024
    elif memory_str.endswith('K'):
        return int(memory_str[:-1]) * 1000
    elif memory_str.endswith('M'):
        return int(memory_str[:-1]) * 1000 * 1000
    elif memory_str.endswith('G'):
        return int(memory_str[:-1]) * 1000 * 1000 * 1000
    elif memory_str.endswith('T'):
        return int(memory_str[:-1]) * 1000 * 1000 * 1000 * 1000
    else:
        # Assume bytes
        return int(memory_str)


def format_memory_bytes(bytes_val: int) -> str:
    """Format bytes to human readable string"""
    if bytes_val == 0:
        return "0"
    
    # Use binary units (Ki, Mi, Gi)
    if bytes_val >= 1024 * 1024 * 1024:
        return f"{bytes_val / (1024 * 1024 * 1024):.1f}Gi"
    elif bytes_val >= 1024 * 1024:
        return f"{bytes_val / (1024 * 1024):.1f}Mi"
    elif bytes_val >= 1024:
        return f"{bytes_val / 1024:.1f}Ki"
    else:
        return f"{bytes_val}B"


def get_kubernetes_pods():
    """Get pods directly without a class wrapper to avoid import issues"""
    if not KUBERNETES_AVAILABLE:
        raise RuntimeError("Kubernetes library is not available. Please install it with: pip install kubernetes>=18.20.0")
    
    # Import kubernetes modules within the function when available
    from kubernetes import client, config
    
    try:
        # Try to load in-cluster config first (when running inside k8s)
        config.load_incluster_config()
        logger.info("Using in-cluster Kubernetes configuration")
    except Exception:
        try:
            # Fall back to kubeconfig file (for local development)
            config.load_kube_config()
            logger.info("Using kubeconfig file for Kubernetes configuration")
        except Exception as e:
            logger.error(f"Failed to load Kubernetes configuration: {e}")
            raise RuntimeError(f"Could not load Kubernetes configuration: {e}")
    
    # Create client and get pods
    k8s_client = client.CoreV1Api()
    pod_list = k8s_client.list_pod_for_all_namespaces(watch=False)
    
    pods = []
    for pod in pod_list.items:
        namespace = pod.metadata.namespace
        name = pod.metadata.name
        status = pod.status.phase or "Unknown"
        
        # Simple active since calculation
        start_time = pod.status.start_time
        if start_time:
            active_since_formatted = format_time_ago(start_time)
        else:
            active_since_formatted = "Unknown"
        
        # Get node name
        node_name = pod.spec.node_name or "Unknown"
        
        # Extract container names
        container_names = []
        if pod.spec.containers:
            container_names = [container.name for container in pod.spec.containers]
        
        # Get CPU and memory usage from pod status (requests/limits)
        cpu_usage = None
        memory_usage = None
        
        if pod.spec.containers:
            total_cpu_requests = 0
            total_cpu_limits = 0
            total_memory_requests = 0
            total_memory_limits = 0
            
            for container in pod.spec.containers:
                if container.resources:
                    # CPU requests and limits
                    if container.resources.requests:
                        if 'cpu' in container.resources.requests:
                            cpu_req = container.resources.requests['cpu']
                            # Convert to millicores for consistent display
                            if cpu_req.endswith('m'):
                                total_cpu_requests += int(cpu_req[:-1])
                            else:
                                total_cpu_requests += int(float(cpu_req) * 1000)
                    
                    if container.resources.limits:
                        if 'cpu' in container.resources.limits:
                            cpu_lim = container.resources.limits['cpu']
                            if cpu_lim.endswith('m'):
                                total_cpu_limits += int(cpu_lim[:-1])
                            else:
                                total_cpu_limits += int(float(cpu_lim) * 1000)
                        
                        if 'memory' in container.resources.limits:
                            mem_lim = container.resources.limits['memory']
                            total_memory_limits += parse_memory_value(mem_lim)
                    
                    if container.resources.requests:
                        if 'memory' in container.resources.requests:
                            mem_req = container.resources.requests['memory']
                            total_memory_requests += parse_memory_value(mem_req)
            
            # Format CPU usage
            if total_cpu_requests > 0 or total_cpu_limits > 0:
                if total_cpu_requests > 0 and total_cpu_limits > 0:
                    cpu_usage = f"{total_cpu_requests}m/{total_cpu_limits}m"
                elif total_cpu_requests > 0:
                    cpu_usage = f"{total_cpu_requests}m/unlimited"
                else:
                    cpu_usage = f"0/{total_cpu_limits}m"
            
            # Format memory usage
            if total_memory_requests > 0 or total_memory_limits > 0:
                req_str = format_memory_bytes(total_memory_requests) if total_memory_requests > 0 else "0"
                lim_str = format_memory_bytes(total_memory_limits) if total_memory_limits > 0 else "unlimited"
                memory_usage = f"{req_str}/{lim_str}"
        
        # Get diagnostic information for non-running pods
        diagnostic_info = None
        if status.lower() not in ["running", "succeeded"]:
            diagnostic_info = get_pod_diagnostic_info(pod)
            # If no diagnostic info was found but pod is not running, add basic info
            if not diagnostic_info and status.lower() == "pending":
                diagnostic_info = "[WAIT] Pod: Pending"
            elif not diagnostic_info and status.lower() == "failed":
                diagnostic_info = "[FAIL] Pod: Failed"
            elif not diagnostic_info:
                diagnostic_info = f"[INFO] Pod: {status}"
        
        pod_info = PodInfo(
            namespace=namespace,
            name=name,
            status=status,
            active_since_formatted=active_since_formatted,
            node_name=node_name,
            diagnostic_info=diagnostic_info,
            container_names=container_names,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage,
        )
        pods.append(pod_info)
    
    # Sort pods by namespace, then by name
    pods.sort(key=lambda p: (p.namespace, p.name))
    return pods



# Create Flask Blueprint
fabric_status_bp = Blueprint(
    "fabric_status",
    __name__,
    url_prefix="/fabric-status",
)


@fabric_status_bp.route("/")
@has_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE),
    ]
)
def fabric_status_view():
    """
    Main view for the fabric status page
    """
    error_message = None
    pods = []
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Check if Kubernetes is available
    if not KUBERNETES_AVAILABLE:
        error_message = "Kubernetes library is not installed. Please install it with: pip install kubernetes>=18.20.0"
        logger.error(error_message)
    else:
        try:
            pods = get_kubernetes_pods()
            logger.info(f"Successfully fetched {len(pods)} pods")
            
        except Exception as e:
            error_message = f"Failed to fetch pod information: {str(e)}"
            logger.error(error_message, exc_info=True)  # Include stack trace
    
    return render_template_string(
        FABRIC_STATUS_TEMPLATE,
        pods=pods,
        error_message=error_message,
        last_updated=last_updated,
    )


class FabricStatusPlugin(AirflowPlugin):
    """
    Apache Airflow Plugin for Microsoft Fabric AKS Status
    """
    
    name = "fabric_status_plugin"
    flask_blueprints = [fabric_status_bp]
    
    # Add menu link (optional - requires Airflow 2.2+)
    menu_links = [
        {
            "name": "Fabric AKS Status",
            "href": "/fabric-status/",
            "category": "Admin",
        }
    ]


# Export the plugin class
__all__ = ["FabricStatusPlugin"] 