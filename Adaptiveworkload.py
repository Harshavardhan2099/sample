from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import logging
import boto3
from botocore.config import Config

app = Flask(__name__)

# Configuration
THRESHOLD_T1 = 5  # RPS - Switch to medium
THRESHOLD_T2 = 10  # RPS - Switch to large
HYSTERESIS = 2  # Buffer to prevent flapping
COOLDOWN_PERIOD = 10  # Seconds between scaling actions
REQUEST_LOG_WINDOW = 60  # Seconds for rate calculation

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS Resources
INSTANCE_GROUPS = {
    'small': {'arn': 'arn:aws:elasticloadbalancing:ap-south-1:975050195505:loadbalancer/app/my-alb/12253abf01a627b6', 'ids': ['i-0c57481c98da9fe7e']},
    'medium': {'arn': 'arn:aws:elasticloadbalancing:ap-south-1:975050195505:loadbalancer/app/my-alb/12253abf01a627b6', 'ids': ['i-0adefdc2a894f3c7b']},
    'large': {'arn': 'arn:aws:elasticloadbalancing:ap-south-1:975050195505:loadbalancer/app/my-alb/12253abf01a627b6', 'ids': ['i-0b8e3663a1e20b934']}
}

REGION = 'ap-south-1'

# AWS Client with retries
session = boto3.Session()
ec2_config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'adaptive'
    }
)
ec2 = session.client('ec2', region_name=REGION, config=ec2_config)

# Global state
REQUEST_LOG = []
LAST_SCALING_TIME = None

def manage_instances(operation, instance_ids):
    """Start/stop instances with cooldown and state checks"""
    global LAST_SCALING_TIME

    try:
        # Check current state
        response = ec2.describe_instances(InstanceIds=instance_ids)
        current_state = response['Reservations'][0]['Instances'][0]['State']['Name']

        # Skip if already in desired state
        if (operation == "start" and current_state == "running") or \
           (operation == "stop" and current_state == "stopped"):
            logger.info(f"Instances {instance_ids} already {current_state}")
            return

        # Enforce cooldown
        if LAST_SCALING_TIME and (datetime.now() - LAST_SCALING_TIME) < timedelta(seconds=COOLDOWN_PERIOD):
            logger.warning("Scaling skipped: Cooldown active")
            return

        # Execute operation
        if operation == "start":
            ec2.start_instances(InstanceIds=instance_ids)
            logger.info(f"Started instances: {instance_ids}")
        elif operation == "stop":
            ec2.stop_instances(InstanceIds=instance_ids)
            logger.info(f"Stopped instances: {instance_ids}")

        LAST_SCALING_TIME = datetime.now()

    except Exception as e:
        logger.error(f"AWS API error: {str(e)}")
        raise

def calculate_arrival_rate():
    """Calculate RPS over time window"""
    global REQUEST_LOG
    now = datetime.now()

    # Prune old entries
    REQUEST_LOG = [
        entry for entry in REQUEST_LOG
        if (now - entry['timestamp']).total_seconds() <= REQUEST_LOG_WINDOW
    ]

    if len(REQUEST_LOG) < 2:
        return 0.0

    time_span = (REQUEST_LOG[-1]['timestamp'] - REQUEST_LOG[0]['timestamp']).total_seconds()
    return len(REQUEST_LOG) / time_span if time_span > 0 else 0.0

def choose_target_group(arrival_rate):
    """Hysteresis-based selection"""
    if arrival_rate <= THRESHOLD_T1 - HYSTERESIS:
        return 'small'
    elif arrival_rate <= THRESHOLD_T2 - HYSTERESIS:
        return 'medium'
    else:
        return 'large'

@app.route('/send_request', methods=['POST'])
def handle_request():
    """Main request handler"""
    try:
        REQUEST_LOG.append({'timestamp': datetime.now()})
        arrival_rate = calculate_arrival_rate()
        target_group = choose_target_group(arrival_rate)

        # Control instances directly
        for group in INSTANCE_GROUPS:
            if group == target_group:
                manage_instances('start', INSTANCE_GROUPS[group]['ids'])
            else:
                manage_instances('stop', INSTANCE_GROUPS[group]['ids'])

        return jsonify({
            "status": "success",
            "target_group": target_group,
            "arrival_rate": round(arrival_rate, 2)
        })

    except Exception as e:
        logger.error(f"Request handling failed: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/get_metrics', methods=['GET'])
def get_metrics():
    """Instance health check"""
    active_groups = []
    for group in INSTANCE_GROUPS:
        try:
            response = ec2.describe_instances(InstanceIds=INSTANCE_GROUPS[group]['ids'])
            state = response['Reservations'][0]['Instances'][0]['State']['Name']
            if state == 'running':
                active_groups.append(group)
        except Exception as e:
            logger.error(f"Status check failed for {group}: {str(e)}")

    return jsonify({
        "active_groups": active_groups,
        "arrival_rate": round(calculate_arrival_rate(), 2)
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)