#!/usr/bin/env python3
"""
Simplified Node Initialization Script for Cloudlab CI/CD

This script automates the setup of Cloudlab nodes without requiring complex secrets.
It can be called by GitHub workflows from other repositories.

Usage:
    python init_node.py --ip <ip_address> --profile <profile_name> [options]

Example:
    python init_node.py --ip 192.168.1.100 --profile hello-world-cluster
"""

import logging
import sys
import os
import argparse
import time
import base64

# Add powder library path
sys.path.append(os.path.join(os.path.dirname(__file__), 'powder'))
try:
    import powder.ssh as pssh
except ImportError:
    print("Error: Could not import powder.ssh.", file=sys.stderr)
    sys.exit(1)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s:%(name)s:%(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# Exit Codes
EXIT_SUCCESS = 0
EXIT_SSH_ERROR = 1
EXIT_CMD_ERROR = 2
EXIT_ARG_ERROR = 3

def check_hostname(ip_address, username=None):
    """
    Connects to an existing node, retrieves and logs its hostname.
    Used when --isDeployed flag is set.
    """
    log.info(f"Node at {ip_address} is pre-existing. Verifying connectivity and getting hostname.")
    ssh_conn = None
    try:
        ssh_conn = pssh.SSHConnection(ip_address=ip_address, username=username)
        log.info("Opening SSH connection...")
        ssh_conn.open()
        log.info("SSH connection established.")

        hostname_cmd = "hostname -f"
        log.info(f"Executing: {hostname_cmd}")
        # Execute command and extract the last line of output as the hostname
        hostname_output = ssh_conn.command(hostname_cmd, timeout=30)
        hostname = hostname_output.strip().splitlines()[-1]
        log.info(f"Node hostname: {hostname}")
        log.info("Skipping installation and deployment steps as --isDeployed flag was provided.")
        return EXIT_SUCCESS

    except (ValueError, FileNotFoundError, ConnectionError, ConnectionRefusedError, 
            pssh.pexpect.exceptions.ExceptionPexpect, TimeoutError, ConnectionAbortedError) as e:
        log.error(f"Failed to connect or execute command on existing node {ip_address}: {e}", 
                 exc_info=log.isEnabledFor(logging.DEBUG))
        return EXIT_SSH_ERROR
    except Exception as e:
        log.error(f"An unexpected error occurred during hostname check: {e}", exc_info=True)
        return EXIT_SSH_ERROR
    finally:
        if ssh_conn:
            log.info("Closing SSH connection.")
            ssh_conn.close()

def initialize_node(ip_address, profile_name, username=None):
    """
    Performs first-time initialization of the node: installs dependencies
    and runs the main startup script in the background.
    """
    log.info(f"Starting initialization process for node at IP: {ip_address}")
    log.info(f"Using profile: {profile_name}")

    ssh_conn = None
    try:
        # Establish SSH connection
        log.info("Establishing SSH connection...")
        ssh_conn = pssh.SSHConnection(ip_address=ip_address, username=username)
        ssh_conn.open()
        log.info("SSH connection established successfully.")

        # Check if the startup script exists
        startup_script_path = f"/local/repository/{profile_name}/scripts/startup.sh"
        log.info(f"Checking for startup script at: {startup_script_path}")
        
        check_cmd = f"test -f {startup_script_path}"
        result = ssh_conn.command(check_cmd, timeout=30)
        
        if ssh_conn.ssh.exitstatus != 0:
            log.error(f"Startup script not found at {startup_script_path}")
            log.error("Please ensure the profile repository has been copied to the node")
            return EXIT_CMD_ERROR

        log.info("Startup script found. Executing in background...")
        
        # Execute the startup script in the background
        # The script will handle all the deployment steps
        startup_cmd = f"cd /local/repository/{profile_name} && nohup ./scripts/startup.sh > /local/logs/startup.log 2>&1 &"
        log.info(f"Executing startup command: {startup_cmd}")
        
        result = ssh_conn.command(startup_cmd, timeout=60)
        
        if ssh_conn.ssh.exitstatus != 0:
            log.error("Failed to execute startup script")
            return EXIT_CMD_ERROR

        log.info("Startup script executed successfully in background")
        log.info("Deployment is now running. Check logs at /local/logs/startup.log")
        
        return EXIT_SUCCESS

    except (ValueError, FileNotFoundError, ConnectionError, ConnectionRefusedError,
            pssh.pexpect.exceptions.ExceptionPexpect, TimeoutError, ConnectionAbortedError) as e:
        log.error(f"Failed to connect or execute command on node {ip_address}: {e}",
                 exc_info=log.isEnabledFor(logging.DEBUG))
        return EXIT_SSH_ERROR
    except Exception as e:
        log.error(f"An unexpected error occurred during node initialization: {e}", exc_info=True)
        return EXIT_SSH_ERROR
    finally:
        if ssh_conn:
            log.info("Closing SSH connection.")
            ssh_conn.close()

def main():
    """Main function to parse arguments and execute node initialization."""
    parser = argparse.ArgumentParser(
        description="Initialize a Cloudlab node for deployment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python init_node.py --ip 192.168.1.100 --profile hello-world-cluster
  python init_node.py --ip 192.168.1.100 --profile hello-world-cluster --isDeployed
  python init_node.py --ip 192.168.1.100 --profile hello-world-cluster --username ubuntu
        """
    )
    
    parser.add_argument(
        '--ip', '--ip-address',
        required=True,
        help='IP address of the node to initialize'
    )
    
    parser.add_argument(
        '--profile', '--profile-name',
        required=True,
        help='Name of the profile/directory containing the startup scripts'
    )
    
    parser.add_argument(
        '--username',
        help='SSH username (defaults to current user)'
    )
    
    parser.add_argument(
        '--isDeployed',
        action='store_true',
        help='Skip initialization if node is already deployed (just check hostname)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=== Cloudlab Node Initialization Started ===")
    log.info(f"Target IP: {args.ip}")
    log.info(f"Profile: {args.profile}")
    log.info(f"Username: {args.username or 'current user'}")
    log.info(f"Mode: {'Check only' if args.isDeployed else 'Full initialization'}")

    try:
        if args.isDeployed:
            # Just check connectivity and get hostname
            exit_code = check_hostname(args.ip, args.username)
        else:
            # Perform full initialization
            exit_code = initialize_node(args.ip, args.profile, args.username)

        if exit_code == EXIT_SUCCESS:
            log.info("=== Node initialization completed successfully ===")
        else:
            log.error(f"=== Node initialization failed with exit code {exit_code} ===")

        sys.exit(exit_code)

    except KeyboardInterrupt:
        log.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        log.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 