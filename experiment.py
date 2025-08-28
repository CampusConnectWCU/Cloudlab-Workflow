#!/usr/bin/env python3
"""
Simple Experiment Management for Cloudlab CI/CD

This module provides basic experiment management functionality
for Cloudlab experiments without complex secret management.
"""

import logging
import os
import sys
import time
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s:%(name)s:%(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

class ExperimentManager:
    """
    Simple experiment manager for Cloudlab experiments.
    """
    
    def __init__(self, experiment_name: str, profile_name: str):
        """
        Initialize the experiment manager.
        
        Args:
            experiment_name: Name of the experiment
            profile_name: Name of the profile to use
        """
        self.experiment_name = experiment_name
        self.profile_name = profile_name
        self.log_dir = "/local/logs"
        
    def setup_logging(self) -> bool:
        """
        Set up logging directory for the experiment.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            os.makedirs(self.log_dir, exist_ok=True)
            log.info(f"Logging directory ready: {self.log_dir}")
            return True
        except Exception as e:
            log.error(f"Failed to create logging directory: {e}")
            return False
    
    def get_experiment_info(self) -> Dict[str, Any]:
        """
        Get basic experiment information.
        
        Returns:
            Dictionary with experiment details
        """
        return {
            "experiment_name": self.experiment_name,
            "profile_name": self.profile_name,
            "log_directory": self.log_dir,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def log_experiment_start(self) -> None:
        """Log the start of the experiment."""
        info = self.get_experiment_info()
        log.info("=== Experiment Started ===")
        for key, value in info.items():
            log.info(f"{key}: {value}")
    
    def log_experiment_complete(self, success: bool = True) -> None:
        """
        Log the completion of the experiment.
        
        Args:
            success: Whether the experiment completed successfully
        """
        status = "SUCCESS" if success else "FAILED"
        log.info(f"=== Experiment {status} ===")
        log.info(f"Completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Example usage of the ExperimentManager."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Simple Cloudlab experiment manager")
    parser.add_argument("--experiment", required=True, help="Experiment name")
    parser.add_argument("--profile", required=True, help="Profile name")
    parser.add_argument("--action", choices=["start", "info"], default="info", help="Action to perform")
    
    args = parser.parse_args()
    
    # Create experiment manager
    manager = ExperimentManager(args.experiment, args.profile)
    
    if args.action == "start":
        manager.setup_logging()
        manager.log_experiment_start()
        log.info("Experiment started successfully")
        manager.log_experiment_complete(success=True)
    elif args.action == "info":
        info = manager.get_experiment_info()
        print("Experiment Information:")
        for key, value in info.items():
            print(f"  {key}: {value}")

if __name__ == "__main__":
    main() 