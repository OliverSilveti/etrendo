
import os
import yaml
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    """Loads the configuration from config.yaml"""
    try:
        with open("config.yaml", "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("config.yaml not found.")
        raise
    except Exception as e:
        logger.error(f"Error loading config.yaml: {e}")
        raise

def load_prompts():
    """Loads the prompts from prompts.yaml"""
    try:
        with open("prompts.yaml", "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("prompts.yaml not found.")
        raise
    except Exception as e:
        logger.error(f"Error loading prompts.yaml: {e}")
        raise

# Load instances
config = load_config()
prompts = load_prompts()
