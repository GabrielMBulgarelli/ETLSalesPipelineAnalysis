"""AWS runtime package for the ecommerce sales ETL pipeline."""

from .config import AwsEtlConfig, load_config

__all__ = ["AwsEtlConfig", "load_config"]
__version__ = "0.1.0"
