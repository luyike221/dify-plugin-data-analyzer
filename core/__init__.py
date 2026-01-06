"""
DeepAnalyze API Package
OpenAI-compatible API server for DeepAnalyze model
"""

__version__ = "1.0.0"
__title__ = "DeepAnalyze OpenAI-Compatible API"

# Main module no longer exports create_app or main
# All functions are directly importable from their respective modules

__all__ = ["create_app", "main"]