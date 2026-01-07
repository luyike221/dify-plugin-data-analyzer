"""
Configuration module for DeepAnalyze API Server
Contains all configuration constants and environment setup
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# 优先加载 .env 文件，确保 .env 中的配置优先级最高
# 在容器环境中，.env 文件位于 /app/.env（通过 volumes 挂载）
# override=True 表示 .env 文件中的值会覆盖已存在的环境变量（包括 docker-compose 传入的）
env_paths = [
    Path("/app/.env"),  # 容器内路径（docker-compose 挂载）
    Path(__file__).parent.parent.parent / ".env",  # 从 core/ 向上找到插件根目录的 .env
    Path(__file__).parent.parent / ".env",  # 插件根目录
    Path(".env"),  # 当前目录
]

for env_path in env_paths:
    if env_path.exists():
        load_dotenv(env_path, override=True)
        print(f"✅ 已加载 .env 文件: {env_path}")
        break
else:
    print("⚠️  未找到 .env 文件，使用环境变量和默认值")

# Environment setup
os.environ.setdefault("MPLBACKEND", "Agg")

# API Configuration
# 注意：数据分析 API 配置现在从 provider credentials 获取，不再从环境变量读取
# 这些配置是必选的，必须在 provider 中配置
# VLLM_API_URL 和 MODEL_PATH 已移除，使用传入的参数代替
# Workspace directory relative to plugin root
WORKSPACE_BASE_DIR = os.path.join(Path(__file__).parent.parent, "workspace")

# API Configuration (kept for reference, not used in plugin mode)
# API_HOST = "0.0.0.0"
# API_PORT = 8200
# API_TITLE = "DeepAnalyze OpenAI-Compatible API"
# API_VERSION = "1.0.0"

# Dify Plugin Daemon Configuration
DIFY_PLUGIN_DAEMON_VERSION = "0.5.2"  # 最新发行版

# Thread cleanup configuration
CLEANUP_TIMEOUT_HOURS = 12
CLEANUP_INTERVAL_MINUTES = 30

# Code execution configuration
CODE_EXECUTION_TIMEOUT = 120
MAX_NEW_TOKENS = 32768

# Stream output configuration (流式输出配置)
STREAM_BUFFER_SIZE = 0  # 缓冲区大小，0表示无限制
STREAM_TIMEOUT_SECONDS = 60  # 流式输出超时时间（秒）
STREAM_MAX_ANALYSIS_ROUNDS = 10  # 最大分析轮次，防止无限循环

# File handling configuration
FILE_STORAGE_DIR = os.path.join(WORKSPACE_BASE_DIR, "_files")
VALID_FILE_PURPOSES = ["fine-tune", "answers", "file-extract", "assistants"]

# Model configuration
DEFAULT_TEMPERATURE = 0.4
DEFAULT_MODEL = "DeepAnalyze-8B"

# Stop token IDs for DeepAnalyze model
STOP_TOKEN_IDS = [151676, 151645]

# Supported tools
SUPPORTED_TOOLS = ["code_interpreter"]

# Excel processing configuration
EXCEL_VALID_EXTENSIONS = ['.xlsx', '.xls', '.xlsm', '.xlsb']
EXCEL_MAX_FILE_SIZE_MB = 100  # 最大文件大小（MB）
EXCEL_MAX_ROWS_PREVIEW = 10   # 表头分析预览行数（默认值，可通过provider配置覆盖）
EXCEL_MAX_COLS_PREVIEW = 5    # 表头分析预览列数（默认值，可通过provider配置覆盖）

# LLM for header analysis (可选，用于智能表头分析)
EXCEL_LLM_API_KEY = os.environ.get("EXCEL_LLM_API_KEY", "")
EXCEL_LLM_BASE_URL = os.environ.get("EXCEL_LLM_BASE_URL", "https://api.openai.com/v1/chat/completions")
EXCEL_LLM_MODEL = os.environ.get("EXCEL_LLM_MODEL", "gpt-4o-mini")

# Default analysis prompt
DEFAULT_EXCEL_ANALYSIS_PROMPT = """请对上传的数据进行全面分析，包括：
1. 数据概览：基本统计信息、数据类型分布
2. 数据质量：缺失值、异常值检测
3. 描述性统计：数值列的统计指标
4. 洞察与建议：基于数据分析的发现和建议

请生成一份完整的数据分析报告。

**重要：请使用中文进行所有分析、代码注释和报告撰写。**
**禁止：请不要生成任何图表绘制代码（matplotlib、plotly、seaborn 等），专注于数据分析和统计计算。**"""