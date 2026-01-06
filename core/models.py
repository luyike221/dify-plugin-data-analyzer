"""
Data models for DeepAnalyze API Server
Contains all Pydantic models for OpenAI compatibility
"""

from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field


class FileObject(BaseModel):
    """OpenAI File Object"""
    id: str
    object: Literal["file"] = "file"
    bytes: int
    created_at: int
    filename: str
    purpose: str


class FileDeleteResponse(BaseModel):
    """OpenAI File Delete Response"""
    id: str
    object: Literal["file"] = "file"
    deleted: bool




class ThreadObject(BaseModel):
    """OpenAI Thread Object"""
    id: str
    object: Literal["thread"] = "thread"
    created_at: int
    last_accessed_at: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    file_ids: List[str] = Field(default_factory=list)
    tool_resources: Optional[Dict[str, Any]] = Field(default=None)


class MessageObject(BaseModel):
    """OpenAI Message Object"""
    id: str
    object: Literal["thread.message"] = "thread.message"
    created_at: int
    thread_id: str
    role: Literal["user", "assistant"]
    content: List[Dict[str, Any]]
    file_ids: List[str] = Field(default_factory=list)
    assistant_id: Optional[str] = None
    run_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ChatCompletionRequest(BaseModel):
    """Chat completion request model"""
    model: str
    messages: List[Dict[str, Any]]
    file_ids: Optional[List[str]] = Field(default=None)
    temperature: Optional[float] = Field(0.4)
    stream: Optional[bool] = Field(False)


class FileInfo(BaseModel):
    """File information model for OpenAI compatibility"""
    filename: str
    url: str


class ChatCompletionChoice(BaseModel):
    """Chat completion choice model"""
    index: int
    message: Dict[str, Any]
    finish_reason: Optional[str] = None


class ChatCompletionResponse(BaseModel):
    """Chat completion response model"""
    id: str
    object: Literal["chat.completion"] = "chat.completion"
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    generated_files: Optional[List[Dict[str, str]]] = Field(default=None)
    attached_files: Optional[List[str]] = Field(default=None)


class ChatCompletionChunk(BaseModel):
    """Chat completion streaming chunk model"""
    id: str
    object: Literal["chat.completion.chunk"] = "chat.completion.chunk"
    created: int
    model: str
    choices: List[Dict[str, Any]]
    generated_files: Optional[List[Dict[str, str]]] = Field(default=None)


class HealthResponse(BaseModel):
    """Health check response model"""
    status: str
    timestamp: int


class ThreadCleanupRequest(BaseModel):
    """Thread cleanup request model"""
    timeout_hours: int = Field(12, description="Timeout in hours for thread cleanup")


class ThreadCleanupResponse(BaseModel):
    """Thread cleanup response model"""
    status: str
    cleaned_threads: int
    timeout_hours: int
    timestamp: int


class ThreadStatsResponse(BaseModel):
    """Thread statistics response model"""
    total_threads: int
    recent_threads: int  # < 1 hour
    old_threads: int     # 1-12 hours
    expired_threads: int # > 12 hours
    timeout_hours: int
    timestamp: int


class ModelObject(BaseModel):
    """OpenAI Model Object"""
    id: str
    object: Literal["model"] = "model"
    created: Optional[int] = None
    owned_by: Optional[str] = None


class ModelsListResponse(BaseModel):
    """OpenAI Models List Response"""
    object: Literal["list"] = "list"
    data: List[ModelObject]


# ============ Excel分析相关模型 ============

class HeaderAnalysisResponse(BaseModel):
    """表头分析结果"""
    skip_rows: int = Field(description="需要跳过的无效行数")
    header_rows: int = Field(description="表头占用的行数")
    header_type: str = Field(description="表头类型: single或multi")
    data_start_row: int = Field(description="数据开始行(1-indexed)")
    confidence: str = Field(description="置信度: high/medium/low")
    reason: str = Field(description="分析原因说明")


class ProcessedFileInfo(BaseModel):
    """处理后的文件信息"""
    file_id: Optional[str] = None
    filename: str
    url: str
    size_bytes: Optional[int] = None


class ExcelAnalyzeRequest(BaseModel):
    """Excel分析请求模型（用于JSON请求体）"""
    thread_id: Optional[str] = Field(None, description="会话ID，不提供则创建新会话")
    use_llm_header: bool = Field(False, description="是否使用LLM分析表头")
    llm_api_key: Optional[str] = Field(None, description="LLM API密钥")
    llm_base_url: Optional[str] = Field(None, description="LLM API地址")
    llm_model: Optional[str] = Field(None, description="LLM模型名称")
    sheet_name: Optional[str] = Field(None, description="工作表名称")
    auto_analysis: bool = Field(True, description="是否自动进行数据分析")
    analysis_prompt: Optional[str] = Field(None, description="自定义分析提示词")
    stream: bool = Field(False, description="是否流式返回分析结果")


class ExcelAnalyzeResponse(BaseModel):
    """Excel分析响应模型"""
    thread_id: str = Field(description="会话ID")
    status: str = Field(description="处理状态: success/error")
    header_analysis: Optional[HeaderAnalysisResponse] = Field(None, description="表头分析结果")
    processed_file: Optional[ProcessedFileInfo] = Field(None, description="处理后的文件信息")
    metadata_file: Optional[ProcessedFileInfo] = Field(None, description="元数据文件信息")
    data_summary: Optional[Dict[str, Any]] = Field(None, description="数据摘要")
    column_metadata: Optional[Dict[str, Dict]] = Field(None, description="列结构元数据")
    analysis_result: Optional[Dict[str, Any]] = Field(None, description="分析结果")
    error_message: Optional[str] = Field(None, description="错误信息")
    available_sheets: Optional[List[str]] = Field(None, description="可用工作表列表")


class ExcelSheetsResponse(BaseModel):
    """获取工作表列表响应"""
    filename: str
    sheets: List[str]
    default_sheet: str