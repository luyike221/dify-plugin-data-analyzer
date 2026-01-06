"""
Dify Plugin Tool Implementation for Excel Data Analyzer
Integrates core analysis functionality into Dify plugin tool interface
"""
import os
import requests
import logging
from collections.abc import Generator
from typing import Any, Optional
from pathlib import Path

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

# Import core functionality
from core.excel_analyze_api import analyze_excel, analyze_excel_stream
from core.config import DEFAULT_EXCEL_ANALYSIS_PROMPT

# 配置日志
logger = logging.getLogger(__name__)

# 配置日志系统（如果还没有配置）
# 检查根 logger 是否有 handler，如果没有则配置
root_logger = logging.getLogger()
if not root_logger.handlers:
    # 配置基础日志
    logging.basicConfig(
        level=logging.INFO,  # 默认 INFO 级别
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# 确保我们的 logger 有足够的级别
logger.setLevel(logging.DEBUG)

# 如果 logger 还没有 handler，添加一个控制台 handler
if not logger.handlers:
    # 创建控制台 handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    
    # 创建格式器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # 添加 handler 到 logger
    logger.addHandler(console_handler)
    
    # 允许日志向上传播（这样可以通过根 logger 统一管理）
    logger.propagate = True

# 测试日志输出（仅在开发时）
logger.debug("Logger 初始化完成，日志系统已配置")


class DifyPluginDataAnalyzerTool(Tool):
    """Excel智能分析工具"""
    
    def _is_dify_file(self, obj: Any) -> bool:
        """
        检查对象是否为 Dify File 对象
        
        根据 Dify 官方文档，文件对象包含以下属性：
        - url: 文件的预览/下载 URL (带签名)
        - filename: 文件名
        - mime_type: MIME 类型
        - extension: 文件扩展名
        - size: 文件大小
        - type: 文件类型
        """
        if obj is None:
            logger.debug("_is_dify_file: 对象为 None")
            return False
        
        # 检查是否有 url 属性（Dify File 对象的标准属性）
        if hasattr(obj, "url") and hasattr(obj, "filename"):
            logger.info("✅ 通过 url 和 filename 属性识别为 Dify File 对象")
            return True
        
        # 检查类型名称（备用方法）
        type_str = str(type(obj))
        logger.debug(f"_is_dify_file: 对象类型字符串: {type_str}")
        
        if "dify_plugin" in type_str and "File" in type_str:
            logger.info(f"✅ 通过类型字符串识别为 Dify File 对象: {type_str}")
            return True
        
        # 检查类名（备用方法）
        if hasattr(obj, "__class__"):
            class_name = obj.__class__.__name__
            module_name = obj.__class__.__module__
            logger.debug(f"_is_dify_file: 类名={class_name}, 模块名={module_name}")
            
            if class_name == "File":
                if "dify_plugin" in module_name:
                    logger.info(f"✅ 通过类名识别为 Dify File 对象: {module_name}.{class_name}")
                    return True
        
        logger.debug(f"_is_dify_file: 不是 Dify File 对象")
        return False
    
    def _get_file_from_dify_file(self, dify_file: Any, api_key: Optional[str] = None) -> tuple[bytes, str]:
        """
        从 Dify File 对象获取文件内容和文件名
        
        根据 Dify 官方文档，文件对象包含以下属性：
        - url: 文件的预览/下载 URL (带签名，可能是相对路径)
        - filename: 文件名
        - mime_type: MIME 类型
        - extension: 文件扩展名
        - size: 文件大小
        - type: 文件类型
        
        文件对象没有直接的 blob 属性，需要通过 url 下载内容。
        
        参数:
            dify_file: Dify File 对象
            api_key: Dify API Key（如果需要通过 API 下载，通常不需要）
        
        返回:
            (file_content: bytes, filename: str)
        """
        logger.info("=" * 60)
        logger.info("🚀 开始处理 Dify File 对象")
        logger.info(f"📦 File 对象类型: {type(dify_file)}")
        logger.info(f"📋 File 对象属性列表: {[attr for attr in dir(dify_file) if not attr.startswith('_')]}")
        
        # 检查并记录文件对象的属性
        if hasattr(dify_file, "url"):
            logger.info(f"🌐 url 属性: {dify_file.url}")
        if hasattr(dify_file, "filename"):
            logger.info(f"📄 filename 属性: {dify_file.filename}")
        if hasattr(dify_file, "mime_type"):
            logger.info(f"📋 mime_type 属性: {dify_file.mime_type}")
        if hasattr(dify_file, "extension"):
            logger.info(f"📎 extension 属性: {dify_file.extension}")
        if hasattr(dify_file, "size"):
            logger.info(f"📦 size 属性: {dify_file.size}")
        
        file_content = None
        filename = "uploaded_file.xlsx"
        method_used = None
        
        # 方法1: 通过 url 属性下载文件（根据 Dify 官方文档，这是标准方法）
        logger.info("")
        logger.info("━━━ 通过 url 属性下载文件 ━━━")
        if hasattr(dify_file, "url"):
            url = dify_file.url
            logger.info(f"🌐 文件 URL: {url}")
            
            # 检查 URL 是否为相对路径，如果是，需要构建完整 URL
            if url.startswith("http://") or url.startswith("https://"):
                full_url = url
                logger.info("✅ URL 是绝对路径，直接使用")
            else:
                # 相对路径，需要加上基础 URL
                # 尝试从环境变量获取 FILES_URL 或 DIFY_API_BASE_URL
                files_base_url = os.environ.get("FILES_URL") or os.environ.get("DIFY_API_BASE_URL")
                if files_base_url:
                    if not files_base_url.startswith("http"):
                        files_base_url = f"https://{files_base_url}"
                    # 移除末尾的斜杠
                    files_base_url = files_base_url.rstrip("/")
                    # 确保 url 以斜杠开头
                    if not url.startswith("/"):
                        url = "/" + url
                    full_url = f"{files_base_url}{url}"
                    logger.info(f"🔧 URL 是相对路径，构建完整 URL: {full_url}")
                else:
                    full_url = url
                    logger.warning("⚠️ URL 是相对路径，但未配置 FILES_URL 或 DIFY_API_BASE_URL，尝试直接使用")
            
            try:
                logger.info("📡 发送 HTTP GET 请求下载文件...")
                response = requests.get(full_url, timeout=30)
                response.raise_for_status()
                file_content = response.content
                logger.info("✅✅✅ 成功: 从 URL 下载文件，文件大小: %d 字节", len(file_content))
                method_used = f"URL download ({full_url})"
            except Exception as e:
                logger.error("❌❌❌ 失败: 从 URL 下载文件失败: %s", str(e))
                logger.debug("异常详情:", exc_info=True)
                file_content = None
        else:
            logger.error("❌ 对象没有 url 属性，无法下载文件")
            file_content = None
        
        # 获取文件名（优先使用 filename 属性，这是 Dify File 对象的标准属性）
        logger.info("")
        logger.info("🔍 尝试获取文件名...")
        if hasattr(dify_file, "filename"):
            filename = dify_file.filename
            logger.info(f"✅ 从 filename 属性获取: {filename}")
        elif hasattr(dify_file, "name"):
            filename = os.path.basename(dify_file.name)
            logger.info(f"✅ 从 name 属性获取: {filename}")
        elif hasattr(dify_file, "file_name"):
            filename = dify_file.file_name
            logger.info(f"✅ 从 file_name 属性获取: {filename}")
        elif hasattr(dify_file, "original_filename"):
            filename = dify_file.original_filename
            logger.info(f"✅ 从 original_filename 属性获取: {filename}")
        else:
            logger.warning(f"⚠️ 无法获取文件名，使用默认值: {filename}")
        
        # 如果文件名没有扩展名，尝试从 extension 属性获取
        if hasattr(dify_file, "extension") and dify_file.extension:
            if not filename.endswith(f".{dify_file.extension}"):
                filename = f"{filename}.{dify_file.extension}"
                logger.info(f"📎 添加扩展名: {filename}")
        
        # 总结
        logger.info("")
        logger.info("=" * 60)
        logger.info("📊 处理结果总结")
        logger.info("=" * 60)
        if file_content is not None:
            logger.info("")
            logger.info("🎉🎉🎉 文件获取成功！🎉🎉🎉")
            logger.info(f"")
            logger.info(f"   ✅ 最终使用的方法: {method_used}")
            logger.info(f"   📄 文件名: {filename}")
            logger.info(f"   📦 文件大小: {len(file_content)} 字节")
            logger.info("")
        else:
            logger.error("")
            logger.error("❌❌❌ 无法获取文件内容 ❌❌❌")
            logger.error("")
            logger.error("失败原因:")
            logger.error("  - 文件对象缺少 url 属性，或 URL 下载失败")
            logger.error("  - 请检查文件对象是否正确传递")
            logger.error("")
        logger.info("=" * 60)
        
        return file_content, filename
    
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        执行Excel数据分析 - 流式输出版本
        
        使用 create_stream_variable_message 实现实时流式输出，
        在 Chatflow 的 Answer 节点中引用 stream_output 变量可获得打字机效果。
        
        参数:
        - input_file: Excel文件（必填）
        - query: 可选的分析查询语句或提示词
        - use_llm_header_validation: 是否使用LLM验证表头（默认true）
        - thread_id: 可选的会话ID，用于复用已有会话
        """
        input_file = tool_parameters.get("input_file")
        query = tool_parameters.get("query", "")
        use_llm_header_validation = tool_parameters.get("use_llm_header_validation", True)
        thread_id = tool_parameters.get("thread_id")  # 从工具参数获取会话ID（由Dify生成并传入）
        
        # 从 provider credentials 获取配置
        llm_api_key = None
        llm_base_url = None
        llm_model = None
        analysis_api_url = None
        analysis_model = None
        analysis_api_key = None
        
        # 尝试多种方式获取 credentials
        credentials = None
        
        if hasattr(self, 'runtime') and hasattr(self.runtime, 'credentials'):
            credentials = self.runtime.credentials
        
        if not credentials and hasattr(self, 'runtime') and hasattr(self.runtime, 'provider_credentials'):
            credentials = self.runtime.provider_credentials
        
        if not credentials and hasattr(self, 'runtime') and hasattr(self.runtime, 'get_credentials'):
            try:
                credentials = self.runtime.get_credentials()
            except Exception:
                pass
        
        if credentials:
            llm_api_key = credentials.get("llm_api_key") or os.environ.get("EXCEL_LLM_API_KEY")
            llm_base_url = credentials.get("llm_base_url") or os.environ.get("EXCEL_LLM_BASE_URL", "https://api.openai.com/v1/chat/completions")
            llm_model = credentials.get("llm_model") or os.environ.get("EXCEL_LLM_MODEL", "gpt-4o-mini")
            analysis_api_url = credentials.get("analysis_api_url") or os.environ.get("ANALYSIS_API_URL")
            analysis_model = credentials.get("analysis_model") or os.environ.get("ANALYSIS_MODEL")
            analysis_api_key = credentials.get("analysis_api_key") or os.environ.get("ANALYSIS_API_KEY")
        else:
            llm_api_key = os.environ.get("EXCEL_LLM_API_KEY")
            llm_base_url = os.environ.get("EXCEL_LLM_BASE_URL", "https://api.openai.com/v1/chat/completions")
            llm_model = os.environ.get("EXCEL_LLM_MODEL", "gpt-4o-mini")
            analysis_api_url = os.environ.get("ANALYSIS_API_URL")
            analysis_model = os.environ.get("ANALYSIS_MODEL")
            analysis_api_key = os.environ.get("ANALYSIS_API_KEY")
        
        # 验证必选配置
        if not analysis_api_url:
            error_msg = (
                "❌ **错误: 缺少必选配置 'analysis_api_url'**\n\n"
                "请在 Dify 插件管理中配置 Analysis API URL。"
            )
            yield self.create_stream_variable_message('stream_output', error_msg)
            return
        
        if not analysis_model:
            error_msg = (
                "❌ **错误: 缺少必选配置 'analysis_model'**\n\n"
                "请在 Dify 插件管理中配置 Analysis Model。"
            )
            yield self.create_stream_variable_message('stream_output', error_msg)
            return
        
        use_llm_validate = use_llm_header_validation and bool(llm_api_key)
        
        if not input_file:
            yield self.create_stream_variable_message('stream_output', "❌ 错误: 缺少文件参数，请上传Excel文件\n")
            return
        
        try:
            # === 流式输出：开始处理 ===
            yield self.create_stream_variable_message('stream_output', "🚀 **开始处理Excel文件...**\n\n")
            
            # 处理文件参数
            file_content = None
            filename = None
            
            logger.info("🔍 检查输入文件类型...")
            
            if self._is_dify_file(input_file):
                yield self.create_stream_variable_message('stream_output', "📥 正在获取上传的文件...\n")
                
                # 获取 Dify API Key
                dify_api_key = None
                if hasattr(self, 'runtime'):
                    if hasattr(self.runtime, 'api_key'):
                        dify_api_key = self.runtime.api_key
                    elif hasattr(self.runtime, 'dify_api_key'):
                        dify_api_key = self.runtime.dify_api_key
                
                if not dify_api_key and credentials:
                    dify_api_key = credentials.get("dify_api_key") or credentials.get("api_key")
                
                if not dify_api_key:
                    dify_api_key = os.environ.get("DIFY_API_KEY")
                
                try:
                    file_content, filename = self._get_file_from_dify_file(input_file, dify_api_key)
                    if file_content is None:
                        yield self.create_stream_variable_message('stream_output', "❌ 无法从 Dify File 对象获取文件内容\n")
                        return
                    yield self.create_stream_variable_message('stream_output', f"✅ 文件获取成功: {filename} ({len(file_content)/1024:.1f} KB)\n\n")
                except Exception as e:
                    yield self.create_stream_variable_message('stream_output', f"❌ 处理文件时出错: {str(e)}\n")
                    return
                    
            elif isinstance(input_file, str):
                if os.path.exists(input_file):
                    with open(input_file, "rb") as f:
                        file_content = f.read()
                    filename = os.path.basename(input_file)
                    yield self.create_stream_variable_message('stream_output', f"✅ 读取本地文件: {filename}\n\n")
                else:
                    yield self.create_stream_variable_message('stream_output', f"❌ 文件不存在: {input_file}\n")
                    return
                    
            elif hasattr(input_file, "read"):
                file_content = input_file.read()
                filename = getattr(input_file, "filename", "uploaded_file.xlsx")
                if hasattr(input_file, "name"):
                    filename = os.path.basename(input_file.name)
                yield self.create_stream_variable_message('stream_output', f"✅ 读取文件对象: {filename}\n\n")
                
            elif isinstance(input_file, dict):
                if "path" in input_file:
                    file_path = input_file["path"]
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as f:
                            file_content = f.read()
                        filename = os.path.basename(file_path)
                        yield self.create_stream_variable_message('stream_output', f"✅ 读取文件: {filename}\n\n")
                    else:
                        yield self.create_stream_variable_message('stream_output', f"❌ 文件不存在: {file_path}\n")
                        return
                elif "content" in input_file:
                    file_content = input_file["content"]
                    if isinstance(file_content, str):
                        file_content = file_content.encode("utf-8")
                    filename = input_file.get("filename", "uploaded_file.xlsx")
                else:
                    yield self.create_stream_variable_message('stream_output', "❌ 无法从文件参数中提取内容\n")
                    return
            else:
                yield self.create_stream_variable_message('stream_output', f"❌ 不支持的文件类型: {type(input_file)}\n")
                return
            
            if not file_content:
                yield self.create_stream_variable_message('stream_output', "❌ 无法读取文件内容\n")
                return
            
            if not filename:
                filename = "uploaded_file.xlsx"
            
            analysis_prompt = query if query else DEFAULT_EXCEL_ANALYSIS_PROMPT
            
            # 处理会话ID：从工具参数获取（由Dify生成并传入）
            # 如果提供了thread_id，使用它；否则创建新会话（静默处理，不输出）
            if thread_id and thread_id.strip():
                thread_id = thread_id.strip()
                final_thread_id = thread_id  # 使用提供的会话ID
            else:
                # 未提供会话ID，插件内部创建新会话
                thread_id = None
                final_thread_id = None  # 将在创建后获取
            
            # 用于从流式输出中提取插件内部创建的会话ID（如果Dify未提供会话ID）
            import re
            thread_id_pattern = re.compile(r'(?:会话ID|Session ID)[:：]\s*(thread-[a-f0-9]{24})', re.IGNORECASE)
            
            # === 核心：使用流式分析函数 ===
            # 直接使用同步 Generator 并逐块输出
            for chunk in analyze_excel_stream(
                file_content=file_content,
                filename=filename,
                analysis_api_url=analysis_api_url,
                analysis_model=analysis_model,
                thread_id=thread_id,  # 传递会话ID（来自Dify的conversation_id或插件内部创建）
                use_llm_validate=use_llm_validate,
                sheet_name=None,
                auto_analysis=True,
                analysis_prompt=analysis_prompt,
                temperature=0.4,
                llm_api_key=llm_api_key,
                llm_base_url=llm_base_url,
                llm_model=llm_model,
                analysis_api_key=analysis_api_key
            ):
                # 流式输出每个块
                yield self.create_stream_variable_message('stream_output', chunk)
                
                # 如果Dify未提供会话ID，尝试从输出中提取插件内部创建的会话ID
                if not final_thread_id and not thread_id:
                    match = thread_id_pattern.search(chunk)
                    if match:
                        final_thread_id = match.group(1)
                        # 立即输出会话ID变量，方便后续引用
                        yield self.create_variable_message('thread_id', final_thread_id)
            
            # 输出会话ID变量
            if final_thread_id:
                # 插件内部创建的会话ID
                yield self.create_variable_message('thread_id', final_thread_id)
            elif thread_id:
                # Dify提供的会话ID
                yield self.create_variable_message('thread_id', thread_id)
            
        except Exception as e:
            import traceback
            error_msg = f"❌ **处理过程出错**\n\n```\n{str(e)}\n{traceback.format_exc()}\n```\n"
            yield self.create_stream_variable_message('stream_output', error_msg)
