"""
Dify Plugin Tool Implementation for Excel Data Analyzer
Integrates core analysis functionality into Dify plugin tool interface
"""
import os
import asyncio
import requests
import logging
from collections.abc import Generator
from typing import Any, Optional
from pathlib import Path

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

# Import core functionality
from core.excel_analyze_api import analyze_excel
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
        执行Excel数据分析
        
        参数:
        - input_file: Excel文件（必填）
        - query: 可选的分析查询语句或提示词
        """
        input_file = tool_parameters.get("input_file")
        query = tool_parameters.get("query", "")
        use_llm_header_validation = tool_parameters.get("use_llm_header_validation", True)  # 默认 true
        
        # 从 provider credentials 获取配置（provider 是唯一输入源）
        llm_api_key = None
        llm_base_url = None
        llm_model = None
        analysis_api_url = None
        analysis_model = None
        analysis_api_key = None
        
        # 尝试多种方式获取 credentials
        credentials = None
        
        # 方式1: 从 runtime.credentials 获取（标准方式）
        if hasattr(self, 'runtime') and hasattr(self.runtime, 'credentials'):
            credentials = self.runtime.credentials
        
        # 方式2: 从 runtime.provider_credentials 获取（备用方式）
        if not credentials and hasattr(self, 'runtime') and hasattr(self.runtime, 'provider_credentials'):
            credentials = self.runtime.provider_credentials
        
        # 方式3: 从 runtime 的 get_credentials 方法获取（如果存在）
        if not credentials and hasattr(self, 'runtime') and hasattr(self.runtime, 'get_credentials'):
            try:
                credentials = self.runtime.get_credentials()
            except Exception:
                pass
        
        # 方式4: 从环境变量获取（用于本地调试，生产环境应使用 Dify UI 配置）
        if credentials:
            # 从 credentials 字典中获取配置
            llm_api_key = credentials.get("llm_api_key") or os.environ.get("EXCEL_LLM_API_KEY")
            llm_base_url = credentials.get("llm_base_url") or os.environ.get("EXCEL_LLM_BASE_URL", "https://api.openai.com/v1/chat/completions")
            llm_model = credentials.get("llm_model") or os.environ.get("EXCEL_LLM_MODEL", "gpt-4o-mini")
            analysis_api_url = credentials.get("analysis_api_url") or os.environ.get("ANALYSIS_API_URL")
            analysis_model = credentials.get("analysis_model") or os.environ.get("ANALYSIS_MODEL")
            analysis_api_key = credentials.get("analysis_api_key") or os.environ.get("ANALYSIS_API_KEY")
        else:
            # 如果没有 credentials，尝试从环境变量读取（仅用于调试）
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
                "**解决方法：**\n"
                "1. 在 Dify 管理界面中，进入 **插件管理** → 找到 **dify-plugin-data-analyzer** 插件\n"
                "2. 点击 **配置** 或 **设置凭据**\n"
                "3. 填写以下必填项：\n"
                "   - **Analysis API URL** (数据分析API地址): 例如 `http://localhost:8118/v1/chat/completions`\n"
                "   - **Analysis Model** (分析模型): 例如 `DeepAnalyze-8B`\n"
                "4. 可选配置（如果需要智能表头验证）：\n"
                "   - **LLM API Key**: OpenAI 兼容的 API 密钥\n"
                "   - **LLM Base URL**: LLM API 地址\n"
                "   - **LLM Model**: LLM 模型名称\n\n"
                "**注意：** Provider 凭据必须在 Dify UI 中配置，不能通过 .env 文件配置。"
            )
            yield self.create_text_message(error_msg)
            return
        
        if not analysis_model:
            error_msg = (
                "❌ **错误: 缺少必选配置 'analysis_model'**\n\n"
                "**解决方法：**\n"
                "1. 在 Dify 管理界面中，进入 **插件管理** → 找到 **dify-plugin-data-analyzer** 插件\n"
                "2. 点击 **配置** 或 **设置凭据**\n"
                "3. 填写 **Analysis Model** (分析模型名称)，例如：`DeepAnalyze-8B`\n\n"
                "**注意：** Provider 凭据必须在 Dify UI 中配置。"
            )
            yield self.create_text_message(error_msg)
            return
        
        # 决定是否使用 LLM 验证（需要同时满足：用户启用 + 提供了 API key）
        use_llm_validate = use_llm_header_validation and bool(llm_api_key)
        
        if not input_file:
            yield self.create_text_message("错误: 缺少文件参数，请上传Excel文件")
            return
        
        try:
            # 处理文件参数
            # Dify 插件中的文件参数可能是：Dify File 对象、文件路径字符串、文件对象或字典
            file_content = None
            filename = None
            
            # 首先检查是否为 Dify File 对象
            logger.info("🔍 检查输入文件类型...")
            logger.debug(f"输入文件类型: {type(input_file)}")
            
            if self._is_dify_file(input_file):
                logger.info("✅ 检测到 Dify File 对象，开始处理...")
                
                # 获取 Dify API Key（用于通过 API 下载文件）
                dify_api_key = None
                
                # 方式1: 从 runtime 获取（如果可用）
                if hasattr(self, 'runtime'):
                    logger.debug("尝试从 runtime 获取 API key...")
                    # 尝试从 runtime 获取 API key
                    if hasattr(self.runtime, 'api_key'):
                        dify_api_key = self.runtime.api_key
                        logger.info("✅ 从 runtime.api_key 获取 API key")
                    elif hasattr(self.runtime, 'dify_api_key'):
                        dify_api_key = self.runtime.dify_api_key
                        logger.info("✅ 从 runtime.dify_api_key 获取 API key")
                    elif hasattr(self.runtime, 'get_api_key'):
                        try:
                            dify_api_key = self.runtime.get_api_key()
                            logger.info("✅ 从 runtime.get_api_key() 获取 API key")
                        except Exception as e:
                            logger.debug(f"runtime.get_api_key() 失败: {e}")
                
                # 方式2: 从 credentials 获取
                if not dify_api_key and credentials:
                    logger.debug("尝试从 credentials 获取 API key...")
                    dify_api_key = credentials.get("dify_api_key") or credentials.get("api_key")
                    if dify_api_key:
                        logger.info("✅ 从 credentials 获取 API key")
                
                # 方式3: 从环境变量获取
                if not dify_api_key:
                    logger.debug("尝试从环境变量获取 API key...")
                    dify_api_key = os.environ.get("DIFY_API_KEY")
                    if dify_api_key:
                        logger.info("✅ 从环境变量 DIFY_API_KEY 获取 API key")
                
                if not dify_api_key:
                    logger.warning("⚠️ 未找到 Dify API Key，某些下载方法可能不可用")
                
                try:
                    file_content, filename = self._get_file_from_dify_file(input_file, dify_api_key)
                    if file_content is None:
                        error_msg = (
                            "❌ **错误: 无法从 Dify File 对象中获取文件内容**\n\n"
                            "**可能的原因：**\n"
                            "1. File 对象缺少 download() 或 read() 方法\n"
                            "2. 未配置 Dify API Key，无法通过 API 下载文件\n\n"
                            "**解决方法：**\n"
                            "1. 确保 Dify File 对象有 download() 或 read() 方法\n"
                            "2. 或者在环境变量中配置 DIFY_API_KEY\n"
                            "3. 或者在 Provider 凭据中配置 dify_api_key\n\n"
                            "**提示：** 请查看日志以获取详细的调试信息"
                        )
                        yield self.create_text_message(error_msg)
                        return
                    logger.info(f"✅ 成功获取文件: {filename} ({len(file_content)} 字节)")
                except Exception as e:
                    import traceback
                    logger.error(f"❌ 处理 Dify File 对象时出错: {str(e)}", exc_info=True)
                    error_msg = f"错误: 处理 Dify File 对象时出错: {str(e)}\n\n{traceback.format_exc()}"
                    yield self.create_text_message(error_msg)
                    return
            elif isinstance(input_file, str):
                # 如果是文件路径字符串
                if os.path.exists(input_file):
                    with open(input_file, "rb") as f:
                        file_content = f.read()
                    filename = os.path.basename(input_file)
                else:
                    yield self.create_text_message(f"错误: 文件不存在: {input_file}")
                    return
            elif hasattr(input_file, "read"):
                # 如果是文件对象
                file_content = input_file.read()
                filename = getattr(input_file, "filename", "uploaded_file.xlsx")
                if hasattr(input_file, "name"):
                    filename = os.path.basename(input_file.name)
            elif isinstance(input_file, dict):
                # 如果是字典，可能包含文件路径或内容
                if "path" in input_file:
                    file_path = input_file["path"]
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as f:
                            file_content = f.read()
                        filename = os.path.basename(file_path)
                    else:
                        yield self.create_text_message(f"错误: 文件不存在: {file_path}")
                        return
                elif "content" in input_file:
                    file_content = input_file["content"]
                    if isinstance(file_content, str):
                        file_content = file_content.encode("utf-8")
                    filename = input_file.get("filename", "uploaded_file.xlsx")
                else:
                    yield self.create_text_message("错误: 无法从文件参数中提取文件内容")
                    return
            else:
                yield self.create_text_message(
                    f"错误: 不支持的文件参数类型: {type(input_file)}。"
                    "支持的类型：Dify File 对象、文件路径字符串、文件对象或包含文件信息的字典。"
                )
                return
            
            if not file_content:
                yield self.create_text_message("错误: 无法读取文件内容")
                return
            
            if not filename:
                filename = "uploaded_file.xlsx"
            
            # 使用自定义查询或默认提示词
            analysis_prompt = query if query else DEFAULT_EXCEL_ANALYSIS_PROMPT
            
            # 调用分析函数（异步函数需要运行在事件循环中）
            try:
                # 检查是否已有事件循环
                loop = asyncio.get_event_loop()
            except RuntimeError:
                # 如果没有事件循环，创建一个新的
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # 运行异步分析函数
            result = loop.run_until_complete(
                analyze_excel(
                    file_content=file_content,
                    filename=filename,
                    analysis_api_url=analysis_api_url,  # 必选
                    analysis_model=analysis_model,  # 必选
                    thread_id=None,  # 创建新会话
                    use_llm_validate=use_llm_validate,  # 如果提供了 LLM 配置则启用验证
                    sheet_name=None,  # 使用默认工作表
                    auto_analysis=True,  # 自动分析
                    analysis_prompt=analysis_prompt,
                    stream=False,  # 不支持流式
                    temperature=0.4,
                    llm_api_key=llm_api_key,
                    llm_base_url=llm_base_url,
                    llm_model=llm_model,
                    analysis_api_key=analysis_api_key
                )
            )
            
            # 格式化返回结果
            if result.get("status") == "error":
                error_message = result.get('error_message', '未知错误')
                # 如果错误信息已经包含格式化的 Markdown，直接使用；否则添加基本格式
                if "❌" in error_message or "**" in error_message:
                    yield self.create_text_message(error_message)
                else:
                    # 简单格式化错误信息
                    formatted_error = f"❌ **分析失败**\n\n{error_message}"
                    yield self.create_text_message(formatted_error)
                return
            
            # 构建成功响应
            response_text = f"✅ Excel文件分析完成\n\n"
            response_text += f"📊 **文件信息**\n"
            response_text += f"- 文件名: {filename}\n"
            response_text += f"- 会话ID: {result.get('thread_id', 'N/A')}\n\n"
            
            # 表头分析结果
            if result.get("header_analysis"):
                ha = result["header_analysis"]
                response_text += f"📋 **表头分析**\n"
                response_text += f"- 表头类型: {ha.get('header_type', 'N/A')}\n"
                response_text += f"- 表头行数: {ha.get('header_rows', 'N/A')}\n"
                response_text += f"- 数据起始行: {ha.get('data_start_row', 'N/A')}\n"
                response_text += f"- 置信度: {ha.get('confidence', 'N/A')}\n\n"
            
            # 数据摘要
            if result.get("data_summary"):
                ds = result["data_summary"]
                response_text += f"📈 **数据摘要**\n"
                response_text += f"- 行数: {ds.get('row_count', 'N/A')}\n"
                response_text += f"- 列数: {ds.get('column_count', 'N/A')}\n"
                if ds.get("column_names"):
                    response_text += f"- 列名: {', '.join(ds['column_names'][:5])}"
                    if len(ds["column_names"]) > 5:
                        response_text += f" ... (共{len(ds['column_names'])}列)"
                    response_text += "\n\n"
            
            # 分析结果
            if result.get("analysis_result"):
                ar = result["analysis_result"]
                if ar.get("reasoning"):
                    response_text += f"🤖 **分析结果**\n{ar['reasoning']}\n\n"
                if ar.get("generated_files"):
                    response_text += f"📁 **生成的文件**\n"
                    for file_info in ar["generated_files"]:
                        response_text += f"- {file_info.get('name', 'N/A')}\n"
            
            # 处理后的文件信息
            if result.get("processed_file"):
                pf = result["processed_file"]
                response_text += f"\n💾 **处理后的文件**\n"
                response_text += f"- 文件名: {pf.get('filename', 'N/A')}\n"
                response_text += f"- 文件路径: {pf.get('file_path', 'N/A')}\n"
            
            yield self.create_text_message(response_text)
            
        except Exception as e:
            import traceback
            error_msg = f"错误: {str(e)}\n{traceback.format_exc()}"
            yield self.create_text_message(error_msg)
