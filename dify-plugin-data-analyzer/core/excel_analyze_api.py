"""
Excel智能分析API
支持：
1. 上传Excel文件自动处理多级表头
2. 会话管理（支持会话复用）
3. 自动数据分析
"""

import json
import os
import time
import uuid
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Any

import openai

from .config import (
    DEFAULT_TEMPERATURE, STOP_TOKEN_IDS, MAX_NEW_TOKENS,
    EXCEL_VALID_EXTENSIONS, EXCEL_MAX_FILE_SIZE_MB,
    EXCEL_LLM_API_KEY, EXCEL_LLM_BASE_URL, EXCEL_LLM_MODEL,
    DEFAULT_EXCEL_ANALYSIS_PROMPT
)
# Import ProcessedFileInfo as it's still used in the code
from .models import ProcessedFileInfo
# Other models are no longer used as Pydantic models, but kept for type reference if needed
# from .models import ExcelAnalyzeResponse, HeaderAnalysisResponse, ExcelSheetsResponse
from .storage import storage
from .utils import (
    get_thread_workspace, build_file_path, WorkspaceTracker,
    render_file_block, generate_report_from_messages, extract_code_from_segment,
    execute_code_safe_async, collect_file_info
)
from .excel_processor import (
    process_excel_file, get_sheet_names, generate_analysis_prompt,
    ExcelProcessResult
)

# matplotlib中文支持代码
Chinese_matplot_str = """
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei'] 
plt.rcParams['axes.unicode_minus'] = False    
"""

# Helper function to extract base URL from full API URL
def extract_api_base(api_url: str) -> str:
    """从完整的API URL中提取base URL"""
    if api_url.endswith("/chat/completions"):
        return api_url.rsplit("/chat/completions", 1)[0]
    elif "/v1" in api_url:
        return api_url.rsplit("/v1", 1)[0] + "/v1"
    else:
        return api_url


def validate_excel_file(filename: str, file_size: int) -> None:
    """验证Excel文件"""
    # 检查扩展名
    ext = Path(filename).suffix.lower()
    if ext not in EXCEL_VALID_EXTENSIONS:
        raise ValueError(
            f"不支持的文件格式: {ext}。支持的格式: {', '.join(EXCEL_VALID_EXTENSIONS)}"
        )
    
    # 检查文件大小
    max_size_bytes = EXCEL_MAX_FILE_SIZE_MB * 1024 * 1024
    if file_size > max_size_bytes:
        raise ValueError(
            f"文件过大: {file_size / 1024 / 1024:.2f}MB，最大支持: {EXCEL_MAX_FILE_SIZE_MB}MB"
        )


def get_or_create_thread(thread_id: Optional[str]) -> tuple:
    """获取或创建会话"""
    if thread_id:
        # 使用已有会话
        thread = storage.get_thread(thread_id)
        if not thread:
            raise ValueError(f"会话 {thread_id} 不存在")
        workspace_dir = get_thread_workspace(thread_id)
        return thread_id, workspace_dir, False  # False表示非新建
    else:
        # 创建新会话
        thread = storage.create_thread(metadata={"type": "excel_analysis"})
        workspace_dir = get_thread_workspace(thread.id)
        return thread.id, workspace_dir, True  # True表示新建


async def run_data_analysis(
    workspace_dir: str,
    thread_id: str,
    process_result: ExcelProcessResult,
    analysis_prompt: str,
    model: str,
    temperature: float,
    analysis_api_url: str,
    analysis_api_key: Optional[str] = None,
    stream: bool = False
) -> Dict[str, Any]:
    """
    执行数据分析流程
    """
    generated_dir = os.path.join(workspace_dir, "generated")
    os.makedirs(generated_dir, exist_ok=True)
    
    # 构建分析提示词
    full_prompt = generate_analysis_prompt(process_result, analysis_prompt)
    
    # 构建消息
    messages = [{"role": "user", "content": full_prompt}]
    
    # 准备vLLM消息格式
    workspace_file_info = collect_file_info(workspace_dir)
    vllm_messages = [{
        "role": "user",
        "content": f"# Instruction\n{full_prompt}\n\n# Data\n{workspace_file_info}"
    }]
    
    # 跟踪生成的文件
    generated_files = []
    tracker = WorkspaceTracker(workspace_dir, generated_dir)
    
    assistant_reply = ""
    finished = False
    
    # 验证 API URL 格式
    if not analysis_api_url:
        raise ValueError("analysis_api_url 不能为空")
    
    if not (analysis_api_url.startswith("http://") or analysis_api_url.startswith("https://")):
        raise ValueError(f"analysis_api_url 格式不正确，必须以 http:// 或 https:// 开头: {analysis_api_url}")
    
    # 创建分析 API 客户端
    try:
        api_base = extract_api_base(analysis_api_url)
        api_key = analysis_api_key or "dummy"
        analysis_client_async = openai.AsyncOpenAI(base_url=api_base, api_key=api_key, timeout=60.0)
    except Exception as e:
        raise ValueError(f"创建分析 API 客户端失败: {str(e)}。请检查 analysis_api_url 配置: {analysis_api_url}")
    
    while not finished:
        # 调用分析 API
        try:
            response = await analysis_client_async.chat.completions.create(
                model=model,
                messages=vllm_messages,
                temperature=temperature,
                stream=True,
                extra_body={
                    "add_generation_prompt": False,
                    "stop_token_ids": STOP_TOKEN_IDS,
                    "max_new_tokens": MAX_NEW_TOKENS,
                },
            )
        except openai.APIConnectionError as e:
            error_msg = (
                f"❌ **连接分析 API 失败**\n\n"
                f"**错误详情：** {str(e)}\n\n"
                f"**可能的原因：**\n"
                f"1. 分析 API 服务未启动或无法访问\n"
                f"2. API 地址配置错误: `{analysis_api_url}`\n"
                f"3. 网络连接问题（防火墙、代理等）\n"
                f"4. API 服务地址不正确或端口未开放\n\n"
                f"**解决方法：**\n"
                f"1. 确认分析 API 服务正在运行\n"
                f"2. 检查 API 地址是否正确: `{analysis_api_url}`\n"
                f"3. 尝试在浏览器或命令行中访问该地址\n"
                f"4. 检查网络连接和防火墙设置\n"
                f"5. 如果使用 localhost，确保服务在正确的端口上运行\n"
            )
            raise ConnectionError(error_msg) from e
        except openai.APIError as e:
            error_msg = (
                f"❌ **分析 API 调用失败**\n\n"
                f"**错误详情：** {str(e)}\n\n"
                f"**API 地址：** {analysis_api_url}\n"
                f"**模型：** {model}\n\n"
                f"**可能的原因：**\n"
                f"1. API 密钥无效或过期\n"
                f"2. 模型名称不正确\n"
                f"3. API 服务返回错误\n"
                f"4. 请求参数不合法\n"
            )
            raise ValueError(error_msg) from e
        except Exception as e:
            error_msg = (
                f"❌ **调用分析 API 时发生未知错误**\n\n"
                f"**错误类型：** {type(e).__name__}\n"
                f"**错误详情：** {str(e)}\n\n"
                f"**API 地址：** {analysis_api_url}\n"
                f"**模型：** {model}\n"
            )
            raise RuntimeError(error_msg) from e
        
        cur_res = ""
        last_finish_reason = None
        
        async for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content is not None:
                delta = chunk.choices[0].delta.content
                cur_res += delta
                assistant_reply += delta
            last_finish_reason = chunk.choices[0].finish_reason
            if "</Answer>" in cur_res:
                finished = True
                break
        
        has_code_segment = "<Code>" in cur_res
        has_closed_code = "</Code>" in cur_res
        
        if last_finish_reason == "stop" and not finished:
            if has_code_segment and not has_closed_code:
                cur_res += "</Code>"
                assistant_reply += "</Code>"
                has_closed_code = True
            elif not has_code_segment:
                finished = True
        
        if "</Answer>" in cur_res:
            finished = True
        
        # 执行代码
        if has_code_segment and has_closed_code and not finished:
            vllm_messages.append({"role": "assistant", "content": cur_res})
            code_str = extract_code_from_segment(cur_res)
            if code_str:
                code_str = Chinese_matplot_str + "\n" + code_str
                exe_output = await execute_code_safe_async(code_str, workspace_dir)
                artifacts = tracker.diff_and_collect()
                exe_str = f"\n<Execute>\n```\n{exe_output}\n```\n</Execute>\n"
                render_file_block(artifacts, workspace_dir, thread_id, generated_files)
                assistant_reply += exe_str
                vllm_messages.append({"role": "execute", "content": exe_output})
            else:
                finished = True
    
    # 生成报告
    report_block = generate_report_from_messages(
        messages, assistant_reply, workspace_dir, thread_id, generated_files
    )
    
    return {
        "reasoning": assistant_reply,
        "generated_files": generated_files,
        "report": report_block
    }


async def analyze_excel(
    file_content: bytes,
    filename: str,
    analysis_api_url: str,
    analysis_model: str,
    thread_id: Optional[str] = None,
    use_llm_validate: bool = False,
    sheet_name: Optional[str] = None,
    auto_analysis: bool = True,
    analysis_prompt: Optional[str] = None,
    stream: bool = False,
    temperature: float = DEFAULT_TEMPERATURE,
    llm_api_key: Optional[str] = None,
    llm_base_url: Optional[str] = None,
    llm_model: Optional[str] = None,
    analysis_api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Excel智能分析函数
    
    功能：
    1. 处理Excel文件
    2. 使用规则分析处理多级表头（默认）
    3. 可选使用LLM验证规则分析结果
    4. 可选自动数据分析
    5. 支持会话复用
    
    参数：
    - file_content: Excel文件内容（bytes）
    - filename: 文件名
    - thread_id: 会话ID（可选，不提供则创建新会话）
    - use_llm_validate: 是否使用LLM验证规则分析结果（可选，默认False）
    - llm_api_key: LLM API密钥（可选）
    - llm_base_url: LLM API地址（可选）
    - llm_model: LLM模型名称（可选）
    - sheet_name: 工作表名称（可选，默认第一个）
    - auto_analysis: 是否自动分析（可选，默认True）
    - analysis_prompt: 自定义分析提示词（可选）
    - stream: 是否流式返回（可选，默认False，当前不支持流式）
    - analysis_api_url: 数据分析API地址（必填）
    - analysis_model: 数据分析模型名称（必填）
    - analysis_api_key: 数据分析API密钥（可选）
    - temperature: 生成温度（默认0.4）
    
    返回：
    - Dict包含: thread_id, status, header_analysis, processed_file, analysis_result等
    """
    file_size = len(file_content)
    
    # 验证文件
    validate_excel_file(filename, file_size)
    
    # 获取或创建会话
    current_thread_id, workspace_dir, is_new = get_or_create_thread(thread_id)
    generated_dir = os.path.join(workspace_dir, "generated")
    os.makedirs(generated_dir, exist_ok=True)
    
    try:
        # 保存上传的文件到工作空间
        excel_path = os.path.join(workspace_dir, filename)
        with open(excel_path, "wb") as f:
            f.write(file_content)
        
        # 获取可用工作表
        available_sheets = get_sheet_names(excel_path)
        
        # 检查LLM配置（优先使用传入的配置，否则使用环境变量）
        api_key = llm_api_key if llm_api_key is not None else EXCEL_LLM_API_KEY
        if use_llm_validate and not api_key:
            use_llm_validate = False  # 没有API key则不进行LLM验证
        
        # 处理Excel文件（先规则分析，再用LLM验证）
        process_result = process_excel_file(
            filepath=excel_path,
            output_dir=workspace_dir,
            sheet_name=sheet_name,
            use_llm_validate=use_llm_validate,
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=llm_model
        )
        
        if not process_result.success:
            return {
                "thread_id": current_thread_id,
                "status": "error",
                "error_message": process_result.error_message,
                "available_sheets": available_sheets
            }
        
        # 构建处理后的文件信息
        processed_file_info = None
        metadata_file_info = None
        
        if process_result.processed_file_path:
            csv_filename = os.path.basename(process_result.processed_file_path)
            processed_file_info = ProcessedFileInfo(
                filename=csv_filename,
                url=build_file_path(current_thread_id, csv_filename),
                size_bytes=os.path.getsize(process_result.processed_file_path)
            )
        
        if process_result.metadata_file_path:
            meta_filename = os.path.basename(process_result.metadata_file_path)
            metadata_file_info = ProcessedFileInfo(
                filename=meta_filename,
                url=build_file_path(current_thread_id, meta_filename),
                size_bytes=os.path.getsize(process_result.metadata_file_path) if os.path.exists(process_result.metadata_file_path) else None
            )
        
        # 构建表头分析响应
        header_analysis_response = None
        if process_result.header_analysis:
            ha = process_result.header_analysis
            header_analysis_response = {
                "skip_rows": ha.skip_rows,
                "header_rows": ha.header_rows,
                "header_type": ha.header_type,
                "data_start_row": ha.data_start_row,
                "confidence": ha.confidence,
                "reason": ha.reason
            }
        
        # 数据摘要
        data_summary = {
            "row_count": process_result.row_count,
            "column_count": len(process_result.column_names),
            "column_names": process_result.column_names
        }
        
        # 注意：流式返回在当前实现中不支持，stream 参数将被忽略
        # 如果需要流式功能，可以在调用方实现
        
        # 非流式处理
        analysis_result = None
        if auto_analysis:
            prompt = analysis_prompt or DEFAULT_EXCEL_ANALYSIS_PROMPT
            analysis_result = await run_data_analysis(
                workspace_dir=workspace_dir,
                thread_id=current_thread_id,
                process_result=process_result,
                analysis_prompt=prompt,
                model=analysis_model,
                temperature=temperature,
                analysis_api_url=analysis_api_url,
                analysis_api_key=analysis_api_key,
                stream=False
            )
        
        # 更新会话元数据
        if current_thread_id in storage.threads:
            excel_files = storage.threads[current_thread_id].get("metadata", {}).get("excel_files", [])
            excel_files.append({
                "original_name": filename,
                "processed_name": os.path.basename(process_result.processed_file_path) if process_result.processed_file_path else None,
                "sheet_name": sheet_name,
                "timestamp": int(time.time())
            })
            storage.threads[current_thread_id]["metadata"]["excel_files"] = excel_files
        
        return {
            "thread_id": current_thread_id,
            "status": "success",
            "header_analysis": header_analysis_response,
            "processed_file": processed_file_info,
            "metadata_file": metadata_file_info,
            "data_summary": data_summary,
            "column_metadata": process_result.column_metadata,
            "analysis_result": analysis_result,
            "available_sheets": available_sheets
        }
        
    except Exception as e:
        import traceback
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        return {
            "thread_id": current_thread_id if 'current_thread_id' in locals() else "",
            "status": "error",
            "error_message": error_msg
        }


async def get_excel_sheets(file_id: str) -> Dict[str, Any]:
    """
    获取Excel文件的工作表列表
    
    参数：
    - file_id: 已上传的文件ID
    
    返回：
    - Dict包含: filename, sheets, default_sheet
    """
    file_obj = storage.get_file(file_id)
    if not file_obj:
        raise ValueError(f"文件 {file_id} 不存在")
    
    filepath = storage.files[file_id].get("filepath")
    if not filepath or not os.path.exists(filepath):
        raise ValueError("文件不存在")
    
    sheets = get_sheet_names(filepath)
    if not sheets:
        raise ValueError("无法读取工作表列表")
    
    return {
        "filename": file_obj.filename,
        "sheets": sheets,
        "default_sheet": sheets[0]
    }


async def process_excel_only(
    file_content: bytes,
    filename: str,
    thread_id: Optional[str] = None,
    use_llm_validate: bool = False,
    sheet_name: Optional[str] = None,
    llm_api_key: Optional[str] = None,
    llm_base_url: Optional[str] = None,
    llm_model: Optional[str] = None
) -> Dict[str, Any]:
    """
    仅处理Excel文件（不进行数据分析）
    
    用于只需要处理表头、转换格式的场景
    默认使用规则分析，可选使用LLM验证结果（LLM配置从.env读取）
    """
    file_size = len(file_content)
    
    # 验证文件
    validate_excel_file(filename, file_size)
    
    # 获取或创建会话
    current_thread_id, workspace_dir, is_new = get_or_create_thread(thread_id)
    
    try:
        # 保存上传的文件
        excel_path = os.path.join(workspace_dir, filename)
        with open(excel_path, "wb") as f:
            f.write(file_content)
        
        # 获取可用工作表
        available_sheets = get_sheet_names(excel_path)
        
        # 检查LLM配置（优先使用传入的配置，否则使用环境变量）
        api_key = llm_api_key if llm_api_key is not None else EXCEL_LLM_API_KEY
        if use_llm_validate and not api_key:
            use_llm_validate = False
        
        # 处理Excel文件（先规则分析，再用LLM验证）
        process_result = process_excel_file(
            filepath=excel_path,
            output_dir=workspace_dir,
            sheet_name=sheet_name,
            use_llm_validate=use_llm_validate,
            llm_api_key=llm_api_key,
            llm_base_url=llm_base_url,
            llm_model=llm_model
        )
        
        if not process_result.success:
            return {
                "thread_id": current_thread_id,
                "status": "error",
                "error_message": process_result.error_message,
                "available_sheets": available_sheets
            }
        
        # 构建响应
        processed_file_info = None
        metadata_file_info = None
        
        if process_result.processed_file_path:
            csv_filename = os.path.basename(process_result.processed_file_path)
            processed_file_info = {
                "filename": csv_filename,
                "file_path": build_file_path(current_thread_id, csv_filename),
                "size_bytes": os.path.getsize(process_result.processed_file_path)
            }
        else:
            processed_file_info = None
        
        if process_result.metadata_file_path:
            meta_filename = os.path.basename(process_result.metadata_file_path)
            metadata_file_info = {
                "filename": meta_filename,
                "file_path": build_file_path(current_thread_id, meta_filename)
            }
        else:
            metadata_file_info = None
        
        header_analysis_response = None
        if process_result.header_analysis:
            ha = process_result.header_analysis
            header_analysis_response = {
                "skip_rows": ha.skip_rows,
                "header_rows": ha.header_rows,
                "header_type": ha.header_type,
                "data_start_row": ha.data_start_row,
                "confidence": ha.confidence,
                "reason": ha.reason
            }
        
        return {
            "thread_id": current_thread_id,
            "status": "success",
            "header_analysis": header_analysis_response,
            "processed_file": processed_file_info,
            "metadata_file": metadata_file_info,
            "data_summary": {
                "row_count": process_result.row_count,
                "column_count": len(process_result.column_names),
                "column_names": process_result.column_names
            },
            "column_metadata": process_result.column_metadata,
            "available_sheets": available_sheets
        }
        
    except Exception as e:
        import traceback
        return {
            "thread_id": current_thread_id if 'current_thread_id' in locals() else "",
            "status": "error",
            "error_message": f"{str(e)}\n{traceback.format_exc()}"
        }


async def continue_analysis(
    thread_id: str,
    prompt: str,
    analysis_api_url: str,
    analysis_model: str,
    temperature: float = DEFAULT_TEMPERATURE,
    stream: bool = False,
    analysis_api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    在已有会话中继续分析
    
    用于对已处理的数据进行后续分析
    
    参数:
    - thread_id: 会话ID（必填）
    - prompt: 分析提示词（必填）
    - analysis_api_url: 数据分析API地址（必填）
    - analysis_model: 数据分析模型名称（必填）
    - temperature: 生成温度（默认0.4）
    - stream: 是否流式返回（当前不支持，将被忽略）
    - analysis_api_key: 数据分析API密钥（可选）
    
    注意：stream 参数当前不支持，将被忽略
    """
    # 验证会话
    thread = storage.get_thread(thread_id)
    if not thread:
        raise ValueError(f"会话 {thread_id} 不存在")
    
    workspace_dir = get_thread_workspace(thread_id)
    generated_dir = os.path.join(workspace_dir, "generated")
    os.makedirs(generated_dir, exist_ok=True)
    
    # 构建消息
    workspace_file_info = collect_file_info(workspace_dir)
    vllm_messages = [{
        "role": "user",
        "content": f"# Instruction\n{prompt}\n\n# Data\n{workspace_file_info}"
    }]
    
    # 注意：流式返回在当前实现中不支持，stream 参数将被忽略
    # 如果需要流式功能，可以在调用方实现
    
    # 非流式处理
    generated_files = []
    tracker = WorkspaceTracker(workspace_dir, generated_dir)
    assistant_reply = ""
    finished = False
    
    # 创建分析 API 客户端
    api_base = extract_api_base(analysis_api_url)
    api_key = analysis_api_key or "dummy"
    analysis_client_async = openai.AsyncOpenAI(base_url=api_base, api_key=api_key)
    
    while not finished:
        response = await analysis_client_async.chat.completions.create(
            model=analysis_model,
            messages=vllm_messages,
            temperature=temperature,
            stream=True,
            extra_body={
                "add_generation_prompt": False,
                "stop_token_ids": STOP_TOKEN_IDS,
                "max_new_tokens": MAX_NEW_TOKENS,
            },
        )
        
        cur_res = ""
        last_finish_reason = None
        
        async for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content is not None:
                delta = chunk.choices[0].delta.content
                cur_res += delta
                assistant_reply += delta
            last_finish_reason = chunk.choices[0].finish_reason
            if "</Answer>" in cur_res:
                finished = True
                break
        
        has_code_segment = "<Code>" in cur_res
        has_closed_code = "</Code>" in cur_res
        
        if last_finish_reason == "stop" and not finished:
            if has_code_segment and not has_closed_code:
                cur_res += "</Code>"
                assistant_reply += "</Code>"
                has_closed_code = True
            elif not has_code_segment:
                finished = True
        
        if has_code_segment and has_closed_code and not finished:
            vllm_messages.append({"role": "assistant", "content": cur_res})
            code_str = extract_code_from_segment(cur_res)
            if code_str:
                code_str = Chinese_matplot_str + "\n" + code_str
                exe_output = await execute_code_safe_async(code_str, workspace_dir)
                artifacts = tracker.diff_and_collect()
                exe_str = f"\n<Execute>\n```\n{exe_output}\n```\n</Execute>\n"
                render_file_block(artifacts, workspace_dir, thread_id, generated_files)
                assistant_reply += exe_str
                vllm_messages.append({"role": "execute", "content": exe_output})
            else:
                finished = True
    
    # 生成报告
    messages = [{"role": "user", "content": prompt}]
    generate_report_from_messages(
        messages, assistant_reply, workspace_dir, thread_id, generated_files
    )
    
    return {
        "thread_id": thread_id,
        "status": "success",
        "reasoning": assistant_reply,
        "generated_files": generated_files
    }

