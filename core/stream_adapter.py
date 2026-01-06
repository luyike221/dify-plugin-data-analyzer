"""
流式输出适配器模块
用于在 Dify 插件中实现流式输出功能

支持:
1. 异步 Generator 到同步 Generator 的转换
2. 流式消息的封装和格式化
3. 进度状态的流式输出
"""

import asyncio
import queue
import threading
from typing import Generator, AsyncGenerator, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum


class StreamPhase(Enum):
    """流式输出阶段枚举"""
    FILE_RECEIVE = "file_receive"       # 文件接收
    HEADER_ANALYSIS = "header_analysis" # 表头分析
    LLM_VALIDATION = "llm_validation"   # LLM验证
    DATA_CONVERT = "data_convert"       # 数据转换
    DATA_ANALYSIS = "data_analysis"     # 数据分析
    CODE_EXECUTE = "code_execute"       # 代码执行
    REPORT_GENERATE = "report_generate" # 报告生成
    COMPLETE = "complete"               # 完成


@dataclass
class StreamMessage:
    """流式消息数据类"""
    phase: StreamPhase
    content: str
    is_final: bool = False
    metadata: Optional[dict] = None
    
    def to_display_string(self) -> str:
        """转换为显示字符串"""
        return self.content


class StreamBuffer:
    """
    流式缓冲区
    用于在异步和同步代码之间传递数据
    """
    
    def __init__(self, maxsize: int = 0):
        self._queue: queue.Queue = queue.Queue(maxsize=maxsize)
        self._closed = False
        self._error: Optional[Exception] = None
    
    def put(self, item: str) -> None:
        """放入数据"""
        if not self._closed:
            self._queue.put(item)
    
    def get(self, timeout: Optional[float] = None) -> Optional[str]:
        """获取数据"""
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def close(self) -> None:
        """关闭缓冲区"""
        self._closed = True
        self._queue.put(None)  # 放入哨兵值
    
    def set_error(self, error: Exception) -> None:
        """设置错误"""
        self._error = error
        self.close()
    
    @property
    def error(self) -> Optional[Exception]:
        """获取错误"""
        return self._error
    
    @property
    def closed(self) -> bool:
        """是否已关闭"""
        return self._closed


def async_generator_to_sync(
    async_gen_func: Callable[..., AsyncGenerator[str, None]],
    *args,
    **kwargs
) -> Generator[str, None, None]:
    """
    将异步 Generator 函数转换为同步 Generator
    
    用于在 Dify 的同步 _invoke 方法中使用异步流式输出
    
    参数:
        async_gen_func: 异步 Generator 函数
        *args: 传递给异步函数的位置参数
        **kwargs: 传递给异步函数的关键字参数
    
    Yields:
        str: 流式输出的字符串块
    
    使用示例:
        async def my_async_gen():
            for i in range(10):
                yield f"chunk {i}"
                await asyncio.sleep(0.1)
        
        for chunk in async_generator_to_sync(my_async_gen):
            print(chunk)
    """
    buffer = StreamBuffer()
    
    def run_async():
        """在新线程中运行异步代码"""
        try:
            # 在新线程中，总是创建全新的事件循环
            # 这样可以避免与主线程的事件循环冲突
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def consume():
                try:
                    async_gen = async_gen_func(*args, **kwargs)
                    async for chunk in async_gen:
                        buffer.put(chunk)
                except Exception as e:
                    buffer.set_error(e)
                finally:
                    buffer.close()
            
            # 在新线程中运行，确保事件循环未运行
            try:
                loop.run_until_complete(consume())
            finally:
                # 清理事件循环
                try:
                    # 取消所有待处理的任务
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    # 等待任务取消完成
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
                
                # 关闭事件循环
                if not loop.is_closed():
                    loop.close()
        except Exception as e:
            buffer.set_error(e)
            buffer.close()
    
    # 在后台线程中运行异步代码
    thread = threading.Thread(target=run_async, daemon=True)
    thread.start()
    
    # 从缓冲区读取数据并 yield
    while True:
        item = buffer.get(timeout=60.0)  # 60秒超时
        if item is None:
            if buffer.error:
                raise buffer.error
            break
        yield item
    
    thread.join(timeout=5.0)


def run_async_generator_sync(
    async_gen: AsyncGenerator[str, None],
    loop: Optional[asyncio.AbstractEventLoop] = None
) -> Generator[str, None, None]:
    """
    运行已创建的异步 Generator 并转换为同步 Generator
    
    参数:
        async_gen: 已创建的异步 Generator
        loop: 事件循环（可选）
    
    Yields:
        str: 流式输出的字符串块
    """
    if loop is None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    
    while True:
        try:
            chunk = loop.run_until_complete(async_gen.__anext__())
            yield chunk
        except StopAsyncIteration:
            break


def format_progress_message(
    phase: StreamPhase,
    message: str,
    icon: Optional[str] = None
) -> str:
    """
    格式化进度消息
    
    参数:
        phase: 当前阶段
        message: 消息内容
        icon: 图标（可选）
    
    返回:
        格式化的消息字符串
    """
    icons = {
        StreamPhase.FILE_RECEIVE: "📥",
        StreamPhase.HEADER_ANALYSIS: "📋",
        StreamPhase.LLM_VALIDATION: "🤖",
        StreamPhase.DATA_CONVERT: "🔄",
        StreamPhase.DATA_ANALYSIS: "🧠",
        StreamPhase.CODE_EXECUTE: "▶️",
        StreamPhase.REPORT_GENERATE: "📄",
        StreamPhase.COMPLETE: "✅",
    }
    
    if icon is None:
        icon = icons.get(phase, "•")
    
    return f"{icon} {message}"


def format_success_message(message: str) -> str:
    """格式化成功消息"""
    return f"✅ {message}"


def format_error_message(message: str) -> str:
    """格式化错误消息"""
    return f"❌ {message}"


def format_info_message(message: str) -> str:
    """格式化信息消息"""
    return f"ℹ️ {message}"


def format_warning_message(message: str) -> str:
    """格式化警告消息"""
    return f"⚠️ {message}"


def format_code_block(code: str, language: str = "python") -> str:
    """格式化代码块"""
    return f"```{language}\n{code}\n```"


def format_execution_result(output: str) -> str:
    """格式化执行结果"""
    return f"```\n{output}\n```"


class StreamProgressTracker:
    """
    流式进度追踪器
    用于追踪和输出处理进度
    """
    
    def __init__(self):
        self.current_phase: StreamPhase = StreamPhase.FILE_RECEIVE
        self.phase_messages: dict = {}
        self.start_time: Optional[float] = None
    
    def start(self) -> str:
        """开始追踪"""
        import time
        self.start_time = time.time()
        return "🚀 开始处理...\n\n"
    
    def update_phase(self, phase: StreamPhase, message: str) -> str:
        """更新阶段"""
        self.current_phase = phase
        self.phase_messages[phase] = message
        return format_progress_message(phase, message) + "\n"
    
    def complete_phase(self, phase: StreamPhase, message: str) -> str:
        """完成阶段"""
        return format_success_message(message) + "\n\n"
    
    def finish(self) -> str:
        """完成追踪"""
        import time
        if self.start_time:
            elapsed = time.time() - self.start_time
            return f"\n🎉 处理完成！耗时: {elapsed:.1f}秒\n"
        return "\n🎉 处理完成！\n"
    
    def error(self, message: str) -> str:
        """错误"""
        return format_error_message(message) + "\n"

