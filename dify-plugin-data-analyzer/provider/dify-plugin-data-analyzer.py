from typing import Any, Optional

from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError


class DifyPluginDataAnalyzerProvider(ToolProvider):
    """Excel数据分析工具提供者"""
    
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        """
        验证配置凭据
        
        参数:
        - credentials: 包含各种配置的字典
        """
        try:
            # 验证表头分析 LLM 配置（可选）
            llm_api_key = credentials.get("llm_api_key", "")
            llm_base_url = credentials.get("llm_base_url", "https://api.openai.com/v1/chat/completions")
            llm_model = credentials.get("llm_model", "gpt-4o-mini")
            
            # 如果提供了 API key，验证 URL 格式
            if llm_api_key:
                if not llm_base_url:
                    raise ValueError("LLM Base URL is required when API key is provided")
                
                # 基本URL格式验证
                if not (llm_base_url.startswith("http://") or llm_base_url.startswith("https://")):
                    raise ValueError("LLM Base URL must start with http:// or https://")
                
                if not llm_model:
                    raise ValueError("LLM Model is required when API key is provided")
            
            # 验证数据分析 API 配置（必选）
            analysis_api_url = credentials.get("analysis_api_url", "")
            analysis_model = credentials.get("analysis_model", "")
            
            if not analysis_api_url:
                raise ValueError("Analysis API URL is required")
            
            if not analysis_model:
                raise ValueError("Analysis Model is required")
            
            # 验证 URL 格式
            if not (analysis_api_url.startswith("http://") or analysis_api_url.startswith("https://")):
                raise ValueError("Analysis API URL must start with http:// or https://")
            
            # 验证 URL 是否包含 /chat/completions 或 /v1
            if "/chat/completions" not in analysis_api_url and "/v1" not in analysis_api_url:
                raise ValueError("Analysis API URL should be a valid OpenAI-compatible endpoint (e.g., .../v1/chat/completions)")
            
            # analysis_api_key 是可选的，不需要验证
            
        except Exception as e:
            raise ToolProviderCredentialValidationError(str(e))

    #########################################################################################
    # If OAuth is supported, uncomment the following functions.
    # Warning: please make sure that the sdk version is 0.4.2 or higher.
    #########################################################################################
    # def _oauth_get_authorization_url(self, redirect_uri: str, system_credentials: Mapping[str, Any]) -> str:
    #     """
    #     Generate the authorization URL for dify-plugin-data-analyzer OAuth.
    #     """
    #     try:
    #         """
    #         IMPLEMENT YOUR AUTHORIZATION URL GENERATION HERE
    #         """
    #     except Exception as e:
    #         raise ToolProviderOAuthError(str(e))
    #     return ""
        
    # def _oauth_get_credentials(
    #     self, redirect_uri: str, system_credentials: Mapping[str, Any], request: Request
    # ) -> Mapping[str, Any]:
    #     """
    #     Exchange code for access_token.
    #     """
    #     try:
    #         """
    #         IMPLEMENT YOUR CREDENTIALS EXCHANGE HERE
    #         """
    #     except Exception as e:
    #         raise ToolProviderOAuthError(str(e))
    #     return dict()

    # def _oauth_refresh_credentials(
    #     self, redirect_uri: str, system_credentials: Mapping[str, Any], credentials: Mapping[str, Any]
    # ) -> OAuthCredentials:
    #     """
    #     Refresh the credentials
    #     """
    #     return OAuthCredentials(credentials=credentials, expires_at=-1)
