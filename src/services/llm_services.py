import logging
from typing import List, Dict, Any, Optional
import openai
from anthropic import Anthropic
from config.settings import settings

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self):
        self.provider = settings.llm_provider
        logger.info(f"Initializing LLM Service with provider: {self.provider}")
        
        if self.provider == "openai":
            self.client = openai.OpenAI(api_key=settings.openai_api_key)
        elif self.provider == "azure-openai":
            self.client = openai.OpenAI(
                api_key=settings.azure_openai_api_key,
                base_url=f"{settings.azure_openai_endpoint}openai/deployments/{settings.azure_openai_model}",
                default_query={"api-version": settings.azure_openai_api_version},
                default_headers={"api-key": settings.azure_openai_api_key}
            )
        elif self.provider == "anthropic":
            self.client = Anthropic(api_key=settings.anthropic_api_key)
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")
    
    async def create_completion(
        self, 
        messages: List[Dict[str, str]], 
        **options
    ) -> Dict[str, Any]:
        """Create a completion using the configured LLM provider."""
        logger.info(f"Creating completion with provider: {self.provider}")
        
        if self.provider in ["openai", "azure-openai"]:
            return await self._create_openai_completion(messages, **options)
        elif self.provider == "anthropic":
            return await self._create_anthropic_completion(messages, **options)
    
    async def _create_openai_completion(
        self, 
        messages: List[Dict[str, str]], 
        **options
    ) -> Dict[str, Any]:
        """Create OpenAI completion."""
        try:
            model = options.get("model") or (
                settings.azure_openai_model if self.provider == "azure-openai" 
                else settings.openai_model
            )
            
            response = self.client.chat.completions.create(
                model=model,
                messages=messages,
                **options
            )
            
            return {
                "content": response.choices[0].message.content.strip(),
                "usage": response.usage.model_dump() if response.usage else None,
                "provider": self.provider
            }
        except Exception as error:
            logger.error(f"{self.provider} API error: {error}")
            raise Exception(f"{self.provider} API error: {error}")
    
    async def _create_anthropic_completion(
        self, 
        messages: List[Dict[str, str]], 
        **options
    ) -> Dict[str, Any]:
        """Create Anthropic completion."""
        try:
            system_message = next((msg for msg in messages if msg["role"] == "system"), None)
            user_messages = [msg for msg in messages if msg["role"] != "system"]
            
            response = self.client.messages.create(
                model=options.get("model", settings.anthropic_model),
                max_tokens=options.get("max_tokens", 4096),
                temperature=options.get("temperature", 0.7),
                system=system_message["content"] if system_message else None,
                messages=[
                    {
                        "role": "user" if msg["role"] == "user" else "assistant",
                        "content": msg["content"]
                    }
                    for msg in user_messages
                ]
            )
            
            return {
                "content": response.content[0].text.strip(),
                "usage": {
                    "prompt_tokens": response.usage.input_tokens,
                    "completion_tokens": response.usage.output_tokens,
                    "total_tokens": response.usage.input_tokens + response.usage.output_tokens
                },
                "provider": "anthropic"
            }
        except Exception as error:
            logger.error(f"Anthropic API error: {error}")
            raise Exception(f"Anthropic API error: {error}")


# Singleton instance
llm_service = LLMService()