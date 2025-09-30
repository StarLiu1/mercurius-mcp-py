"""
LLM Factory for centralized model management.
Supports OpenAI, Azure OpenAI, Azure OSS, and GPT-5 mini.
"""

import logging
from typing import Dict, Any, Optional
from openai import OpenAI, AzureOpenAI

logger = logging.getLogger(__name__)


class LLMClientWrapper:
    """Wrapper class to handle model-specific parameter differences and API routing."""

    def __init__(self, client, provider_type: str, model_name: str, provider_config: Dict[str, Any]):
        self.client = client
        self.provider_type = provider_type
        self.model_name = model_name
        self.provider_config = provider_config
        # Determine if this is a GPT-5 model that requires Responses API
        self.is_gpt5_model = self._is_gpt5_model()

    def _is_gpt5_model(self) -> bool:
        """Check if the model is a GPT-5 variant requiring Responses API."""
        gpt5_models = ['gpt-5', 'gpt-5-mini', 'gpt-5-nano', 'gpt-5-chat']
        return any(model in self.model_name.lower() for model in gpt5_models)

    @property
    def chat(self):
        """Provide access to chat completions with parameter adaptation."""
        return self

    @property
    def completions(self):
        """Provide access to completions API."""
        return self

    @property
    def responses(self):
        """Provide access to responses API for GPT-5 models."""
        return self

    def _extract_response_text(self, response) -> str:
        """Extract text content from Responses API response."""
        output_text = ""
        if hasattr(response, 'output') and response.output:
            for item in response.output:
                if hasattr(item, 'content') and item.content:
                    for content in item.content:
                        if hasattr(content, 'text'):
                            output_text += content.text
        return output_text

    def _convert_messages_to_input(self, messages):
        """Convert chat messages format to Responses API input format."""
        # For simple cases, just concatenate the messages
        input_parts = []

        for msg in messages:
            role = msg.get('role', 'user')
            content = msg.get('content', '')

            if role == 'system':
                # System messages become developer messages in Responses API
                input_parts.append({'role': 'developer', 'content': content})
            elif role == 'user':
                input_parts.append({'role': 'user', 'content': content})
            elif role == 'assistant':
                # Skip assistant messages for now (could be added as context)
                continue

        # If we have multiple parts, return as list, otherwise return single input
        if len(input_parts) == 1 and input_parts[0]['role'] == 'user':
            return input_parts[0]['content']
        elif input_parts:
            return input_parts
        else:
            return ""

    def create(self, **kwargs):
        """Create a completion with model-specific parameter adaptation."""
        # Check if this is a GPT-5 model that needs Responses API
        if self.is_gpt5_model:
            return self._create_with_responses_api(**kwargs)
        else:
            return self._create_with_chat_api(**kwargs)

    def _create_with_chat_api(self, **kwargs):
        """Create completion using Chat Completions API."""
        adapted_kwargs = dict(kwargs)

        # Handle GPT-5 mini specific requirements (if using chat API fallback)
        if self.provider_type == "azure" and "gpt-5" in self.model_name.lower():
            # GPT-5 mini requires max_completion_tokens instead of max_tokens
            if "max_tokens" in adapted_kwargs:
                adapted_kwargs["max_completion_tokens"] = adapted_kwargs.pop("max_tokens")
            # GPT-5 mini only supports temperature=1.0
            if "temperature" in adapted_kwargs:
                adapted_kwargs["temperature"] = 1.0

        # Call the underlying client
        return self.client.chat.completions.create(**adapted_kwargs)

    def _create_with_responses_api(self, **kwargs):
        """Create completion using Responses API for GPT-5 models."""
        logger.info(f"Using Responses API for {self.model_name}")

        # Convert parameters from Chat API format to Responses API format
        responses_kwargs = {}

        # Convert messages to input
        if 'messages' in kwargs:
            responses_kwargs['input'] = self._convert_messages_to_input(kwargs['messages'])

        # Set model
        responses_kwargs['model'] = kwargs.get('model', self.model_name)

        # Add text parameters with verbosity
        responses_kwargs['text'] = {
            'verbosity': 'medium'  # Default to medium verbosity
        }

        # Add reasoning parameters
        responses_kwargs['reasoning'] = {
            'effort': 'medium'  # Default to medium reasoning effort
        }

        # Handle max tokens (Responses API uses different parameter names)
        if 'max_tokens' in kwargs or 'max_completion_tokens' in kwargs:
            max_tokens = kwargs.get('max_tokens') or kwargs.get('max_completion_tokens', 200)
            # Responses API doesn't have a direct max_tokens parameter
            # The model handles this internally based on context
            logger.info(f"Note: max_tokens ({max_tokens}) is handled differently in Responses API")

        try:
            # Call Responses API
            response = self.client.responses.create(**responses_kwargs)

            # Create a mock ChatCompletion response to maintain compatibility
            # Extract text from the response
            text_content = self._extract_response_text(response)

            # Create a compatible response structure
            class MockMessage:
                def __init__(self, content):
                    self.content = content

            class MockChoice:
                def __init__(self, message):
                    self.message = message
                    self.finish_reason = 'stop'

            class MockResponse:
                def __init__(self, content):
                    self.choices = [MockChoice(MockMessage(content))]

            return MockResponse(text_content)

        except Exception as e:
            logger.error(f"Responses API call failed: {e}")
            logger.info("Attempting fallback to Chat Completions API...")
            # Fallback to Chat API if Responses API fails
            # Need to preserve the original kwargs for fallback
            return self._create_with_chat_api(**kwargs)


class LLMFactory:
    """Factory class for creating LLM clients based on provider configuration."""

    @staticmethod
    def create_client(provider_config: Dict[str, Any], provider_type: str):
        """
        Create an LLM client based on provider type and configuration.

        Args:
            provider_config: Configuration dictionary for the provider
            provider_type: Type of provider (openai, azure, azure_oss, gpt5_mini)

        Returns:
            LLMClientWrapper instance that handles model-specific parameters
        """
        logger.info(f"Creating LLM client for provider: {provider_type}")

        # Get the model name for the wrapper
        model_name = LLMFactory.get_model_name(provider_config, provider_type)

        if provider_type == "azure_oss":
            # Azure OSS model configuration
            logger.info(f"Configuring Azure OSS model: {provider_config.get('deployment_name')}")
            client = OpenAI(
                base_url=provider_config['endpoint'],
                api_key=provider_config['api_key']
            )

        elif provider_type == "azure":
            # Standard Azure OpenAI configuration
            logger.info(f"Configuring Azure OpenAI model: {provider_config.get('deployment_name')}")
            client = AzureOpenAI(
                azure_endpoint=provider_config['endpoint'],
                api_key=provider_config['api_key'],
                api_version=provider_config['api_version'],
                azure_deployment=provider_config['deployment_name']
            )

        elif provider_type == "gpt5_mini":
            # GPT-5 mini configuration (similar to Azure)
            logger.info(f"Configuring GPT-5 mini model: {provider_config.get('deployment_name')}")
            client = AzureOpenAI(
                azure_endpoint=provider_config['endpoint'],
                api_key=provider_config['api_key'],
                api_version=provider_config['api_version'],
                azure_deployment=provider_config['deployment_name']
            )

        elif provider_type == "openai":
            # Standard OpenAI configuration
            logger.info(f"Configuring OpenAI model: {provider_config.get('model')}")
            base_url = provider_config.get('base_url')
            if base_url:
                client = OpenAI(
                    api_key=provider_config['api_key'],
                    base_url=base_url
                )
            else:
                client = OpenAI(api_key=provider_config['api_key'])

        else:
            raise ValueError(f"Unsupported provider type: {provider_type}")

        # Return wrapped client that handles model-specific parameters
        return LLMClientWrapper(client, provider_type, model_name, provider_config)

    @staticmethod
    def get_model_name(provider_config: Dict[str, Any], provider_type: str) -> str:
        """
        Get the model name from provider configuration.

        Args:
            provider_config: Configuration dictionary for the provider
            provider_type: Type of provider

        Returns:
            Model name string
        """
        if provider_type in ["azure", "azure_oss", "gpt5_mini"]:
            return provider_config.get('deployment_name', 'default-model')
        else:
            return provider_config.get('model', 'gpt-4-turbo')

    @staticmethod
    def create_component_client(config: Dict[str, Any], component_name: str):
        """
        Create an LLM client for a specific component.

        Args:
            config: Full configuration dictionary
            component_name: Name of the component (cql_parser, sql_generator, etc.)

        Returns:
            Tuple of (client, model_name)
        """
        # Check for model override
        if config.get('model_override'):
            provider_type = config['model_override']
            logger.info(f"Using model override: {provider_type} for {component_name}")
        else:
            # Get component-specific model selection
            model_selection = config.get('model_selection', {})
            provider_type = model_selection.get(component_name, config.get('model_provider', 'openai'))
            logger.info(f"Using model: {provider_type} for {component_name}")

        # Get provider configuration
        models_config = config.get('models', {})
        provider_config = models_config.get(provider_type)

        if not provider_config:
            raise ValueError(f"No configuration found for provider: {provider_type}")

        # Create client
        client = LLMFactory.create_client(provider_config, provider_type)
        model_name = LLMFactory.get_model_name(provider_config, provider_type)

        return client, model_name