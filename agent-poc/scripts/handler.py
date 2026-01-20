from typing import Optional

class ThinkingCallbackHandler:
    def __init__(self, thinking_color: str = "\033[90m", output_color: str = "\033[0m"):
        """
        Initialize the ThinkingCallbackHandler.

        Args:
            thinking_color: ANSI color code for reasoning/thinking text. Default is gray (\033[90m).
            output_color: ANSI color code for standard output text. Default is reset (\033[0m).
        """
        self.tool_count = 0
        self.previous_tool_use = None
        self.thinking_color = thinking_color
        self.output_color = output_color
        self.reset_color = "\033[0m"
    
    def __call__(self, **kwargs):
        reasoning_text = kwargs.get("reasoningText")
        data = kwargs.get("data")
        complete = kwargs.get("complete")
        current_tool_use = kwargs.get("current_tool_use", {})
        
        # 1. Handle Thinking/Reasoning
        if reasoning_text:
            # Print reasoning in specified thinking color
            print(f"{self.thinking_color}{reasoning_text}{self.reset_color}", end="", flush=True)
        
        # 2. Handle Text Data
        if data:
            # Print output in specified output color
            # If output_color is reset (\033[0m), it's redundant to wrap, but consistent.
            # If it's a specific color (e.g. blue), we need to reset after.
            print(f"{self.output_color}{data}{self.reset_color}", end="" if not complete else "\n", flush=True)

        # 3. Handle Tool Use (Restored functionality)
        if current_tool_use and current_tool_use.get("name"):
            # Check if this is a new tool call or continuation of the same one
            if self.previous_tool_use != current_tool_use:
                self.previous_tool_use = current_tool_use
                self.tool_count += 1
                tool_name = current_tool_use.get("name", "Unknown tool")
                # Tool info can remain default or use a specific color if needed, keep default for now
                print(f"\nTool #{self.tool_count}: {tool_name}.")
        
        # 4. Handle Completion
        if complete and data:
            print("\n")

def print_token_usage(usage: dict, label: str = "Token Usage"):
    """
    Helper to print token usage including cache stats if available.
    """
    if not usage:
        return

    print(f"\n[{label}]:")
    
    # Standard tokens
    input_tokens = usage.get('inputTokens', 0)
    output_tokens = usage.get('outputTokens', 0)
    total_tokens = usage.get('totalTokens', 0)
    
    print(f"  Input: {input_tokens}")
    print(f"  Output: {output_tokens}")
    
    # Cache tokens (Bedrock/Claude usually use camelCase, but checking snake_case too)
    cache_read = usage.get('cacheReadInputTokens') or usage.get('cache_read_input_tokens')
    cache_write = usage.get('cacheWriteInputTokens') or usage.get('cache_creation_input_tokens') or usage.get('cacheWriteTokens')
    
    if cache_read is not None:
        print(f"  Cache Read: {cache_read}")
    
    if cache_write is not None:
        print(f"  Cache Write: {cache_write}")
        
    print(f"  Total: {total_tokens}")
