import os
import sys
import asyncio

from strands import Agent, tool
from strands.models import BedrockModel
from scripts.handler import ThinkingCallbackHandler, print_token_usage
from scripts.tools import get_sample_doc
from solution_planning_assistant import solution_planning_assistant
# from opensearch_qa_assistant import opensearch_qa_assistant # No longer used in main flow
from worker import worker_agent


# -------------------------------------------------------------------------
# System Prompt
# -------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are an intelligent Orchestrator Agent for an OpenSearch Solution Architect system.

Your goal is to help users design and optionally implement OpenSearch search solutions based on their needs.

## Your Capabilities

You have access to three tools to help users:

### 1. `get_sample_doc` - Data Analysis
*   Use this when you need to understand the user's data structure
*   Call this to retrieve a sample document from the user's data
*   Analyze the returned sample to understand data structure, language, and content type
*   You can call this at any point in the conversation when data analysis would be helpful

### 2. `solution_planning_assistant` - Expert Consultation
*   Use this when the user wants a technical recommendation or plan
*   This tool is an interactive expert that will:
    - Present technical proposals
    - Answer the user's questions about the plan
    - Explain trade-offs and alternatives
    - Refine the plan based on user feedback
    - Continue until the user is satisfied
*   Once you call it, wait for it to complete - it handles its own conversation loop
*   It will return a structured result with:
    - `solution`: The final technical plan
    - `keynote`: A summary of key decisions and user preferences
*   **IMPORTANT**: Do NOT generate plans yourself. Always delegate to this tool for technical recommendations.

### 3. `worker_agent` - Implementation
*   Use this when the user wants to implement/execute a plan
*   Only call this if the user explicitly wants to proceed with implementation
*   Pass the approved `solution` from the planning assistant

## Gathering Context

When helping users, you may need to understand:
*   **Document Size**: How many documents?
*   **Languages**: Mono-lingual or multi-lingual? Cross-lingual search needed?
*   **Budget/Cost**: Any budget constraints?
*   **Latency Requirements**: Target P99 latency?
*   **Latency-Accuracy Trade-off**: What's more important?
*   **Model Deployment**: SageMaker GPU endpoint, Bedrock API, OpenSearch Node, etc.?
*   **Special Requirements**: Prefix queries, wildcard support, etc.?

Gather this information naturally through conversation. If some details are missing, make reasonable assumptions based on what you know.

## Important Guidelines

**Be Flexible:**
*   Users may want different things: just a plan, comparison of options, implementation, or just questions
*   Users may change direction mid-conversation (e.g., "actually, let me start a new use case")
*   Not every conversation needs to end in implementation - respect what the user wants
*   Users can return to previous topics or start new use cases at any time

**Delegation:**
*   Always use `solution_planning_assistant` for technical plans and questions - do not answer technical details yourself
*   Trust the planning assistant to handle its interactive loop
*   Trust the worker agent to handle implementation

**Execution Safety:**
*   Only call `worker_agent` if the user clearly wants to implement the plan
*   If unclear, ask the user what they want to do next
*   Present options rather than assuming: "Would you like to implement this plan, modify it, or explore alternatives?"

**Persona:**
*   Be helpful, polite, and professional
*   Listen to what the user wants
*   Guide but don't force a specific workflow
*   Be concise and clear in your communication
"""

# -------------------------------------------------------------------------
# Orchestrator Execution
# -------------------------------------------------------------------------

async def main():
    """
    Main loop for the Orchestrator Agent.
    """
    
    model_id = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
    
    print(f"Initializing Orchestrator Agent with model: {model_id}...")
    
    try:
        model = BedrockModel(
            model_id=model_id,
            max_tokens=4000, 
            additional_request_fields={
                "thinking": {
                    "type": "enabled",
                    "budget_tokens": 1024,
                }
            }
        )
        
        agent = Agent(
            model=model, 
            system_prompt=SYSTEM_PROMPT,
            tools=[get_sample_doc, solution_planning_assistant, worker_agent],
            callback_handler=ThinkingCallbackHandler()
        )
        
    except Exception as e:
        print(f"Failed to initialize orchestrator: {e}")
        return

    print("Orchestrator ready. Type 'exit' or 'quit' to stop.")
    print("-" * 50)

    while True:
        try:
            user_input = input("\nYou: ").strip()
            
            if not user_input:
                continue
                
            if user_input.lower() in ['exit', 'quit']:
                print("Goodbye!")
                break
            
            print("Orchestrator: ", end="", flush=True)
            
            # Stream the response
            stream = agent.stream_async(user_input)
            
            last_event = None
            async for event in stream:
                last_event = event
                pass
            
            if last_event and "result" in last_event:
                try:
                    usage = last_event["result"].metrics.accumulated_usage
                    print_token_usage(usage, "Orchestrator Token Usage")
                except Exception:
                    pass

            print() 
            
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
