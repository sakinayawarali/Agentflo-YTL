# Agentflo ADK - Intelligent Voice Agent Platform

This repository houses the Agentflo Agent Development Kit (ADK), a powerful framework for building and deploying intelligent voice agents. It leverages a modular architecture to enable seamless integration with various tools and services, facilitating complex conversational flows and automated tasks.

## Use Case

The Agentflo ADK is designed for businesses and developers who need to create sophisticated AI-powered voice assistants capable of:

*   **Automated Customer Service**: Handling inquiries, providing information, and resolving common issues without human intervention.
*   **Sales and Order Management**: Assisting customers with product discovery, drafting orders, applying promotions, and managing profiles.
*   **Personalized Recommendations**: Offering tailored product or service suggestions based on user preferences and historical data.
*   **Interactive Voice Experiences**: Building dynamic and engaging voice interfaces for various applications.

## Functionality and Workflows

The Agentflo ADK operates through a robust agent-based system, orchestrating interactions between users, various AI agents, and a suite of specialized tools.

### Core Components:

*   **Voice Agent**: The primary interface for voice interactions, responsible for Speech-to-Text (STT) processing, natural language understanding (NLU), and Text-to-Speech (TTS) generation.
*   **Orchestrator Agent**: Manages the overall conversational flow, determines the user's intent, and delegates tasks to specialized tools or other agents.
*   **Specialized Tools**: A collection of modular tools that agents can invoke to perform specific actions, such as:
    *   **Database Queries**: Interacting with databases (e.g., Supabase) for data retrieval and manipulation.
    *   **Order Drafting**: Creating and managing customer orders.
    *   **Profile Management**: Accessing and updating user profiles.
    *   **Promotions**: Applying and managing promotional offers.
    *   **Recommender**: Providing personalized product or service recommendations.
    *   **Semantic Search**: Performing intelligent searches based on meaning and context.
    *   **TTS Tool**: Generating speech output for the voice agent.
    *   **AWS Lambda Schema**: Integration with AWS Lambda functions for extended functionality.

### Workflow:

1.  **User Voice Input**: A user speaks to the voice agent.
2.  **Speech-to-Text (STT)**: The voice agent converts the audio input into text.
3.  **Natural Language Understanding (NLU)**: The orchestrator agent analyzes the text to understand the user's intent and extract relevant entities.
4.  **Tool/Agent Invocation**: Based on the NLU, the orchestrator agent decides which specialized tool or other agent (e.g., order drafting agent, recommender agent) needs to be invoked.
5.  **Tool Execution**: The selected tool performs its specific function (e.g., queries a database, drafts an order, fetches promotions).
6.  **Response Generation**: The tool's output is processed, and a natural language response is formulated.
7.  **Text-to-Speech (TTS)**: The voice agent converts the text response back into speech.
8.  **Voice Output to User**: The spoken response is delivered to the user.

## Local Development

This project uses `invoke` for task automation.

1.  **Install Invoke**:
    ```bash
    pip install invoke
    ```
2.  **Set Project ID**:
    ```bash
    export GOOGLE_CLOUD_PROJECT=<GCP_PROJECT_ID>
    ```
3.  **Start the server with hot reload**:
    ```bash
    invoke dev
    ```

## Deployment

This project is designed for deployment on Cloud Run. Refer to the original Cloud Run Template Microservice documentation for detailed deployment instructions, including enabling APIs, creating Artifact Registry repositories, and configuring Docker.

## Project Structure

*   `agents/`: Contains the core agent logic, including `agent.py`, `orchestrator_agent.py`, and `voice_agent.py`.
*   `agents/tools/`: Houses various specialized tools (e.g., `order_draft_tools.py`, `recommender_tool.py`).
*   `prompts/`: Stores prompt templates for agents.
*   `app.py`: The main Flask application entry point.
*   `route_handlers.py`: Defines API routes and their handlers.
*   `audio_helper.py`, `upliftai_voice_helper.py`: Utilities for audio and voice processing.
*   `requirements.txt`: Project dependencies.
*   `tasks.py`: Invoke tasks for development and deployment.

## Maintenance & Support

Please use the issue tracker for bug reports, feature requests, and submitting pull requests.

## Contributions

Please see the [contributing guidelines](CONTRIBUTING.md).

## License

This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE).
