"""
Streamlit UI for AI Query System
Quick chat interface with lineage visibility
"""

import streamlit as st
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from main_pipeline import AIQuerySystem
from layers.layer6_storyteller import QueryResponse


# Page configuration
st.set_page_config(
    page_title="AI Query System",
    page_icon="🤖",
    layout="wide"
)


def initialize_session_state():
    """Initialize Streamlit session state."""
    if "query_system" not in st.session_state:
        with st.spinner("Initializing AI Query System..."):
            try:
                st.session_state.query_system = AIQuerySystem()
                st.success("System initialized successfully!")
            except Exception as e:
                st.error(f"Initialization failed: {str(e)}")
                st.session_state.query_system = None

    if "messages" not in st.session_state:
        st.session_state.messages = []


def display_lineage(lineage):
    """Display lineage trace in an expander."""
    import json

    with st.expander("📊 View Lineage Trace", expanded=False):
        col1, col2 = st.columns(2)

        with col1:
            st.write("**Routing**")
            st.write(f"- Route: `{lineage.route}`")
            st.write(f"- Cache hit: {'Yes' if lineage.cache_hit else 'No'}")
            if lineage.cache_similarity:
                st.write(f"- Similarity: {lineage.cache_similarity:.2%}")

        with col2:
            st.write("**Data Sources**")
            st.write(f"- Tables used: {', '.join(lineage.tables_used) or 'None'}")
            st.write(f"- Schemas retrieved: {', '.join(lineage.schemas_retrieved) or 'None'}")
            st.write(f"- Documents retrieved: {', '.join(lineage.documents_retrieved) or 'None'}")

        st.write("**SQL Query**")
        if lineage.sql_run:
            st.code(lineage.sql_run, language="sql")
        else:
            st.write("No SQL query executed")

        st.write("**Timing**")
        st.write(f"- Execution time: {lineage.execution_time_ms:.2f}ms")

        st.write("**Full JSON**")
        st.json(json.loads(lineage.to_json()))


def main():
    """Main Streamlit application."""
    st.title("🤖 AI Query System")
    st.markdown("*Natural language to SQL/Data pipeline*")

    # Initialize
    initialize_session_state()

    # Sidebar
    with st.sidebar:
        st.header("System Info")

        if st.session_state.query_system:
            # Health check
            st.subheader("Health Status")
            health = st.session_state.query_system.health_check()
            for component, status in health.items():
                emoji = "✅" if status else "❌"
                st.write(f"{emoji} {component.title()}")

            # Stats
            st.subheader("Statistics")
            stats = st.session_state.query_system.get_stats()
            st.write(f"Cache entries: {stats['cache_stats']['total_entries']}")
            st.write(f"Schemas loaded: {stats['tag_collections']['schemas']}")
            st.write(f"Documents indexed: {stats['tag_collections']['documents']}")

            # Recent queries
            if stats["recent_lineage"]:
                st.subheader("Recent Queries")
                for log in stats["recent_lineage"][-5:]:
                    st.caption(f"• {log['query'][:50]}...")
        else:
            st.warning("System not initialized")

        st.divider()

        # Clear cache button
        if st.button("🗑️ Clear Cache"):
            if st.session_state.query_system:
                count = st.session_state.query_system.cache.clear()
                st.success(f"Cleared {count} cache entries")
                st.rerun()

    # Main chat interface
    st.header("Ask Questions")

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])
            if "lineage" in message:
                display_lineage(message["lineage"])

    # Chat input
    if prompt := st.chat_input("Ask a question about your data..."):
        # Add user message
        st.session_state.messages.append({
            "role": "user",
            "content": prompt
        })
        with st.chat_message("user"):
            st.write(prompt)

        # Get response
        if st.session_state.query_system:
            with st.chat_message("assistant"):
                with st.spinner("Processing query..."):
                    try:
                        response = st.session_state.query_system.run_pipeline(prompt)

                        st.write("### Answer")
                        st.write(response.answer)

                        display_lineage(response.lineage)

                        # Store in history
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": response.answer,
                            "lineage": response.lineage
                        })

                    except Exception as e:
                        st.error(f"Query failed: {str(e)}")
        else:
            st.error("System not initialized")

    # Example queries
    st.divider()
    st.header("Example Queries")

    col1, col2, col3 = st.columns(3)

    examples = [
        ("SQL Query", "How many customers do we have?"),
        ("Data Query", "What is the total revenue?"),
        ("List Data", "Show me recent orders")
    ]

    for col, (name, query) in zip([col1, col2, col3], examples):
        with col:
            if st.button(f"Try: {query}", key=f"example_{name}"):
                st.session_state.messages.append({
                    "role": "user",
                    "content": query
                })
                st.rerun()


if __name__ == "__main__":
    main()
