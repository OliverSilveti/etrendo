import streamlit as st
import sys
import os
import time

# Add current directory to path so we can import agent modules
# This assumes ui.py is run from agent/etrendo-agent/ directory
sys.path.append(os.getcwd())

from agent.agent import run_agent_query

st.set_page_config(page_title="Etrendo Revenue Agent", page_icon="🛒")

st.title("Etrendo Revenue Agent 🛒")
st.markdown("Ask about your sales, pricing, and buy box status.")

# Function to simulate streaming response
def stream_data(text):
    for word in text.split(" "):
        yield word + " "
        time.sleep(0.02)

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Initialize agent session ID (for memory)
if "agent_session_id" not in st.session_state:
    st.session_state.agent_session_id = None

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Accept user input
if prompt := st.chat_input("How is my coffee machine sales?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        response = None
        # Use status container to show processing
        with st.status("Analyzing market data...", expanded=True) as status:
            try:
                # Call the agent with history (session_id)
                response, session_id, logs = run_agent_query(prompt, st.session_state.agent_session_id)
                
                # Update session ID (in case it was None or changed)
                st.session_state.agent_session_id = session_id
                
                # Mark status as complete and collapse
                status.update(label="Analysis Complete", state="complete", expanded=False)
            except Exception as e:
                status.update(label="Error", state="error")
                st.error(f"Error: {e}")
                logs = []
        
        if response:
            # Stream the response OUTSIDE the status block
            st.write_stream(stream_data(response))
            
            # Add assistant response to chat history
            st.session_state.messages.append({"role": "assistant", "content": response})
        
        # Show debug logs
        with st.expander("Debug Logs"):
            if 'logs' in locals():
                for log in logs:
                    st.text(log)
            else:
                st.text("No logs available.")
