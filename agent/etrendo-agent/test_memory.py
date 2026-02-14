import sys
import os

# Add current directory to path so we can import agent
sys.path.append(os.getcwd())

from agent.agent import run_agent_query

def test_memory():
    print("Test 1: Sending first message...")
    response1, session_id1, logs1 = run_agent_query("Hi, I am AeroPress and I am selling coffee machines.")
    print(f"Response 1: {response1}")
    print(f"Session ID: {session_id1}")

    print("\nTest 2: Sending second message with SAME session_id...")
    response2, session_id2, logs2 = run_agent_query("What is my seller name and what am I selling?", session_id=session_id1)
    print(f"Response 2: {response2}")
    print(f"Session ID 2: {session_id2}")
    
    assert session_id1 == session_id2
    print("\nSUCCESS: Session ID preserved!")

if __name__ == "__main__":
    try:
        test_memory()
    except Exception as e:
        print(f"\nERROR: {e}")
