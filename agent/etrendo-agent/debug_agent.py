
import os
import vertexai
from google.adk.agents import LlmAgent
from google.adk.apps import App
from google.adk.runners import Runner
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.genai import types
import yaml
import uuid

def debug():
    print("Step 1: Loading config...")
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    project_id = config["vertex_ai"]["project_id"]
    location = config["vertex_ai"]["location"]
    model_name = config["vertex_ai"]["model_name"]
    
    print(f"Config: {project_id}, {location}, {model_name}")

    print("\nStep 2: Initializing Vertex AI...")
    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
    os.environ["GOOGLE_CLOUD_LOCATION"] = location
    vertexai.init(project=project_id, location=location)

    print("\nStep 3: Creating Agent...")
    agent = LlmAgent(
        name="debug_agent",
        model=model_name,
        instruction="You are a helpful assistant."
    )
    
    app = App(name="debug_app", root_agent=agent)
    session_service = InMemorySessionService()
    runner = Runner(app=app, session_service=session_service)

    print("\nStep 4: Creating Session EXPLICITLY...")
    session_id = str(uuid.uuid4())
    user_id = "debug-user"
    
    session_service.create_session_sync(
        app_name=app.name,
        user_id=user_id,
        session_id=session_id,
    )
    print(f"Session {session_id} created.")

    print("\nStep 5: Running Test Query...")
    try:
        events = runner.run(
            user_id=user_id,
            session_id=session_id,
            new_message=types.Content(
                role="user", parts=[types.Part.from_text(text="Hi, are you there?")]
            ),
        )
        
        has_events = False
        for event in events:
            has_events = True
            print(f"Event: {event}")
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        print(f"\n✅ SUCCESS! AI responded: {part.text}")
        
        if not has_events:
            print("FAILED: No events returned from runner.")
            
    except Exception as e:
        print(f"CRASH: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug()
