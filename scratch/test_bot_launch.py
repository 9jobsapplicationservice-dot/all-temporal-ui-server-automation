import os
import sys
import subprocess
from pathlib import Path

def test_bot_cli():
    print("Testing bot CLI launch...")
    workspace = Path(os.getcwd())
    env = os.environ.copy()
    env["PIPELINE_MODE"] = "true"
    env["PIPELINE_RUN_ID"] = "cli-test-run"
    env["PIPELINE_RUN_IN_BACKGROUND"] = "true" # Headless for speed in test
    
    # Use a dummy config or the existing one
    # We just want to see if it reaches the login stage
    
    cmd = [sys.executable, "linkdin_automation/runAiBot.py"]
    print(f"Running command: {' '.join(cmd)}")
    
    try:
        # Run for 30 seconds and capture output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            cwd=str(workspace)
        )
        
        output = []
        import time
        start = time.time()
        while time.time() - start < 30:
            line = process.stdout.readline()
            if line:
                print(f"BOT: {line.strip()}")
                output.append(line)
                if "Launching Chrome" in line:
                    print("SUCCESS: Bot reached Chrome launch stage!")
                    process.terminate()
                    return True
            if process.poll() is not None:
                break
        
        process.terminate()
        print("Bot did not reach Chrome launch stage within 30s.")
        return False
    except Exception as e:
        print(f"Error testing bot: {e}")
        return False

if __name__ == "__main__":
    test_bot_cli()
