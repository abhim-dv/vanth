import time

from vanth.agent_events import agent_event, progress


for step in range(1, 4):
    time.sleep(0.5)
    progress(step, 3, unit="step", stage="demo", message=f"step {step}/3")

agent_event("checkpoint", "demo checkpoint", step=3)
time.sleep(0.5)
