from datetime import datetime
import time
start_waiting = datetime.utcnow()

time.sleep(1)

current_waiting = datetime.utcnow()
print (current_waiting - start_waiting).seconds