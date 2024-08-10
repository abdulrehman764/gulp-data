from IPython import get_ipython

# Get the IPython history object
ipython = get_ipython()
history = ipython.history_manager

# Retrieve all input/output history
for session_id, line_number, input in history.get_range():
    print(f'In [{session_id}:{line_number}]: {input}')

# Retrieve all output history
for output in ipython.user_ns['_oh']:
    print(f'Out: {ipython.user_ns["_oh"][output]}')
