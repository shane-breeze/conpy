# Conpy

Call python functions on (CERN) HTCondor batch cluster - with tqdm progress bars.

## How to use

High-level interface use:

```
import conpy

tasks = [...] # list of dicts with keys ["task", "args", "kwargs"] run on each node as task(*args, **kwargs)

results = conpy.local_submit(tasks)
results = conpy.mp_submit(tasks, ncores=4)
results = conpy.sge_submit("name", "/tmp/conpy-temporaries", tasks=tasks, options="-q hep.q")
```

The return value is a list of results for each task, in order o tasks.
