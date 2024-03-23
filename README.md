# PYWAVEAI
Python library for creating WaveAI workers


## Development

### Create virtual environment

```bash
python3.11 -m venv venv

# Activate virtual environment
source venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

## Using samples

### Runing echo sample

```bash
python samples/echo.py
```

see test api under http://localhost:8000/docs


### Runing echo tests 
```bash
TEST='samples/echo/tests' python samples/echo/main.py
```

pywaveai using environment variables for configuration enable test mode. The argument list is not used to avoid conflicts with application arguments.


The `TEST` environment variable is used to specify the path to the test directory. The test directory must contain the test cases. Each test is directory with `options.json` and files used in `options.json` files section. The case name `task_type-id` is used to identify the task type and task id. 


```
├── tests
│   ├── echo_dev-test1
│   │   ├── options.json
│   │   ├── test1.wav
│   │   └── test2.wav
│   └── echo_dev-test2
│       ├── options.json
│       ├── test1.wav
│       └── test2.wav
```



The result will be saved in same directory. The result will be written to `result.json` file. The result files will start with `result_` prefix.
The error will be written to `error.json` file. And tests will be stopped.

The etalon result can be placed under `expected.json` file. The test will be failed if the result is different from the etalon

You can have `.gitignore` file in test directory to ignore result files.

```.gitignore
# Ignore result files
result_*
result.json
error.json
```

You can override defaulting test result comparison by setting the comparison function before running the worker.

```python
...

from pywaveai.testing import set_test_result_processor

def test_processor(task, task_dir):
    """
    :param task: Task object
    :param task_dir: Task directory
    :return: True if test passed, False otherwise
    """
    # Compare result and etalon
    return True

set_test_result_processor(test_processor)
```

