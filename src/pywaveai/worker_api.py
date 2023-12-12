from pywaveai.runtime import TaskExectionInfo


def worker_api(supported_tasks: list[TaskExectionInfo]):
    """
    Generate a dictionary containing information about the supported tasks.

    Args:
        supported_tasks (list[TaskType]): A list of supported task types.

    Returns:
        dict: A dictionary containing information about the supported tasks.
            Each task is represented as a dictionary with the following keys:
            - "name": The name of the task type.
            - "options": The schema of the task type's options.
            - "options_class": The name of the task type's options class.
            - "result": The schema of the task type's result.
            - "result_class": The name of the task type's result class.
            - "doc": The documentation of the task type.
    """
    tasks = []
    for task_type in supported_tasks:
        tasks.append({
            "name": task_type.task_name,
            "options": task_type.options_type.model_json_schema(),
            "options_class": task_type.options_type.__name__,
            "result": task_type.result_type.model_json_schema(),
            "result_class": task_type.result_type.__name__,
            "doc": task_type.documantation,
        })
    return {
        "tasks": tasks
    }