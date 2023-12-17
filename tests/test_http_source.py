from pywaveai.http_api import HTTPTaskSource, TaskExectionInfo, TaskInfo
from pywaveai.task import Task, TaskOptions, TaskResult
from pytest import fixture
from fastapi import FastAPI
import pytest 
from fastapi.testclient import TestClient

def tf(data: TaskOptions) -> TaskResult:
    return TaskResult()


    
@fixture
def execution_info():
    return TaskExectionInfo(task_name='test',
                             func=tf,
                             options_type=TaskOptions,
                             result_type=TaskResult,
                             documantation='test task')

@fixture
def http_source(execution_info: TaskExectionInfo):
    return HTTPTaskSource([
        execution_info
    ])

def test_http_build_api(http_source: HTTPTaskSource):
    api = http_source.build_api_router()
    assert api is not None


@fixture
def api_client(http_source: HTTPTaskSource):
    router = http_source.build_api_router()
    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        yield client

def test_api_docs(api_client: TestClient):
    response = api_client.get('/docs')
    response.raise_for_status()


def test_api_json(api_client: TestClient):
    response = api_client.get('/api.json')
    response.raise_for_status()    


@pytest.mark.asyncio
async def test_schedule_and_fetch(http_source: HTTPTaskSource, api_client: TestClient):
    response = api_client.post('/task/test/create', json={
    })

    response.raise_for_status()

    task_id = response.json()['id']


    response = api_client.post(f'/task/test/{task_id}/file/new', json={
        'filename': 'test.txt',
    })

    response.raise_for_status()

    upload_url = response.json()['url']

    response = api_client.put(upload_url, files={
        'file': ('test.txt', b'hello world')
    })

    response.raise_for_status()


    response = api_client.post(f'/task/test/{task_id}/schedule', json={
        'files': {
            'f': 'test.txt',
        },
    })

    response.raise_for_status()

    task_info = await http_source.fetch_task()

    assert task_info is not None

    assert task_info.task.id == task_id

    assert task_info.task_io_manager is not None

    t = task_info.task

    assert t.options.files['f'] == 'test.txt'

    io_manager = task_info.task_io_manager

    file_b = await io_manager.download_bytes(task_info.task, 'test.txt')

    assert file_b == b'hello world'

    url2 = await io_manager.upload_bytes(task_info.task, 'test2.txt', b'hello world 2')

    await io_manager.mark_task_completed(task_info.task, TaskResult(
        files={
            'f': url2,
        }
    ))

    response = api_client.get(f'/task/test/{task_id}/result')

    response.raise_for_status()

    file2 = response.json()['result']['files']['f']

    response = api_client.get(file2)

    response.raise_for_status()

    assert response.content == b'hello world 2'


@pytest.mark.asyncio
async def test_schedule_and_fetch_with_error(http_source: HTTPTaskSource, api_client: TestClient):
    response = api_client.post('/task/test/create', json={
    })

    response.raise_for_status()

    task_id = response.json()['id']

    response.raise_for_status()

    response = api_client.post(f'/task/test/{task_id}/schedule', json={})


    task_info = await http_source.fetch_task()

  
    io_manager = task_info.task_io_manager

    await io_manager.mark_task_failed(task_info.task, Exception('test error'))

    response = api_client.get(f'/task/test/{task_id}/result')

    assert response.status_code == 500

    assert response.json()['detail'] == 'test error'


    


