import json
from importlib import import_module


def handler(event, context):
    print(f'receive request, event: {event}')

    module_name = event['module_name']
    module = import_module(module_name)

    print(f'module: {module} imported')

    method_name = event['method_name']
    arg_map = event['arg_map'] if 'arg_map' in event else None
    if arg_map is None or not arg_map:
        result = eval(f'module.{method_name}')()
    else:
        result = eval(f'module.{method_name}')(**arg_map)
    return {
        'statusCode': 200,
        'body': {
            'result': result
        }
    }


if __name__ == '__main__':
    with open('event.json') as event_file:
        event = json.load(event_file)
    result = handler(event, None)
    print(result)

