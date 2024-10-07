from typing import Any

def extendRecord(record: dict[str,Any], structure: dict[str,Any]) -> dict[str,Any]:
    for def_key in structure.keys():
        if def_key not in record:
            record[def_key] = structure[def_key]
    return record


tableUseCase: dict[str,Any] = {
    'ucid': None,
    'Description': None,
}


tableWorkflow: dict[str,Any] = {
    'wid': None,
    'Description': None,
    'UseCase': None,
    'Version': None,
    'GridFSID': None,
    'modulename': None,
    'classname': None,
    'IOCard': {'Inputs': [], 'Outputs': []},
    'Models': [],
    'EDMMapping': []
}


tableExecution: dict[str,Any] = {
    'WorkflowID': None,
    'WorkflowVersion': None,
    'Status': "Created",
    'CreatedDate': None,
    'SubmittedDate': None,
    'StartDate': None,
    'EndDate': None,
    'ExecutionLog': None,
    'RequestedBy': None,
    'UserIP': None,
    'Inputs': None,
    'Outputs': None,
    'Task_ID': '',
    'label': '',
    'Attempts': 0,
    'EDMMapping': []
}


tableUser: dict[str,Any] = {
    'IP': None,
    'Name': None,
    'Organization': None,
    'Rights': None,
}
