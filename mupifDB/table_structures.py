
def extendRecord(record, structure):
    for def_key in structure.keys():
        if def_key not in record:
            record[def_key] = structure[def_key]
    return record


tableExecution = {
    'WorkflowID': None,
    'WorkflowVersion': None,
    'Status': "Created",
    'CreatedDate': None,
    'SubmittedDate': None,
    'StartDate': None,
    'EndDate': None,
    'ExecutionLog': None,
    'RequestedBy': None,
    'Inputs': None,
    'Outputs': None,
    'Task_ID': '',
    'label': ''
}
