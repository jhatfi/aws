import argparse
from datetime import datetime
import json
import boto3
import os
from tools.utils import GluePythonShellLogger

client = boto3.client('stepfunctions')
now = datetime.now()
date_time = now.strftime("%Y%m%d-%H%M")
outtag = os.getenv('ENVIRONMENT_TAG')
execution_arn = os.getenv('EXECUTION_ARN')
logger = GluePythonShellLogger(name='{} {}'.format(__name__, outtag))


def main():
    parser = argparse.ArgumentParser(description='Execution Arn of the failed state machine.')
    args = parser.parse_args()
    failed_state_machine_state, failed_state_info = parse_failure_history(execution_arn)
    failed_state_machine_arn = get_state_machine_arn_from_execution_arn(execution_arn)
    new_machine = attach_go_to_state(failed_state_machine_state, failed_state_machine_arn)
    new_machine_arn = new_machine['stateMachineArn']
    logger.info(f'New State Machine Arn: {new_machine_arn}')
    logger.info(f'Execution had failed at state: {failed_state_machine_state} with Input: {failed_state_info}')
    start_new_state_machine_execution(new_machine_arn)

def get_state_machine_arn_from_execution_arn(execution_arn: str) -> str:
    sm_arn = execution_arn.split(':')[:-1]
    sm_arn[5] = 'stateMachine'
    return ':'.join(sm_arn)


def parse_failure_history(failed_execution_arn):
    logger.info(f'Parsing failure history for arn: {failed_execution_arn}')
    failed_at_parallel_state = False

    try:
        # Get the execution history
        failed_events = paginate_execution(failed_execution_arn)
    except Exception as e:
        raise Exception(f'Unable to get execution history: {e}')

    for failed_event in failed_events:
        # Confirm that the execution actually failed, raise exception if it didn't fail
        if not failed_event or len(failed_event) < 1:
            raise Exception('no failed events')
        logger.info(f'failed_event: {failed_event}')
        failed_event_details = failed_event[0]['executionFailedEventDetails']
        logger.info(f'failed event details: {failed_event_details}')
        if not failed_event_details:
            raise Exception(f'unexpected failed event: {failed_event}')
        '''
        If we have a 'States.Runtime' error (for example if a task state in our state
        machine attempts to execute a function in a different region than the
        state machine), get the id of the failed state, use id of the failed state to
        determine failed state name and input
        '''
        if failed_event_details['error'] == 'States.Runtime':
            failed_id = int(filter(str.isdigit, str(failed_event[0]['executionFailedEventDetails']['cause'].split()[13])))
            failed_state = failed_event[-1 * failed_id]['stateEnteredEventDetails']['name']
            failed_input = failed_event[-1 * failed_id]['stateEnteredEventDetails']['input']
            return failed_state, failed_input
        current_event_id = failed_event[0]['id']
        while current_event_id != 0:
            # multiply event id by -1 for indexing because we're looking at the reversed history
            current_event = failed_event[-1 * current_event_id]
            if current_event['type'] == 'ParallelStateFailed':
                failed_at_parallel_state = True
            if current_event['type'] == 'TaskStateEntered' and not failed_at_parallel_state:
                failed_state = current_event['stateEnteredEventDetails']['name']
                failed_input = current_event['stateEnteredEventDetails']['input']
                return failed_state, failed_input
            if current_event['type'] == 'ParallelStateEntered' and failed_at_parallel_state:
                failed_state = current_event['stateEnteredEventDetails']['name']
                failed_input = current_event['stateEnteredEventDetails']['input']
                return failed_state, failed_input
            # Update the id for the next execution of the loop
            current_event_id = current_event['previousEventId']


def attach_go_to_state(failed_state_name, state_machine_arn):
    logger.info(f'Creating new state machine with go to position from failed step: {failed_state_name}')
    try:
        response = client.describe_state_machine(
            stateMachineArn=state_machine_arn
        )
    except Exception as cause:
        raise Exception(f'Could not get ASL definition of state machine: {cause}')
    role_arn = response['roleArn']
    state_machine = json.loads(response['definition'])
    # Create a name for the new state machine
    new_name = response['name'] + '-resumeState-' + date_time
    # Get the StartAt state for the original state machine, because we will point the 'GoToState' to this state
    original_start_at = state_machine['StartAt']
    go_to_state = {
        'Type': 'Choice',
        'Choices': [{'Variable': '$.resuming', 'BooleanEquals': False, 'Next': original_start_at}],
        'Default': failed_state_name
    }
    # Add GoToState to the set of states in the new state machine
    state_machine['States']['GoToState'] = go_to_state
    # Add StartAt
    state_machine['StartAt'] = 'GoToState'
    # Create new state machine
    try:
        response = client.create_state_machine(
            name=new_name,
            definition=json.dumps(state_machine),
            roleArn=role_arn
        )
    except Exception as e:
        raise Exception(f'Failed to create new state machine with GoToState: {e}')
    return response

def start_new_state_machine_execution(new_machine_arn):
    logger.info(f'Starting Execution of New State machine: {new_machine_arn}')
    try:
        response = client.start_execution(
        stateMachineArn=new_machine_arn,
        input="{\"resuming\": true}")
    except Exception as e:
        raise Exception(f'Failed to start Execution: {e}')

def paginate_execution(execution_arn):
        paginator = client.get_paginator('get_execution_history')
        response_iterator = paginator.paginate(
            executionArn=execution_arn,
            reverseOrder=True,
            includeExecutionData=True,
            PaginationConfig={
                'PageSize': 1000
            }
        )
        for results_page in response_iterator:
            logger.info(f'results_page: {results_page}')
            yield results_page['events']

if __name__ == '__main__':
    main()
