#!/bin/bash
set -e -x
##
## simulates web browser: uploading a new workflow, creating execution, waiting for it to finish, checking inputs/outpus, downloading the log
##

export SRV=http://127.0.0.1:11004
export CURL="curl --fail --no-progress-meter -o /dev/null"
if true; then
	for WHAT in "" about status usecases workflows workflowexecutions contact workflow_add/1; do
		$CURL $SRV/$WHAT
	done
fi
# upload a new workflow file to usecase 1 (assuming it exists)
URL=$( curl $SRV/workflow_add/1 -F file_workflow=@../workflows/mini01.py -F file_add_1=@../workflows/workflowdemo01.py -F file_add_2=@../workflows/workflowdemo02.py -o /tmp/aa.html -L -w %{url_effective} )
WID=`echo $URL | cut -d/ -f5`
WVERSION=$( curl $SRV/workflows/$WID --fail --no-progress-meter | grep -Po 'Version:</td><td>\K[0-9]+(?=</td>)' )
# intialize new execution record of the lateest version, get its WEID
WEID=$( $CURL $SRV/workflowexecutions/init/$WID/$WVERSION -L -w %{url_effective} | cut -d/ -f5 )
# set inputs via form (once there are some inputs):
# curl $SRV/workflowexecutions/$WEID/inputs
$CURL $SRV/executeworkflow/$WEID
for i in `seq 20`; do
	sleep 1
	STATUS=$( curl $SRV/workflowexecutions/$WEID --fail --no-progress-meter | grep -Po 'Status:</td><td>\K[^<]+(?=</td>)' )
	if [ "$STATUS" = "Finished" ]; then break; fi
done
[ "$STATUS" == "Finished" ] || exit 1
sleep 1
LOGID=$( curl $SRV/workflowexecutions/$WEID --fail --no-progress-meter | grep -Po 'a href="/file/\K[a-z0-9]+(?="> Execution log</a>)' )
$CURL $SRV/workflowexecutions/$WEID/inputs
$CURL $SRV/workflowexecutions/$WEID/outputs
LOGSIZE=$( $CURL $SRV/file/$LOGID -w %{size_download} )
echo "Logfile is $LOGSIZE bytes."

