window.exec_check_status_sum = 0;

function reloadIfExecStatusIsChanged(){
    window.exec_check_status_sum += 1;

    if(window.exec_check_status_sum < 20){
        console.log('Checking update of execution');

        let xmlhttp=new XMLHttpRequest();
        xmlhttp.onreadystatechange=function() {
            if (this.readyState===4 && this.status===200) {
                let jsn = JSON.parse(this.responseText);
                if('Status' in jsn){
                    if(jsn['Status'] === 'Finished' || jsn['Status'] === 'Failed'){
                        location.reload();
                    }
                }
                console.log('Reload not needed.');
            }
        };
        xmlhttp.open("GET", "/api/?executions/" + window.execution_id, true);
        xmlhttp.send();
    }
}