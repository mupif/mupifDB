window.exec_check_status_sum = 0;

function reloadIfExecStatusIsChanged(){
    window.exec_check_status_sum += 1;

    if(window.exec_check_status_sum < 20){
        console.log('Checking update of execution');

        let xmlhttp=new XMLHttpRequest();
        xmlhttp.onreadystatechange=function() {
            if (this.readyState===4 && this.status===200) {
                jsn = JSON.parse(this.responseText)
                if('result' in jsn){
                    if('Status' in jsn['result']){
                        if(jsn['result']['Status'] == 'Finished'){
                            location.reload();
                        }
                    }
                }
                console.log('Reload not needed.');
            }
        };
        xmlhttp.open("GET", "/restapi/?action=get_execution&id=" + window.execution_id, true);
        xmlhttp.send();
    }
}