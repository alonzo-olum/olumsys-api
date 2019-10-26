<?php

$contentServices = array('22677', '22554','22088'); 
$textServices = array('61026','21026','22567','21735');
$daytoday = date("Y-m-d: H:i:s", time());
$report_log = "/root/logs/roamtech_report.log";

$logfile = fopen($report_log, 'a');

getServiceData($contentServices, $logfile, $daytoday);
#getServiceData($textServices, $logfile, $daytoday);



// the critical func lies herein:
function getServiceData($services,$logfile,$daytoday){
	foreach($services as $s){
		$serv_param = $s;
		$serv_urls = array('get_msg','get_sub');
		foreach($serv_urls as $serv){
			$serv_url = "curl -i \"http://localhost:5000/$serv/$serv_param\"";
			print_r($serv_url);
			$output = shell_exec($serv_url);
		}
		if ($logfile){
			$msg = "[" . $daytoday . "]" . $output;
			fwrite($logfile, $msg);

		}
		else{
			echo "the log file could not be opened!";

		}
		echo $output . "\n";
	}

}


?>
