<?php
/*
* $LastChangedDate: 2011-05-02 07:17:17 +0000 (Mon, 02 May 2011) $
* $LastChangedBy: jasondevj@gmail.com $
* $LastChangedRevision: 74 $
*/


define("NORMAL_MESSAGE", "X-USSD-Message");
define("TERMINATE_MESSAGE", "X-USSD-Terminate-Message");
define("ALIVE_MESSAGE", "X-USSD-Alive-Message");

class MchoiceUssdApi
{

    private $url;
    private $username;
    private $password;
    private $address;
    private $message;
    private $messageType;
    private $correlationId;
    private $conversationId;

    public function __construct()
    {
        $a = func_get_args();
        $i = func_num_args();
        if (method_exists($this, $f = '__construct' . $i)) {
            call_user_func_array(array($this, $f), $a);
        }
    }

    public function __construct0()
    {
        $arrHeaders = $this->getHeaders();
        $this->messageType = $arrHeaders['X-Message-Type'];
        $this->conversationId = $arrHeaders['X-Requested-Conversation-Id'];

        ///read the request body
        $body = @file_get_contents('php://input');

        $json = json_decode($body);

        //If the message type is ALIVE sends the Alive message
        if ($this->messageType == ALIVE_MESSAGE) {
            header("HTTP/1.1 202 Accepted");
            return;
        }

        $this->address = $json->{'address'};
        $this->message = $json->{'message'};
        $this->correlationId = $json->{'correlationId'};

        if (!((isset($this->address) && isset($this->correlationId)))) {
            throw new Exception("Some of the required parameters are not provided");
        }
    }

    /*
        * Creating the sender object
        * $url - sender url
        * $username
        * $password
        */
    public function __construct3($url, $username, $password)
    {
        $this->url = $url;
        $this->username = $username;
        $this->password = $password;
    }


    /*
    * Read the request Header
    */
    function getHeaders()
    {
        $headers = array();
        foreach ($_SERVER as $k => $v) {
            if (substr($k, 0, 5) == "HTTP_") {
                $k = str_replace('_', ' ', substr($k, 5));
                $k = str_replace(' ', '-', ucwords(strtolower($k)));
                $headers[$k] = $v;
                //                logFile("headers: ". $k . " : ".$v);
            }
        }
        return $headers;
    }

    /*
     * Sends the ussd message
     * $address - Address of the sender(phone number)
     * $message
     * $conversationId - this should be taken from the received request
     * $sessionTermination - must be set to true or false
     */
    public function sendUssd($address, $message, $conversationId, $sessionTermination = false)
    {
        $headers = array(
            'Content-type: application/json',
            'X-Requested-Encoding: UTF-8',
            'X-Requested-Conversation-ID:' . $conversationId,
            'X-Requested-Version: 1.0',
            $this->getAuthHeader());

        $postData = array('address' => $address, 'message' => $message, 'sessionTermination' => $sessionTermination);

        return $this->sendRequest($postData, $headers);
    }

    private function getAuthHeader()
    {
        $auth = $this->username . ':' . $this->password;
        $auth = base64_encode($auth);
        return 'Authorization: Basic ' . $auth;
    }

    /*
     * Creates the JSON object that's sent using cURL
     * $postData - request body
     * $header - request header
     */
    private function sendRequest($postData, $header)
    {
        $ch = curl_init($this->url);

        // Configuring curl options
        $options = array(
            CURLOPT_HTTPHEADER => $header,
            CURLOPT_POST => 1,
            CURLOPT_POSTFIELDS => json_encode($postData),
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_FOLLOWLOCATION => true
        );

        curl_setopt_array($ch, $options);
        $result = curl_exec($ch);
        curl_close($ch);
        return $this->handleResponse($result);
    }

    /*
     * Handles the response message
     */
    private function handleResponse($result)
    {
        $resp = json_decode($result);
        if ($result == "") {
            throw new AppZoneException
            ("Server URL is invalid", '500');
        } else if ($resp->{'statusCode'} == 'SBL-USSD-2000') {
            return $resp;
        } else {
            throw new AppZoneException($resp->{'statusDescription'}, $resp->{'statusCode'}, $resp);
        }
    }

    public function getAddress()
    {
        return $this->address;
    }

    public function getMessage()
    {
        return $this->message;
    }

    public function getMessageType()
    {
        return $this->messageType;
    }

    public function getCorrelationId()
    {
        return $this->correlationId;
    }

    public function getConversationId()
    {
        return $this->conversationId;
    }

}


class AppZoneException extends Exception
{
    var $code;
    var $response;
    var $statusMessage;

    public function __construct($message, $code, $response = null)
    {
        parent::__construct($message);
        $this->statusMessage = $message;
        $this->code = $code;
        $this->response = $response;
    }

    public function getStatusCode()
    {
        return $this->code;
    }

    public function getStatusMessage()
    {
        return $this->statusMessage;
    }

    public function getRawResponse()
    {
        return $this->response;
    }
}

