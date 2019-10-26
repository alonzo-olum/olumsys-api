<?php
/*
* $LastChangedDate: 2011-05-02 07:17:17 +0000 (Mon, 02 May 2011) $
* $LastChangedBy: jasondevj@gmail.com $
* $LastChangedRevision: 74 $
*/

include_once '../api/MchoiceUssdApi.php';
include_once 'logs.php';


try {
    $receiver = new MchoiceUssdApi();
    //create the receiver
    logFile("\nMessage Received...");
    //getting the message content
    $rtn = "{$receiver->getAddress()} :: {$receiver->getMessage()} :: {$receiver->getConversationId()}";
    logFile("Message: " . $rtn);


    $res = '';
    //sending a message
    if ($receiver->getMessageType() == NORMAL_MESSAGE) {
        $sender = new MchoiceUssdApi("http://127.0.0.1:8000/ussd/", "appid", "pass");

        $res = $sender->sendUssd($receiver->getAddress(), 'Thank You for your message', $receiver->getConversationId(), 'false');

    } else if ($receiver->getMessageType() == TERMINATE_MESSAGE) {
        logFile("Terminate message received address : " . $receiver->getAddress() . " conversationId : " . $receiver->getConversationId());
    }

    logFile("\nRESPONSE::: correlationId :" . $res->{'correlationId'} . ", statusCode :" . $res->{'statusCode'} . ", statusDescription :" . $res->{'statusDescription'});

}
catch (AppZoneException $ex) {
    //throws when failed sending or receiving the message
    logFile("ERROR: $ex");
}
