<?php

namespace Islambey\RSMQ\Exception;

use Islambey\RSMQ\Exception;

class MessageToLongException extends Exception
{
    public function __construct()
    {
        parent::__construct('Message too long');
    }
}