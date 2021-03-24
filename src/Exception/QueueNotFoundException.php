<?php

namespace Islambey\RSMQ\Exception;

use Islambey\RSMQ\Exception;

class QueueNotFoundException extends Exception
{
    /**
     * @var string
     */
    private $queueName;

    public function __construct(string $queueName)
    {
        parent::__construct('Queue not found.');

        $this->queueName = $queueName;
    }

    public function getQueueName(): string
    {
        return $this->queueName;
    }
}