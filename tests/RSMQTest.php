<?php

use Islambey\RSMQ\Exception\QueueAlreadyExistsException;
use Islambey\RSMQ\Exception\QueueNotFoundException;
use Islambey\RSMQ\RSMQ;

class RSMQTest extends PHPUnit\Framework\TestCase
{
    private const QUEUE_NAME = 'foo';

    /**
     * @var RSMQ
     */
    private $rsmq;

    public function setUp(): void
    {
//        $redis = new Redis();
//        $redis->connect('127.0.0.1', 6379);
//        $this->rsmq = new RSMQ($redis);

        $nodes = ['127.0.0.1:7001', '127.0.0.1:7002', '127.0.0.1:7003'];
        $redis = new RedisCluster(null, $nodes);
        $this->rsmq = new RSMQ($redis);
    }

    public function testScriptsShouldInitialized(): void
    {
        $reflection = new ReflectionClass($this->rsmq);

        $recvMsgRef = $reflection->getProperty('receiveMessageSha1');
        $recvMsgRef->setAccessible(true);

        $this->assertSame(40, strlen($recvMsgRef->getValue($this->rsmq)));

        $popMsgRef = $reflection->getProperty('popMessageSha1');
        $popMsgRef->setAccessible(true);

        $this->assertSame(40, strlen($popMsgRef->getValue($this->rsmq)));
    }

    public function testCreateQueue(): void
    {
        $this->assertTrue($this->rsmq->createQueue(self::QUEUE_NAME));
    }

    public function testCreateQueueWithInvalidName(): void
    {
        $this->expectException(\Islambey\RSMQ\Exception::class);
        $this->expectExceptionMessage('Invalid queue name');
        $this->rsmq->createQueue(' sad');
    }

    public function testCreateQueueWithBigVt(): void
    {
        $this->expectException(\Islambey\RSMQ\Exception::class);
        $this->expectExceptionMessage('Visibility time must be between');
        $this->rsmq->createQueue(self::QUEUE_NAME, PHP_INT_MAX);
    }

    public function testCreateQueueWithNegativeVt(): void
    {
        $this->expectException(\Islambey\RSMQ\Exception::class);
        $this->expectExceptionMessage('Visibility time must be between');
        $this->rsmq->createQueue(self::QUEUE_NAME, -1);
    }

    public function testCreateQueueWithBigDelay(): void
    {
        $this->expectException(\Islambey\RSMQ\Exception::class);
        $this->expectExceptionMessage('Delay must be between');
        $this->rsmq->createQueue(self::QUEUE_NAME, 30, PHP_INT_MAX);
    }

    public function testCreateQueueWithNegativeDelay(): void
    {
        $this->expectException(\Islambey\RSMQ\Exception::class);
        $this->expectExceptionMessage('Delay must be between');
        $this->rsmq->createQueue(self::QUEUE_NAME, 30, -1);
    }

    public function testCreateQueueWithBigMaxSize(): void
    {
        $this->expectException(\Islambey\RSMQ\Exception::class);
        $this->expectExceptionMessage('Maximum message size must be between');
        $this->rsmq->createQueue(self::QUEUE_NAME, 30, 0, PHP_INT_MAX);
    }

    public function testCreateQueueWithSmallMaxSize(): void
    {
        $this->expectException(\Islambey\RSMQ\Exception::class);
        $this->expectExceptionMessage('Maximum message size must be between');
        $this->rsmq->createQueue(self::QUEUE_NAME, 30, 0, 1023);
    }

    public function testGetQueueAttributes(): void
    {
        $vt = 40;
        $delay = 60;
        $maxSize = 1024;
        $this->rsmq->createQueue(self::QUEUE_NAME, $vt, $delay, $maxSize);

        $attributes = $this->rsmq->getQueueAttributes(self::QUEUE_NAME);

        $this->assertSame($vt, $attributes['vt']);
        $this->assertSame($delay, $attributes['delay']);
        $this->assertSame($maxSize, $attributes['maxsize']);
    }

    public function testGetQueueAttributesThatDoesNotExists(): void
    {
        $this->expectException(QueueNotFoundException::class);
        $this->expectExceptionMessage('Queue not found.');
        $this->rsmq->getQueueAttributes('not_existent_queue');
    }

    public function testCreateQueueMustThrowExceptionWhenQueueExists(): void
    {
        $this->expectException(QueueAlreadyExistsException::class);

        $this->rsmq->createQueue(self::QUEUE_NAME);
        $this->rsmq->createQueue(self::QUEUE_NAME);
    }

    public function testListQueues(): void
    {
        $this->assertNotContains(self::QUEUE_NAME, $this->rsmq->listQueues());

        $this->rsmq->createQueue(self::QUEUE_NAME);
        $this->assertContains(self::QUEUE_NAME, $this->rsmq->listQueues());
    }

    public function testValidateWithInvalidQueueName(): void
    {
        $this->expectExceptionMessage('Invalid queue name');
        $this->invokeMethod($this->rsmq, 'validate', [
            ['queue' => ' foo']
        ]);

    }

    public function testValidateWithInvalidVt(): void
    {
        $this->expectExceptionMessage('Visibility time must be');
        $this->invokeMethod($this->rsmq, 'validate', [
            ['vt' => '-1']
        ]);
    }

    public function testValidateWithInvalidId(): void
    {
        $this->expectExceptionMessage('Invalid message id');
        $this->invokeMethod($this->rsmq, 'validate', [
            ['id' => '123456']
        ]);
    }

    public function testValidateWithInvalidDelay(): void
    {
        $this->expectExceptionMessage('Delay must be');
        $this->invokeMethod($this->rsmq, 'validate', [
            ['delay' => 99999999]
        ]);
    }

    public function testValidateWithInvalidMaxSize(): void
    {
        $this->expectExceptionMessage('Maximum message size must be');
        $this->invokeMethod($this->rsmq, 'validate', [
            ['maxsize' => 512]
        ]);
    }

    public function testSendMessage(): void
    {
        $this->rsmq->createQueue(self::QUEUE_NAME);
        $id = $this->rsmq->sendMessage(self::QUEUE_NAME, 'foobar');
        $this->assertSame(32, strlen($id));
    }

    public function testSendMessageWithBigMessage(): void
    {
        $this->rsmq->createQueue(self::QUEUE_NAME);
        $bigStr = str_repeat(bin2hex(random_bytes(512)), 100);

        $this->expectExceptionMessage('Message too long');
        $this->rsmq->sendMessage(self::QUEUE_NAME, $bigStr);
    }

    public function testDeleteMessage(): void
    {
        $this->rsmq->createQueue(self::QUEUE_NAME);
        $id = $this->rsmq->sendMessage(self::QUEUE_NAME, 'bar');
        $this->assertTrue($this->rsmq->deleteMessage(self::QUEUE_NAME, $id));
    }

    public function testReceiveMessage(): void
    {
        $queue = self::QUEUE_NAME;
        $message = 'Hello World';
        $this->rsmq->createQueue($queue);
        $id = $this->rsmq->sendMessage($queue, $message);
        $received = $this->rsmq->receiveMessage($queue);

        $this->assertSame($message, $received['message']);
        $this->assertSame($id, $received['id']);
    }

    public function testReceiveMessageWhenNoMessageExists(): void
    {
        $queue = self::QUEUE_NAME;
        $this->rsmq->createQueue($queue);
        $received = $this->rsmq->receiveMessage($queue);

        $this->assertEmpty($received);
    }

    public function testChangeMessageVisibility(): void
    {
        $queue = self::QUEUE_NAME;
        $this->rsmq->createQueue($queue);
        $id = $this->rsmq->sendMessage($queue, 'bar');
        $this->assertTrue($this->rsmq->changeMessageVisibility($queue, $id, 60));
        $this->assertEmpty($this->rsmq->receiveMessage($queue));
    }

    public function testGetQueue(): void
    {
        $queueName = self::QUEUE_NAME;
        $vt = 30;
        $delay = 0;
        $maxSize = 65536;
        $this->rsmq->createQueue($queueName, $vt, $delay, $maxSize);
        $queue = $this->invokeMethod($this->rsmq, 'getQueue', [$queueName, true]);

        $this->assertSame($vt, $queue['vt']);
        $this->assertSame($delay, $queue['delay']);
        $this->assertSame($maxSize, $queue['maxsize']);
        $this->assertArrayHasKey('uid', $queue);
        $this->assertSame(32, strlen($queue['uid']));
    }

    public function testGetQueueNotFound(): void
    {
        $this->expectExceptionMessage('Queue not found');
        $this->invokeMethod($this->rsmq, 'getQueue', ['notfound']);
    }

    public function testPopMessage(): void
    {
        $queue = self::QUEUE_NAME;
        $message = 'bar';
        $this->rsmq->createQueue($queue);

        $id = $this->rsmq->sendMessage($queue, $message);
        $received = $this->rsmq->popMessage($queue);

        $this->assertSame($id, $received['id']);
        $this->assertSame($message, $received['message']);
    }

    public function testPopMessageWhenNoMessageExists(): void
    {
        $queue = self::QUEUE_NAME;
        $this->rsmq->createQueue($queue);

        $received = $this->rsmq->popMessage($queue);

        $this->assertEmpty($received);

    }

    public function testSetQueueAttributes(): void
    {
        $queue = self::QUEUE_NAME;
        $vt = 100;
        $delay = 10;
        $maxsize = 2048;
        $this->rsmq->createQueue($queue);
        $attrs = $this->rsmq->setQueueAttributes($queue, $vt, $delay, $maxsize);

        $this->assertSame($vt, $attrs['vt']);
        $this->assertSame($delay, $attrs['delay']);
        $this->assertSame($maxsize, $attrs['maxsize']);
    }

    /**
     * @param object $object
     * @param string $methodName
     * @param array<int, mixed> $parameters
     * @return mixed
     * @throws ReflectionException
     */
    public function invokeMethod(object &$object, string $methodName, array $parameters = array())
    {
        $reflection = new \ReflectionClass(get_class($object));
        $method = $reflection->getMethod($methodName);
        $method->setAccessible(true);

        return $method->invokeArgs($object, $parameters);
    }

    public function tearDown(): void
    {
        try {
            $this->rsmq->deleteQueue(self::QUEUE_NAME);
        } catch (Exception $ignore) {

        }
    }
}