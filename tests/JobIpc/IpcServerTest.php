<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\JobIpc;

use Amp\ByteStream\StreamChannel;
use Amp\DeferredCancellation;
use Amp\Serialization\PassthroughSerializer;
use Amp\Socket\Socket;
use Amp\TimeoutCancellation;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;
use function Amp\Socket\socketConnector;

class IpcServerTest extends TestCase
{
    private IpcServer $ipcServer;
    private DeferredCancellation $jobsLoopCancellation;
    private mixed $jobHandler       = null;

    protected function setUp(): void
    {
        $this->ipcServer            = new IpcServer(1);
        $this->jobsLoopCancellation = new DeferredCancellation();
        EventLoop::queue($this->ipcServer->receiveLoop(...), $this->jobsLoopCancellation->getCancellation());
        EventLoop::queue($this->jobsLoop(...));
    }

    protected function tearDown(): void
    {
        $this->jobsLoopCancellation->cancel();
        $this->ipcServer->close();
        $this->jobHandler           = null;
    }

    #[RunInSeparateProcess]
    public function testDefault(): void
    {
        $receivedData               = null;

        $this->jobHandler           = function (StreamChannel $channel, JobRequest $jobRequest) use (&$receivedData) {
            $receivedData           = $jobRequest->getData();
            $channel->send('OK: ' . $receivedData);
        };

        $client                     = $this->getSocketForClient();
        $channel                    = new StreamChannel($client, $client, new PassthroughSerializer());

        $jobSerializer              = new JobSerializer();

        $channel->send($jobSerializer->createRequest(1, 1, 1, 'Test'));

        $response                   = $channel->receive(new TimeoutCancellation(5));

        $this->assertEquals('Test', $receivedData);
        $this->assertEquals('OK: Test', $response);
    }

    private function jobsLoop(): void
    {
        $iterator                   = $this->ipcServer->getJobQueue()->iterate();
        $abortCancellation          = $this->jobsLoopCancellation->getCancellation();

        while ($iterator->continue($abortCancellation)) {
            [$channel, $data]       = $iterator->getValue();

            if (\is_callable($this->jobHandler)) {
                \call_user_func($this->jobHandler, $channel, $data);
            }
        }
    }

    private function getSocketForClient(int $workerId = 1): Socket
    {
        $connector                  = socketConnector();

        $client                     = $connector->connect(
            IpcServer::getSocketAddress($workerId),
            cancellation: new TimeoutCancellation(2)
        );

        $client->write(IpcServer::HAND_SHAKE);

        return $client;
    }
}
