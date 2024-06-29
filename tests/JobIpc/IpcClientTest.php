<?php
declare(strict_types=1);

namespace CT\AmpPool\JobIpc;

use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use CT\AmpPool\PoolState\PoolStateStorage;
use CT\AmpPool\Worker\WorkerState\WorkersInfo;
use CT\AmpPool\Worker\WorkerState\WorkerStateStorage;
use CT\AmpPool\WorkerGroup;
use CT\AmpPool\WorkerTypeEnum;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

class IpcClientTest extends TestCase
{
    private IpcClient $ipcClient;
    private IpcServer $ipcServer;
    private PoolStateStorage $poolState;
    private WorkerStateStorage $workerState;
    private DeferredCancellation   $jobsLoopCancellation;
    private JobSerializerInterface $jobSerializer;
    private mixed                  $jobHandler = null;

    protected function setUp(): void
    {
        $workerId                   = 1;
        $groupId                    = 1;

        $this->poolState            = new PoolStateStorage($groupId);
        $this->poolState->setWorkerGroupState($groupId, $workerId, $workerId);

        $this->workerState          = new WorkerStateStorage($workerId, $groupId, true);
        $this->workerState->workerReady();

        $this->jobSerializer        = new JobSerializer;

        $this->jobsLoopCancellation = new DeferredCancellation;

        $workerGroup                = new WorkerGroup(
            '',
            WorkerTypeEnum::REACTOR,
            pickupStrategy: new PickupStrategyDummy($workerId)
        );

        $workersInfo                = new WorkersInfo;

        $this->ipcServer            = new IpcServer($workerId);

        $this->ipcClient            = new IpcClient(
            $workerId,
            $workerGroup,
            [$workerGroup],
            $workersInfo,
            $this->poolState,
            $this->jobSerializer,
            $this->jobsLoopCancellation->getCancellation()
        );

        EventLoop::queue($this->ipcServer->receiveLoop(...), $this->jobsLoopCancellation->getCancellation());
        EventLoop::queue($this->jobsLoop(...));

        EventLoop::queue($this->ipcClient->mainLoop(...));
    }

    protected function tearDown(): void
    {
        $this->jobsLoopCancellation->cancel();
        $this->ipcClient->close();
        $this->ipcServer->close();
        $this->poolState->close();
        $this->jobHandler           = null;
    }

    #[RunInSeparateProcess]
    public function testDefault(): void
    {
        $receivedData               = null;

        $this->jobHandler           = function (JobRequest $request) use (&$receivedData) {
            $receivedData           = $request->getData();
            return 'OK: ' . $request->getData();
        };

        $future                     = $this->ipcClient->sendJobImmediately('Test', allowedGroups: [1], awaitResult: true);

        $future->await(new TimeoutCancellation(5));

        $this->assertEquals('Test', $receivedData);
    }

    private function jobsLoop(): void
    {
        $iterator                   = $this->ipcServer->getJobQueue()->iterate();
        $abortCancellation          = $this->jobsLoopCancellation->getCancellation();

        try {
            while ($iterator->continue($abortCancellation)) {
                [$channel, $request]= $iterator->getValue();

                if($request === null) {
                    break;
                }

                if(false === $request instanceof JobRequestInterface) {
                    throw new \RuntimeException('Invalid request');
                }

                if(\is_callable($this->jobHandler)) {
                    $response       = \call_user_func($this->jobHandler, $request);

                    if($request->getJobId() !== 0) {
                        $channel->send(
                            $this->jobSerializer->createResponse(
                                $request->getJobId(),
                                $request->getFromWorkerId(),
                                $request->getWorkerGroupId(),
                                $response ?? ''
                            )
                        );
                    }
                }
            }
        } catch (CancelledException) {
            // Ignore
        }
    }
}
