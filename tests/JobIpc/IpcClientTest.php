<?php
declare(strict_types=1);

namespace CT\AmpServer\JobIpc;

use Amp\DeferredCancellation;
use Amp\TimeoutCancellation;
use CT\AmpServer\PoolState\PoolStateStorage;
use CT\AmpServer\WorkerState\WorkerStateStorage;
use PHPUnit\Framework\TestCase;
use Revolt\EventLoop;

class IpcClientTest                 extends TestCase
{
    private IpcClient $ipcClient;
    private IpcServer $ipcServer;
    private PoolStateStorage $poolState;
    private WorkerStateStorage $workerState;
    private DeferredCancellation $jobsLoopCancellation;
    private JobSerializerI       $jobSerializer;
    private mixed                $jobHandler = null;

    protected function setUp(): void
    {
        $this->poolState            = new PoolStateStorage(1);
        $this->poolState->setWorkerGroupInfo(1, 1, 1);
        
        $this->workerState          = new WorkerStateStorage(1, 1, true);
        $this->workerState->workerReady();
        
        $this->jobSerializer        = new JobSerializer;
        $this->ipcServer            = new IpcServer(1);
        $this->ipcClient            = new IpcClient(2);
        
        $this->jobsLoopCancellation = new DeferredCancellation();
        
        EventLoop::queue($this->ipcServer->receiveLoop(...), $this->jobsLoopCancellation->getCancellation());
        EventLoop::queue($this->jobsLoop(...));
        
        EventLoop::queue($this->ipcClient->mainLoop(...));
    }

    protected function tearDown(): void
    {
        $this->jobsLoopCancellation->cancel();
        $this->ipcClient->close();
        $this->jobHandler = null;
        $this->poolState->close();
    }

    public function testDefault(): void
    {
        $receivedData               = null;

        $this->jobHandler = function (JobRequest $request) use (&$receivedData) {
            $receivedData           = $request->data;
            return 'OK: ' . $request->data;
        };

        $future                     = $this->ipcClient->sendJobImmediately('Test', 1, true);

        $future->await(new TimeoutCancellation(5));

        $this->assertEquals('Test', $receivedData);
    }
    
    private function jobsLoop(): void
    {
        $iterator                   = $this->ipcServer->getJobQueue()->iterate();
        $abortCancellation          = $this->jobsLoopCancellation->getCancellation();
        
        while ($iterator->continue($abortCancellation)) {
            [$channel, $data]       = $iterator->getValue();
            
            if(is_callable($this->jobHandler)) {
                
                $request            = $this->jobSerializer->parseRequest($data);
                $response           = call_user_func($this->jobHandler, $request);
                
                if($request->jobId !== 0) {
                    $channel->send($this->jobSerializer->createResponse($request->jobId, $request->fromWorkerId, $request->workerGroupId, $response ?? ''));
                }
            }
        }
    }
}