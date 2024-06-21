<?php
declare(strict_types=1);

namespace CT\AmpPool\JobIpc;

use Amp\ByteStream\StreamChannel;
use Amp\ByteStream\StreamException;
use Amp\Cancellation;
use Amp\DeferredFuture;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Future;
use Amp\Serialization\PassthroughSerializer;
use Amp\TimeoutCancellation;
use Amp\TimeoutException;
use CT\AmpPool\Exceptions\NoWorkersAvailable;
use CT\AmpPool\Exceptions\SendJobException;
use CT\AmpPool\PoolState\PoolStateStorage;
use CT\AmpPool\Worker\WorkerState\WorkersInfoInterface;
use CT\AmpPool\WorkerGroupInterface;
use Revolt\EventLoop;
use function Amp\delay;
use function Amp\Socket\socketConnector;

/**
 * The class is responsible for sending JOBs to other workers.
 */
final class IpcClient                   implements IpcClientInterface
{
    use ForbidCloning;
    use ForbidSerialization;
    
    /**
     * @var StreamChannel[]
     */
    private array                       $workerChannels = [];
    private JobSerializerInterface|null $jobTransport   = null;
    /**
     * List of futures that are waiting for the result of the job with SocketId, and time when the job was sent
     * @var array [Future, int, int]
     */
    private array $resultsFutures   = [];
    private int $maxTryCount        = 3;
    private int $futureTimeout      = 60 * 10;
    private string $futureTimeoutCallbackId;
    
    /**
     * IpcClient constructor.
     *
     * @param JobSerializerInterface|null $jobSerializer Job serializer
     * @param Cancellation|null           $cancellation  Cancellation
     * @param int                         $retryInterval Retry interval for sending a job
     */
    public function __construct(
        private readonly int $workerId,
        private readonly WorkerGroupInterface $workerGroup,
        private readonly array $groupsScheme,
        private readonly WorkersInfoInterface $workersInfo,
        private readonly PoolStateStorage $poolState,
        JobSerializerInterface                $jobSerializer = null,
        private readonly Cancellation|null    $cancellation = null,
        private readonly int                  $retryInterval = 1,
        private readonly int                  $scalingTimeout = 2
    )
    {
        if($this->workerGroup->getPickupStrategy() === null) {
            throw new \InvalidArgumentException('WorkerGroup must have a PickupStrategy');
        }
        
        $this->jobTransport         = $jobSerializer ?? new JobSerializer();
    }
    
    public function mainLoop(): void
    {
        $this->futureTimeoutCallbackId = EventLoop::repeat($this->futureTimeout / 2, $this->updateFuturesByTimeout(...));
    }
    
    /**
     * @inheritDoc
     */
    public function sendJob(string $data, array $allowedGroups = [], array $allowedWorkers = [], bool $awaitResult = false, int $priority = 0): Future|null
    {
        $deferred                   = null;
        
        if($awaitResult) {
            $deferred               = new DeferredFuture();
        }
        
        EventLoop::queue($this->sendJobImmediately(...), $data, $allowedGroups, $allowedWorkers, $deferred, $priority);
        
        return $deferred?->getFuture();
    }
    
    /**
     * Try to send a job to the worker immediately in the current fiber.
     *
     * @param string              $data
     * @param array               $allowedGroups
     * @param array               $allowedWorkers
     * @param bool|DeferredFuture $awaitResult
     * @param int                 $priority
     *
     * @return Future|null
     * @throws \Throwable
     */
    public function sendJobImmediately(string              $data,
                                       array               $allowedGroups = [],
                                       array               $allowedWorkers = [],
                                       bool|DeferredFuture $awaitResult = false,
                                       int                 $priority = 0
    ): Future|null
    {
        $tryCount                   = 0;
        $ignoreWorkers              = [];
        
        if($awaitResult instanceof DeferredFuture) {
            $deferred               = $awaitResult;
        } else {
            $deferred               = $awaitResult ? new DeferredFuture() : null;
        }
        
        if($allowedGroups === []) {
            $allowedGroups          = $this->workerGroup->getJobGroups();
        }
        
        // Add self-worker to ignore-list
        if(false === in_array($this->workerId, $ignoreWorkers, true)) {
            $ignoreWorkers[]        = $this->workerId;
        }
        
        while($tryCount < $this->maxTryCount) {
            
            $isScalingPossible      = false;
            $foundedWorkerId        = $this->pickupWorker($allowedGroups, $allowedWorkers, $ignoreWorkers, $tryCount);
            
            try {
                
                if($foundedWorkerId === null) {
                    $isScalingPossible  = $this->requestScaling($allowedGroups);
                    throw new NoWorkersAvailable($allowedGroups);
                }
                
                $socketId           = $this->tryToSendJob($foundedWorkerId, $data, $priority, $deferred);
                
                if($deferred !== null) {
                    $this->resultsFutures[spl_object_id($deferred)] = [$deferred, $socketId, time()];
                    return $deferred->getFuture();
                } else {
                    return null;
                }
                
            } catch (NoWorkersAvailable $exception) {
                
                if($isScalingPossible && $this->scalingTimeout > 0) {
                    $tryCount++;
                    // suspend the current task for a while
                    delay($this->scalingTimeout, true, $this->cancellation);
                } else if($this->retryInterval > 0) {
                    $tryCount++;
                    // suspend the current task for a while
                    delay((float)$this->retryInterval, true, $this->cancellation);
                } else {
                    $deferred?->complete($exception);
                    throw $exception;
                }
                
            } catch (StreamException) {
                $tryCount++;
                $ignoreWorkers[]    = $foundedWorkerId;
            }
        }
        
        if($deferred !== null) {
            $deferred->complete(new SendJobException($allowedGroups, $this->maxTryCount));
            return $deferred->getFuture();
        }
        
        throw new SendJobException($allowedGroups, $this->maxTryCount);
    }
    
    private function tryToSendJob(
        $foundedWorkerId,
        string $data,
        int $priority               = 0,
        DeferredFuture $deferred    = null
    ): int
    {
        $channel                    = $this->getWorkerChannel($foundedWorkerId);
        $jobId                      = $deferred !== null ? spl_object_id($deferred) : 0;
        
        try {
            $channel->send(
                $this->jobTransport->createRequest($jobId, $this->workerId, $this->workerGroup->getWorkerGroupId(), $data, $priority)
            );
        } catch (\Throwable $exception) {
            $deferred->complete($exception);
            throw $exception;
        }
        
        return spl_object_id($channel);
    }
    
    public function __destruct()
    {
        $this->close();
    }
    
    public function close(): void
    {
        EventLoop::cancel($this->futureTimeoutCallbackId);
        
        $channels                   = $this->workerChannels;
        $this->workerChannels       = [];
        
        foreach($channels as $channel) {
            try {
                // Close connection gracefully
                $channel->send(IpcServer::CLOSE_HAND_SHAKE);
                $channel->close();
            } catch (\Throwable) {
            }
        }
    }
    
    private function pickupWorker(array $allowedGroups = [], array $allowedWorkers = [], array $ignoreWorkers = [], int $priority = 0, int $tryCount = 0): int|null
    {
        if($allowedGroups === []) {
            $allowedGroups          = $this->workerGroup->getJobGroups();
        }

        // Add self-worker to ignore a list
        if(false === in_array($this->workerId, $ignoreWorkers, true)) {
            $ignoreWorkers[]        = $this->workerId;
        }

        return $this->workerGroup->getPickupStrategy()?->pickupWorker($allowedGroups, $allowedWorkers, $ignoreWorkers, $priority, $tryCount);
    }
    
    private function requestScaling(array $allowedGroups): bool
    {
        $workerId                   = $this->workerId;
        $groupsScheme               = $this->groupsScheme;
        $isPossible                 = false;
        
        foreach ($allowedGroups as $groupId) {
            
            if(array_key_exists($groupId, $groupsScheme) === false) {
                continue;
            }
            
            if($groupsScheme[$groupId]->getScalingStrategy()?->requestScaling($workerId) === true) {
                $isPossible         = true;
            }
        }
        
        return $isPossible;
    }
    
    private function getWorkerChannel(int $workerId): StreamChannel
    {
        if(array_key_exists($workerId, $this->workerChannels)) {
            return $this->workerChannels[$workerId];
        }
        
        $this->workerChannels[$workerId] = $this->createWorkerChannel($workerId);
        
        EventLoop::queue($this->readLoop(...), $workerId);
        
        return $this->workerChannels[$workerId];
    }
    
    private function createWorkerChannel(int $workerId): StreamChannel
    {
        $connector                  = socketConnector();
        
        $client                     = $connector->connect(
            IpcServer::getSocketAddress($workerId), cancellation: new TimeoutCancellation(5)
        );
        
        $client->write(IpcServer::HAND_SHAKE);
        
        return new StreamChannel($client, $client, new PassthroughSerializer);
    }
    
    private function readLoop(int $workerId): void
    {
        $channel                    = $this->workerChannels[$workerId] ?? null;
        
        if($channel === null) {
            return;
        }
        
        try {
            while (($data = $channel->receive($this->cancellation)) !== null) {
                
                $response           = $this->jobTransport->parseResponse($data);
                
                if(array_key_exists($response->jobId, $this->resultsFutures)) {
                    [$deferred, ] = $this->resultsFutures[$response->jobId];
                    unset($this->resultsFutures[$response->jobId]);
                    $deferred->complete($response->data);
                }
            }
        } catch (\Throwable $exception) {
            
            unset($this->workerChannels[$workerId]);
            
            try {
                $channel->send(IpcServer::CLOSE_HAND_SHAKE);
                $channel->close();
            } catch (\Throwable) {
            }
        }
    }
    
    private function updateFuturesByTimeout(): void
    {
        $currentTime                = time();
        
        foreach($this->resultsFutures as $id => [$deferred, $socketId, $time]) {
            if($currentTime - $time > $this->futureTimeout) {
                unset($this->resultsFutures[$id]);
                $deferred->error(new TimeoutException('Future timeout: ' . $this->futureTimeout));
            }
        }
    }
}