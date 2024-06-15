<?php
declare(strict_types=1);

namespace CT\AmpCluster;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\Cluster\ClusterException;
use Amp\Cluster\ClusterWorkerMessage;
use Amp\CompositeException;
use Amp\DeferredCancellation;
use Amp\Future;
use Amp\Parallel\Context\ContextException;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ContextPanicError;
use Amp\Parallel\Context\DefaultContextFactory;
use Amp\Parallel\Ipc\IpcHub;
use Amp\Parallel\Ipc\LocalIpcHub;
use Amp\Parallel\Worker\TaskFailureThrowable;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;
use Amp\Sync\ChannelException;
use CT\AmpCluster\Exceptions\FatalWorkerException;
use CT\AmpCluster\Exceptions\TerminateWorkerException;
use CT\AmpCluster\PoolState\PoolStateStorage;
use CT\AmpCluster\SocketPipe\SocketListenerProvider;
use CT\AmpCluster\SocketPipe\SocketPipeProvider;
use CT\AmpCluster\Worker\PickupStrategy\PickupLeastJobs;
use CT\AmpCluster\Worker\RestartStrategy\RestartAlways;
use CT\AmpCluster\Worker\ScalingStrategy\ScalingSimple;
use CT\AmpCluster\Worker\WorkerDescriptor;
use CT\AmpCluster\Worker\WorkerStrategies;
use CT\AmpCluster\Worker\WorkerStrategyInterface;
use Psr\Log\LoggerInterface as PsrLogger;
use Revolt\EventLoop;
use function Amp\async;

/**
 * Worker Pool Manager Class.
 *
 * A worker pool allows you to create groups of processes belonging to different types of workers,
 * and then use them to perform tasks.
 *
 * @template-covariant TReceive
 * @template TSend
 */
class WorkerPool                    implements WorkerPoolInterface
{
    protected int $workerStartTimeout = 5;
    private int $lastGroupId        = 0;
    
    /**
     * @var WorkerDescriptor[]
     */
    protected array $workers        = [];
    
    /** @var array<int, Future<void>> */
    protected array $workerFutures  = [];
    
    /** @var Queue<ClusterWorkerMessage<TReceive, TSend>> */
    protected readonly Queue $queue;
    /** @var ConcurrentIterator<ClusterWorkerMessage<TReceive, TSend>> */
    private readonly ConcurrentIterator $iterator;
    private bool $running = false;
    private SocketPipeProvider $provider;
    
    private ?SocketListenerProvider $listenerProvider = null;
    
    private PoolStateStorage $poolState;
    
    /**
     * @var WorkerGroupInterface[]
     */
    private array $groupsScheme             = [];
    
    public function __construct(
        protected readonly IpcHub $hub      = new LocalIpcHub(),
        protected ?ContextFactory $contextFactory = null,
        protected string|array $script      = '',
        protected ?PsrLogger $logger        = null
    ) {
        
        $this->script               = \array_merge(
            [__DIR__ . '/runner.php'],
            \is_array($script) ? \array_values(\array_map(\strval(...), $script)) : [$script],
        );
        
        $this->provider             = new SocketPipeProvider($this->hub);
        $this->contextFactory       ??= new DefaultContextFactory(ipcHub: $this->hub);
        $this->queue                = new Queue();
        $this->iterator             = $this->queue->iterate();
        
        // For Windows, we should use the SocketListenerProvider instead of the SocketPipeProvider
        if(PHP_OS_FAMILY === 'Windows') {
            $this->listenerProvider = new SocketListenerProvider($this);
        }
    }
    
    public function describeGroup(WorkerGroupInterface $group): self
    {
        $group                      = clone $group;
        
        if(class_exists($group->getEntryPointClass()) === false) {
            throw new \Error("The worker class '{$group->getEntryPointClass()}' does not exist");
        }
        
        if($group->getMinWorkers() < 0) {
            throw new \Error('The minimum number of workers must be greater than zero');
        }
        
        if($group->getMaxWorkers() < $group->getMinWorkers()) {
            throw new \Error('The maximum number of workers must be greater than or equal to the minimum number of workers');
        }
        
        if($group->getMaxWorkers() === 0) {
            $group->defineMaxWorkers($group->getMinWorkers());
        }
        
        if($group->getMaxWorkers() === 0) {
            throw new \Error('The maximum number of workers must be greater than zero');
        }
        
        $groupId                    = ++$this->lastGroupId;
        
        if($group->getGroupName() === '') {
            // If group name undefined, use the worker class name without a namespace
            $groupName              = \strrchr($group->getEntryPointClass(), '\\');
            
            if($groupName === false) {
                $groupName          = 'Group'.$groupId;
            } else {
                $groupName          = \ucfirst(\substr($groupName, 1));
            }
            
            $group->defineGroupName($groupName);
        }
        
        $this->groupsScheme[$groupId] = $group->defineWorkerGroupId($groupId);
        
        return $this;
    }
    
    public function getGroupsScheme(): array
    {
        return $this->groupsScheme;
    }
    
    /**
     * @throws \Exception
     */
    public function validateGroupsScheme(): void
    {
        if(empty($this->groupsScheme)) {
            throw new \Exception('The worker groups scheme is empty');
        }
        
        $lastGroupId                = 0;
        
        foreach ($this->groupsScheme as $group) {
            
            if(class_exists($group->getEntryPointClass()) === false) {
                throw new \Exception("The worker class '{$group->getEntryPointClass()}' does not exist");
            }
            
            if($group->getWorkerGroupId() <= $lastGroupId) {
                throw new \Exception('The group ID must be greater than the previous group id');
            }
            
            $lastGroupId            = $group->getWorkerGroupId();
            
            if($group->getMinWorkers() < 0) {
                throw new \Exception('The minimum number of workers must be greater than zero or equal to zero');
            }
            
            if($group->getMaxWorkers() < $group->getMinWorkers()) {
                throw new \Exception('The maximum number of workers must be greater than or equal to the minimum number of workers');
            }
            
            if($group->getMaxWorkers() === 0) {
                throw new \Exception('The maximum number of workers must be greater than zero');
            }
            
            foreach ($group->getJobGroups() as $jobGroupId) {
                if(\array_key_exists($jobGroupId, $this->groupsScheme)) {
                    throw new \Exception("The job group id '{$jobGroupId}' is not found in the worker groups scheme");
                }
                
                if($jobGroupId === $group->getWorkerGroupId()) {
                    throw new \Exception("The job group id '{$jobGroupId}' must be different from the worker group id");
                }
            }
            
        }
    }
    
    /**
     * @throws \Throwable
     */
    public function run(): void
    {
        if ($this->running || $this->queue->isComplete()) {
            throw new \Exception('The cluster watcher is already running or has already run');
        }
        
        $this->validateGroupsScheme();
        $this->applyGroupScheme();
        
        if (count($this->workers) <= 0) {
            throw new \Exception('The number of workers must be greater than zero');
        }
        
        $this->running              = true;

        try {
            
            $this->poolState        = new PoolStateStorage(count($this->groupsScheme));
            $this->poolState->setGroups($this->groupsScheme);
            
            foreach ($this->workers as $worker) {
                if($worker->shouldBeStarted) {
                    $this->startWorker($worker);
                }
            }
        } catch (\Throwable $exception) {
            $this->stop();
            throw $exception;
        }
    }
    
    public function getMessageIterator(): iterable
    {
        return $this->iterator;
    }
    
    public function mainLoop(): void
    {
        foreach ($this->getMessageIterator() as $message) {
            continue;
        }
    }
    
    protected function applyGroupScheme(): void
    {
        foreach ($this->groupsScheme as $group) {
            $this->fillWorkersGroup($group);
        }
    }
    
    private function startWorker(WorkerDescriptor $workerDescriptor): void
    {
        $context                    = $this->contextFactory->start($this->script);
        $key                        = $this->hub->generateKey();
        
        $context->send([
            'id'                    => $workerDescriptor->id,
            'uri'                   => $this->hub->getUri(),
            'key'                   => $key,
            'group'                 => $workerDescriptor->group,
            'groupsScheme'          => $this->groupsScheme,
        ]);
        
        try {
            $socketTransport        = $this->provider->createSocketTransport($key);
        } catch (\Throwable $exception) {
            if (!$context->isClosed()) {
                $context->close();
            }
            
            throw new \Exception("Starting the worker '{$workerDescriptor->id}' failed. Socket provider start failed", previous: $exception);
        }
        
        $deferredCancellation       = new DeferredCancellation();
        
        $worker                     = new WorkerProcessContext(
            $workerDescriptor->id,
            $context,
            $socketTransport ?? $this->listenerProvider,
            $this->queue,
            $deferredCancellation
        );
        
        if($this->logger !== null) {
            $worker->setLogger($this->logger);
        }
        
        $workerDescriptor->setWorker($worker);
        
        $worker->info(\sprintf('Started %s worker #%d', $workerDescriptor->group->getWorkerType()->value, $workerDescriptor->id));
        
        // Server stopped while worker was starting, so immediately throw everything away.
        if (false === $this->running) {
            $worker->shutdown();
            return;
        }
        
        $workerDescriptor->setFuture(async(function () use (
            $worker,
            $context,
            $socketTransport,
            $deferredCancellation,
            $workerDescriptor
        ): void {
            async($this->provider->provideFor(...), $socketTransport, $deferredCancellation->getCancellation())->ignore();
            
            $id                         = $workerDescriptor->id;
            $allowRestart               = $workerDescriptor->shouldBeRestarted;
            
            try {
                try {
                    $worker->runWorkerLoop();
                    
                    $worker->info("Worker {$id} terminated cleanly" . ($this->running ? ", restarting..." : ""));
                } catch (CancelledException) {
                    $worker->info("Worker {$id} forcefully terminated as part of watcher shutdown");
                } catch (ChannelException $exception) {
                    $worker->error(
                        "Worker {$id} died unexpectedly: {$exception->getMessage()}" .
                        ($this->running ? ", restarting..." : "")
                    );
                    
                    $remoteException = $exception->getPrevious();
                    
                    if (($remoteException instanceof TaskFailureThrowable
                         || $remoteException
                            instanceof
                            ContextPanicError)
                        && $remoteException->getOriginalClassName() === FatalWorkerException::class) {
                        
                        // The Worker died due to a fatal error, so we should stop the server.
                        $this->logger?->error('Server shutdown due to fatal worker error');
                        throw $remoteException;
                    }
                } catch (TerminateWorkerException) {
                    $worker->info("Worker {$id} terminated cleanly without restart");
                } catch (\Throwable $exception) {
                    $worker->error(
                        "Worker {$id} failed: " . (string) $exception,
                        ['exception' => $exception],
                    );
                    throw $exception;
                } finally {
                    $deferredCancellation->cancel();
                    $workerDescriptor->reset();
                    $context->close();
                }
                
                if ($this->running) {
                    $this->startWorker($workerDescriptor);
                }
            } catch (\Throwable $exception) {
                $this->stop();
                throw $exception;
            }
        })->ignore());
    }
    
    protected function fillWorkersGroup(WorkerGroup $group): void
    {
        if($group->getWorkerGroupId() === 0) {
            throw new \Error('The group id must be greater than zero');
        }
        
        if($group->getMinWorkers() <= 0) {
            throw new \Error('The minimum number of workers must be greater than zero');
        }

        if($group->getMaxWorkers() < $group->getMinWorkers()) {
            throw new \Error('The maximum number of workers must be greater than or equal to the minimum number of workers');
        }
        
        $baseWorkerId               = $this->getLastWorkerId() + 1;
        
        // All workers in the group will have the same strategies
        $this->defaultWorkerStrategies($group);
        $this->initWorkerStrategies($group);
        
        foreach (range($baseWorkerId, $baseWorkerId + $group->getMinWorkers() - 1) as $id) {
            $this->addWorker(new WorkerDescriptor(
                $id, $group, $id <= ($baseWorkerId + $group->getMinWorkers() - 1
            )));
        }
    }
    
    protected function getLastWorkerId(): int
    {
        $maxId                      = 0;
        
        foreach ($this->workers as $worker) {
            if($worker->id > $maxId) {
                $maxId              = $worker->id;
            }
        }
        
        return $maxId;
    }
    
    protected function addWorker(WorkerDescriptor $worker): self
    {
        $this->workers[]            = $worker;
        return $this;
    }
    
    public function getWorkers(): array
    {
        return $this->workers;
    }
    
    /**
     * Stops all cluster workers. Workers are killed if the cancellation token is cancelled.
     *
     * @param Cancellation|null $cancellation Token to request cancellation of waiting for shutdown.
     * When cancelled, the workers are forcefully killed. If null, the workers are killed immediately.
     */
    public function stop(?Cancellation $cancellation = null): void
    {
        if ($this->queue->isComplete() || false === $this->running) {
            return;
        }
        
        $this->running              = false;
        $this->listenerProvider?->close();
        
        $futures                    = [];
        
        foreach ($this->workers as $workerDescriptor) {
            $futures[]              = async(static function () use ($workerDescriptor, $cancellation): void {
                $future             = $workerDescriptor->getFuture();
                
                try {
                    $workerDescriptor->getWorker()?->shutdown($cancellation);
                } catch (ContextException) {
                    // Ignore if the worker has already died unexpectedly.
                }
                
                // We need to await this future here, otherwise we may not log things properly if the
                // event-loop exits immediately after.
                $future?->await();
            });
        }
        
        [$exceptions]               = Future\awaitAll($futures);
        
        try {
            if (!$exceptions) {
                $this->queue->complete();
                return;
            }
            
            if (\count($exceptions) === 1) {
                $exception          = \array_shift($exceptions);
                $this->queue->error(new ClusterException(
                    "Stopping the cluster failed: " . $exception->getMessage(),
                    previous: $exception,
                ));
                
                return;
            }
            
            $exception              = new CompositeException($exceptions);
            $message                = \implode('; ', \array_map(static fn (\Throwable $e) => $e->getMessage(), $exceptions));
            
            $this->queue->error(new ClusterException("Stopping the cluster failed: " . $message, previous: $exception));
        } finally {
            $this->workers          = [];
        }
    }
    
    public function __destruct()
    {
        EventLoop::queue($this->stop(...));
    }
    
    protected function defaultWorkerStrategies(WorkerGroup $group): void
    {
        if($group->getPickupStrategy() === null) {
            $group->definePickupStrategy(new PickupLeastJobs);
        }
        
        if($group->getScalingStrategy() === null) {
            $group->defineScalingStrategy(new ScalingSimple);
        }
        
        if($group->getRestartStrategy() === null) {
            $group->defineRestartStrategy(new RestartAlways);
        }
    }
    
    protected function initWorkerStrategies(WorkerGroup $group): void
    {
        $strategy                   = $group->getPickupStrategy();
        
        if($strategy instanceof WorkerStrategyInterface) {
            $strategy->setWorkerPool($this)->setWorkerGroup($group);
        }
        
        $strategy                   = $group->getScalingStrategy();
        
        if($strategy instanceof WorkerStrategyInterface) {
            $strategy->setWorkerPool($this)->setWorkerGroup($group);
        }
        
        $strategy                   = $group->getRestartStrategy();
        
        if($strategy instanceof WorkerStrategyInterface) {
            $strategy->setWorkerPool($this)->setWorkerGroup($group);
        }
    }
}