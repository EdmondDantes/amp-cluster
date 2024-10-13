<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Worker;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;
use Amp\Sync\Channel;
use Amp\TimeoutCancellation;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\Exceptions\RemoteException;
use IfCastle\AmpPool\Internal\Messages\MessageIpcShutdown;
use IfCastle\AmpPool\Internal\Messages\MessagePingPong;
use IfCastle\AmpPool\Internal\Messages\WorkerShouldBeShutdown;
use IfCastle\AmpPool\Internal\Messages\WorkerSoftShutdown;
use IfCastle\AmpPool\Internal\Messages\WorkerStarted;
use IfCastle\AmpPool\Strategies\WorkerStrategyInterface;
use IfCastle\AmpPool\Worker\Internal\PeriodicTask;
use IfCastle\AmpPool\Worker\Internal\WorkerLogHandler;
use IfCastle\AmpPool\WorkerEventEmitter;
use IfCastle\AmpPool\WorkerEventEmitterInterface;
use IfCastle\AmpPool\WorkerGroup;
use IfCastle\AmpPool\WorkersStorage\WorkersStorageInterface;
use IfCastle\AmpPool\WorkersStorage\WorkerStateInterface;
use IfCastle\AmpPool\WorkerTypeEnum;
use Psr\Log\LoggerInterface;
use Revolt\EventLoop;

/**
 * Abstraction of Worker Representation within the worker process.
 * This class should not be used within the process that creates workers!
 *
 * @template-covariant TReceive
 * @template TSend
 * @implements Channel<TReceive, TSend>
 */
class Worker implements WorkerInterface
{
    protected readonly DeferredCancellation $mainCancellation;

    /**
     * A Future is resolved when the Worker needs to be stopped.
     * This Future never throws exceptions,
     * but its result can be an exception thrown when the Worker had to terminate.
     */
    private readonly DeferredFuture $workerFuture;

    /** @var Queue<TReceive> */
    protected readonly Queue $queue;

    /** @var ConcurrentIterator<TReceive> */
    protected readonly ConcurrentIterator $iterator;
    
    protected readonly array $context;

    private LoggerInterface $logger;
    private WorkersStorageInterface $workersStorage;
    private WorkerStateInterface $workerState;
    private WorkerEventEmitterInterface $eventEmitter;

    private bool $isStopped         = false;

    private array $periodicTasks    = [];

    /**
     * Was received a MessageIpcShutdown message.
     */
    private bool $ipcChannelShutdown = false;

    private mixed $softShutdownHandler = null;

    public function __construct(
        private readonly int     $id,
        private readonly Channel $ipcChannel,
        private readonly WorkerGroup $group,
        /**
         * @var array<int, WorkerGroup>
         */
        private readonly array $groupsScheme,
        string $workersStorageClass,
        ?LoggerInterface        $logger = null,
        array $context          = []
    ) {
        $this->queue                = new Queue();
        $this->iterator             = $this->queue->iterate();
        $this->mainCancellation     = new DeferredCancellation;
        $this->workerFuture         = new DeferredFuture;

        $this->eventEmitter         = new WorkerEventEmitter;
        $this->context              = $context;

        if(\class_exists($workersStorageClass) === false) {
            throw new \RuntimeException('Invalid storage class provided. Expected ' . WorkersStorageInterface::class . ' implementation');
        }

        $this->workersStorage       = \forward_static_call([$workersStorageClass, 'instanciate'], $this->getTotalWorkersCount(), $this->id);

        if($logger !== null) {
            $this->logger           = $logger;
        } else {
            $this->logger           = new \Monolog\Logger('worker-'.$id);
            $this->logger->pushHandler(new WorkerLogHandler($this->ipcChannel));
        }
    }

    private function getTotalWorkersCount(): int
    {
        $totalWorkersCount          = 0;

        foreach ($this->groupsScheme as $group) {
            $totalWorkersCount      += $group->getMaxWorkers();
        }

        return $totalWorkersCount;
    }

    public function getWorkersStorage(): WorkersStorageInterface
    {
        return $this->workersStorage;
    }

    public function initWorker(): void
    {
        $this->workerState         = $this->workersStorage->getWorkerState($this->id);
        $this->initWorkerStrategies();
        WorkerGroup::startStrategies($this->groupsScheme);
    }

    public function sendMessageToWatcher(mixed $message): void
    {
        $this->ipcChannel->send($message);
    }

    public function getWatcherChannel(): Channel
    {
        return $this->ipcChannel;
    }

    public function getWorkerState(): WorkerStateInterface
    {
        return $this->workerState;
    }

    /**
     * @return array<int, WorkerGroup>
     */
    public function getGroupsScheme(): array
    {
        return $this->groupsScheme;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    public function getWorkerId(): int
    {
        return $this->id;
    }

    public function getWorkerGroup(): WorkerGroup
    {
        return $this->group;
    }

    public function getWorkerGroupId(): int
    {
        return $this->group->getWorkerGroupId();
    }

    public function getWorkerType(): WorkerTypeEnum
    {
        return $this->group->getWorkerType();
    }
    
    #[\Override]
    public function getWorkerContext(): array
    {
        return $this->context;
    }
    
    public function getWorkerEventEmitter(): WorkerEventEmitterInterface
    {
        return $this->eventEmitter;
    }

    public function getAbortCancellation(): Cancellation
    {
        return $this->mainCancellation->getCancellation();
    }

    public function mainLoop(): void
    {
        $abortCancellation          = $this->mainCancellation->getCancellation();

        try {

            // Update Worker State
            $this->workerState
                ->initDefaults()
                ->setPid(\getmypid())
                ->markAsReady()
                ->setGroupId($this->group->getWorkerGroupId())
                ->update();

            // Confirm that the worker has started
            $this->ipcChannel->send(new WorkerStarted($this->id));

            while ($message = $this->ipcChannel->receive($abortCancellation)) {

                if($message instanceof MessagePingPong) {
                    $this->ipcChannel->send(new MessagePingPong);
                    continue;
                }

                if($message instanceof MessageIpcShutdown) {
                    $this->ipcChannelShutdown = true;
                }

                if($message instanceof MessageIpcShutdown || $message instanceof WorkerShouldBeShutdown) {
                    break;
                }

                if($message instanceof WorkerSoftShutdown) {

                    // handle the soft shutdown handler if defined
                    if($this->softShutdownHandler !== null) {
                        \call_user_func($this->softShutdownHandler);
                    } else {
                        // else terminate the worker
                        break;
                    }
                }

                $this->eventEmitter->emitWorkerEvent($message, $this->id);
            }
        } catch (\Throwable $exception) {

            // Extract the original exception
            if($exception instanceof CancelledException && $exception->getPrevious() !== null) {
                $exception          = $exception->getPrevious();
            }

            try {
                $this->workerState->increaseAndUpdateShutdownErrors();
            } catch (\Throwable $stateException) {
                $this->logger->error('Failed to update worker state: '.$stateException->getMessage());
            }

            if(false === $exception instanceof CancelledException) {
                $this->sendException($exception);
            }

            // IPC Channel manually closed
            if(false === $this->workerFuture->isComplete()) {
                //
                // We always use the complete() method instead of error()
                // because this Future should not throw any exceptions!
                //
                $this->workerFuture->complete($exception);
            }
        } finally {
            $this->stop();
        }
    }

    public function awaitShutdown(): void
    {
        $this->workerState->markUsShutdown();

        if($this->ipcChannel->isClosed()) {
            return;
        }

        // Send a message to the watcher to confirm that the worker has been shutdown
        $this->ipcChannel->send(null);

        if($this->ipcChannelShutdown) {
            return;
        }

        try {
            $this->ipcChannel->receive(new TimeoutCancellation(2));
        } catch (\Throwable) {
            // Ignore
        }
    }

    public function awaitTermination(?Cancellation $cancellation = null): void
    {
        try {
            $this->workerFuture->getFuture()->await($cancellation);
        } catch (CancelledException) {
            // Ignore
        }
    }

    public function initiateTermination(?\Throwable $throwable = null): void
    {
        if(false === $this->mainCancellation->isCancelled()) {
            $this->mainCancellation->cancel($throwable);
        }
    }

    public function stop(?\Throwable $throwable = null): void
    {
        if($this->isStopped) {
            return;
        }

        $this->isStopped            = true;

        if($throwable !== null) {

            if(false === $throwable instanceof RemoteException) {

                $pid                = \getmypid();

                // Make sure that the exception is a FatalWorkerException
                $throwable          = new FatalWorkerException(
                    "Fatal Worker Exception  (id:{$this->id}, group:{$this->group->getGroupName()}, pid:$pid): "
                    .$throwable->getMessage()
                    ." in {$throwable->getFile()}:{$throwable->getLine()}",
                    0,
                    $throwable
                );
            }

            $this->sendException($throwable);
        }

        try {
            $this->workerState->markAsShutdown()->updateStateSegment();
        } catch (\Throwable) {
            // Ignore
        }

        if(false === $this->mainCancellation->isCancelled()) {
            $this->mainCancellation->cancel($throwable);
        }

        try {
            foreach ($this->periodicTasks as $task) {
                $task->cancel();
            }
        } finally {
            $this->periodicTasks    = [];
        }

        try {
            WorkerGroup::stopStrategies($this->groupsScheme, $this->logger);
        } finally {
            $this->eventEmitter->free();

            try {
                $this->workersStorage->close();
            } catch (\Throwable) {
                // Ignore
            }

            if(false === $this->workerFuture->isComplete()) {
                $this->workerFuture->complete();
            }

            if(false === $this->queue->isComplete()) {
                $this->queue->complete();
            }
        }

        if($throwable !== null) {
            throw $throwable;
        }
    }

    public function isStopped(): bool
    {
        return $this->isStopped;
    }

    public function onClose(\Closure $onClose): void
    {
        $this->mainCancellation->getCancellation()->subscribe(static fn () => $onClose());
    }

    public function __toString(): string
    {
        return $this->group->getGroupName().'-'.$this->id;
    }

    protected function initWorkerStrategies(): void
    {
        foreach ($this->groupsScheme as $group) {
            foreach ($group->getWorkerStrategies() as $strategy) {
                if($strategy instanceof WorkerStrategyInterface) {
                    $strategy->setWorker($this, $group === $this->group)->setWorkerGroup($group);
                }
            }
        }
    }

    public function addPeriodicTask(float $delay, \Closure $task): int
    {
        $self                       = \WeakReference::create($this);
        $taskId                     = \spl_object_id($task);

        $task                       = new PeriodicTask($delay, static function (string $id) use ($task, $taskId, $self) {

            try {
                $task();
            } catch (\Throwable $exception) {
                $self               = $self->get();

                if(false === $exception instanceof RemoteException) {
                    $exception      = new FatalWorkerException(
                        'Periodic task encountered an error: '.$exception->getMessage(),
                        0,
                        $exception
                    );
                }

                $self?->cancelPeriodicTask($taskId);
                $self?->getLogger()->error($exception->getMessage(), ['exception' => $exception]);
                $self?->initiateTermination($exception);
            }
        });

        $this->periodicTasks[$taskId] = $task;

        return $taskId;
    }

    public function cancelPeriodicTask(int $taskId): void
    {
        if(isset($this->periodicTasks[$taskId])) {
            $task = $this->periodicTasks[$taskId];
            unset($this->periodicTasks[$taskId]);
            $task->cancel();
        }
    }

    public function defineSoftShutdownHandler(callable $handler): void
    {
        if($this->softShutdownHandler !== null) {
            throw new \RuntimeException('Soft shutdown handler already defined');
        }

        $this->softShutdownHandler  = $handler;
    }

    public function applyGlobalErrorHandler(): void
    {
        $self                       = \WeakReference::create($this);

        EventLoop::setErrorHandler(static function (\Throwable $exception) use ($self): void {

            $self                   = $self->get();

            if($self instanceof self) {
                $self->initiateTermination($exception);
            }
        });
    }

    protected function sendException(\Throwable $exception): void
    {
        if($this->ipcChannel->isClosed()) {
            return;
        }

        if(false === $exception instanceof RemoteException) {
            $pid                    = \getmypid();
            $exception              = new RemoteException(
                "Uncaught worker (id: {$this->id}, pid: $pid) exception: "
                                                  .$exception->getMessage(),
                0,
                $exception
            );
        }

        try {
            $this->ipcChannel->send($exception);
        } catch (\Throwable) {
            // Ignore
        }
    }
}
