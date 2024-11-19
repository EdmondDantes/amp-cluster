<?php
declare(strict_types=1);

namespace IfCastle\AmpPool;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\CompositeCancellation;
use Amp\DeferredCancellation;
use Amp\DeferredFuture;
use Amp\Future;
use Amp\Parallel\Context\Context;
use Amp\Parallel\Context\ContextFactory;
use Amp\Parallel\Context\ContextPanicError;
use Amp\Parallel\Context\DefaultContextFactory;
use Amp\Parallel\Ipc\IpcHub;
use Amp\Parallel\Ipc\LocalIpcHub;
use Amp\Parallel\Worker\TaskFailureThrowable;
use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;
use Amp\Sync\ChannelException;
use Amp\TimeoutCancellation;
use IfCastle\AmpPool\Exceptions\FatalWorkerException;
use IfCastle\AmpPool\Exceptions\RemoteException;
use IfCastle\AmpPool\Exceptions\StopException;
use IfCastle\AmpPool\Exceptions\TerminateWorkerException;
use IfCastle\AmpPool\Exceptions\WorkerPoolException;
use IfCastle\AmpPool\Exceptions\WorkerShouldBeStopped;
use IfCastle\AmpPool\Internal\Safe;
use IfCastle\AmpPool\Internal\WorkerProcessContext;
use IfCastle\AmpPool\Strategies\JobClient\JobClientDefault;
use IfCastle\AmpPool\Strategies\JobExecutor\JobExecutorScheduler;
use IfCastle\AmpPool\Strategies\PickupStrategy\PickupLeastJobs;
use IfCastle\AmpPool\Strategies\RestartStrategy\RestartAlways;
use IfCastle\AmpPool\Strategies\RunnerStrategy\DefaultRunner;
use IfCastle\AmpPool\Strategies\ScalingStrategy\ScalingByRequest;
use IfCastle\AmpPool\Strategies\SocketStrategy\Unix\SocketUnixStrategy;
use IfCastle\AmpPool\Strategies\SocketStrategy\Windows\SocketWindowsStrategy;
use IfCastle\AmpPool\Strategies\WorkerStrategyInterface;
use IfCastle\AmpPool\Telemetry\Collectors\ApplicationCollector;
use IfCastle\AmpPool\Telemetry\Collectors\ApplicationCollectorInterface;
use IfCastle\AmpPool\WatcherEvents\WorkerProcessStarted;
use IfCastle\AmpPool\WatcherEvents\WorkerProcessTerminating;
use IfCastle\AmpPool\Worker\Internal\Exceptions\ScalingTrigger;
use IfCastle\AmpPool\Worker\Internal\WorkerDescriptor;
use IfCastle\AmpPool\WorkersStorage\WorkersStorage;
use IfCastle\AmpPool\WorkersStorage\WorkersStorageInterface;
use Psr\Log\LoggerInterface as PsrLogger;
use Revolt\EventLoop;
use const Amp\Process\IS_WINDOWS;
use function Amp\async;
use function Amp\delay;
use function Amp\trapSignal;

/**
 * Worker Pool Manager Class.
 *
 * A worker pool allows you to create groups of processes belonging to different types of workers,
 * and then use them to perform tasks.
 */
final class WorkerPool implements WorkerPoolInterface
{
    protected int $workerStartTimeout = 5;
    protected int $workerStopTimeout  = 60;
    private int $lastGroupId        = 0;

    private bool $shouldRestart      = false;

    /**
     * @var WorkerDescriptor[]
     */
    protected array $workers        = [];

    protected array $poolContext    = [];

    protected readonly Queue $queue;
    private readonly ConcurrentIterator $iterator;
    private bool $running           = false;

    private WorkersStorageInterface $workersStorage;

    /**
     * Cancellation token for the main process watcher.
     *
     */
    private ?DeferredCancellation $mainCancellation = null;

    /**
     * Cancellation token for the workers.
     *
     */
    private ?DeferredCancellation $workersCancellation = null;

    private ?DeferredCancellation $scalingTrigger = null;

    private ?DeferredFuture $scalingFuture = null;

    /**
     * @var WorkerGroupInterface[]
     */
    private array $groupsScheme             = [];

    private WorkerEventEmitterInterface $eventEmitter;

    private mixed $pidFileHandler           = null;

    private ApplicationCollectorInterface|null $applicationCollector = null;

    private string $applicationCollectorId  = '';

    public function __construct(
        private readonly IpcHub $hub                    = new LocalIpcHub(),
        private readonly string $workersStorageClass    = WorkersStorage::class,
        private readonly string $collectorClass         = ApplicationCollector::class,
        private ?ContextFactory $contextFactory         = null,
        private ?PsrLogger $logger                      = null,
        private readonly string|bool $pidFile           = false,
        private readonly int $statsUpdateInterval       = 5
    ) {
        $this->contextFactory       ??= new DefaultContextFactory(ipcHub: $this->hub);
        $this->eventEmitter         = new WorkerEventEmitter;
    }

    private function initWorkersStorage(): void
    {
        if (\class_exists($this->workersStorageClass) === false) {
            throw new \Error("The workers storage class '{$this->workersStorageClass}' does not exist");
        }

        $this->workersStorage       = \forward_static_call([$this->workersStorageClass, 'instanciate'], \count($this->workers), 0);

        // Assign worker states to workers
        foreach ($this->workers as $workerDescriptor) {
            $workerDescriptor->workerState = $this->workersStorage->getWorkerState($workerDescriptor->id);
            $workerDescriptor->workerState->setGroupId($workerDescriptor->group->getWorkerGroupId())->update();
        }
    }

    private function initApplicationCollector(): void
    {
        if ($this->collectorClass === '') {
            return;
        }

        $this->applicationCollector = \forward_static_call([$this->collectorClass, 'instanciate'], $this->workersStorage);

        $this->applicationCollector->startApplication();
        $this->applicationCollectorId = EventLoop::repeat($this->statsUpdateInterval, $this->updateApplicationState(...));
    }

    public function getWorkersStorage(): WorkersStorageInterface
    {
        return $this->workersStorage;
    }

    public function getIpcHub(): IpcHub
    {
        return $this->hub;
    }

    public function getLogger(): PsrLogger|null
    {
        return $this->logger;
    }

    public function describeGroup(WorkerGroupInterface $group): self
    {
        $group                      = clone $group;

        if (\class_exists($group->getEntryPointClass()) === false) {
            throw new \Error("The worker class '{$group->getEntryPointClass()}' does not exist");
        }

        if ($group->getMinWorkers() < 0) {
            throw new \Error('The minimum number of workers must be greater than zero');
        }

        if ($group->getMaxWorkers() === 0) {
            $group->defineMaxWorkers($group->getMinWorkers());
        }

        if ($group->getMaxWorkers() < $group->getMinWorkers()) {
            throw new \Error('The maximum number of workers must be greater than or equal to the minimum number of workers');
        }

        if ($group->getMaxWorkers() === 0) {
            throw new \Error('The maximum number of workers must be greater than zero');
        }

        $groupId                    = ++$this->lastGroupId;

        if ($group->getGroupName() === '') {
            // If group name undefined, use the worker class name without a namespace
            $groupName              = \strrchr($group->getEntryPointClass(), '\\');

            if ($groupName === false) {
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

    public function findGroup(int|string $groupIdOrName): WorkerGroupInterface|null
    {
        foreach ($this->getGroupsScheme() as $group) {
            if ($group->getWorkerGroupId() === $groupIdOrName || $group->getGroupName() === $groupIdOrName) {
                return $group;
            }
        }

        return null;
    }

    /**
     * @throws \Exception
     */
    public function validateGroupsScheme(): void
    {
        if (empty($this->groupsScheme)) {
            throw new \Exception('The worker groups scheme is empty');
        }

        $lastGroupId                = 0;

        foreach ($this->groupsScheme as $group) {

            if (\class_exists($group->getEntryPointClass()) === false) {
                throw new \Exception("The worker class '{$group->getEntryPointClass()}' does not exist");
            }

            if ($group->getWorkerGroupId() <= $lastGroupId) {
                throw new \Exception('The group ID must be greater than the previous group id');
            }

            $lastGroupId            = $group->getWorkerGroupId();

            if ($group->getMinWorkers() < 0) {
                throw new \Exception('The minimum number of workers must be greater than zero or equal to zero');
            }

            if ($group->getMaxWorkers() < $group->getMinWorkers()) {
                throw new \Exception('The maximum number of workers must be greater than or equal to the minimum number of workers');
            }

            if ($group->getMaxWorkers() === 0) {
                throw new \Exception('The maximum number of workers must be greater than zero');
            }

            foreach ($group->getJobGroups() as $jobGroupId) {

                // Resolve group name to group id
                if (\is_string($jobGroupId)) {
                    $jobGroupId     = $this->findGroup($jobGroupId)?->getWorkerGroupId() ?? 0;
                }

                if (false === \array_key_exists($jobGroupId, $this->groupsScheme)) {
                    throw new \Exception("The job group id '{$jobGroupId}' is not found in the worker groups scheme");
                }

                if ($jobGroupId === $group->getWorkerGroupId()) {
                    throw new \Exception("The job group id '{$jobGroupId}' must be different from the worker group id");
                }
            }

        }
    }

    #[\Override]
    public function getPoolsContext(): array
    {
        return $this->poolContext;
    }

    #[\Override]
    public function setPoolContext(array $context): static
    {
        $this->poolContext          = $context;
        return $this;
    }

    public function run(): void
    {
        if ($this->running) {
            throw new \Exception('The server watcher is already running or has already run');
        }

        $this->catchPidFile();

        $this->validateGroupsScheme();
        $this->applyGroupScheme();

        if (\count($this->workers) <= 0) {
            throw new \Exception('The number of workers must be greater than zero');
        }

        $this->initWorkersStorage();
        $this->initApplicationCollector();

        $this->running              = true;
        $this->mainCancellation     = new DeferredCancellation;

        try {
            WorkerGroup::startStrategies($this->groupsScheme);
        } catch (\Throwable $exception) {
            $this->running          = false;

            if ($this->mainCancellation->isCancelled() === false) {
                $this->mainCancellation->cancel($exception);
            }

            $this->mainCancellation = null;

            throw $exception;
        }

        //
        // Main watcher loop
        //
        do {

            if ($this->shouldRestart) {
                $this->applicationCollector?->restartApplication();
            }

            $this->shouldRestart    = false;

            if (false === $this->workersCancellation?->isCancelled()) {
                $this->workersCancellation->cancel();
            }

            $this->workersCancellation = new DeferredCancellation;

            if (true === $this->scalingFuture?->isComplete() || $this->scalingFuture === null) {
                $this->scalingFuture = new DeferredFuture;
            }

            $workersWatcher         = async($this->workersWatcher(...));

            if (IS_WINDOWS) {
                $this->awaitWindowsEvents();
            } else {
                $this->awaitUnixEvents();
            }

            Future\await([$workersWatcher]);

        } while ($this->shouldRestart);

        $this->running              = false;
    }

    public function awaitStart(): void
    {
        if ($this->mainCancellation === null || false === $this->running) {
            return;
        }

        // await scaling first
        if ($this->scalingFuture !== null) {
            try {
                Future\await([$this->scalingFuture->getFuture()], $this->mainCancellation?->getCancellation());
            } catch (CancelledException) {
                return;
            }
        }

        // and await workers
        $futures                    = [];

        foreach ($this->workers as $workerDescriptor) {

            $future                = $workerDescriptor->getStartFuture();

            if ($future !== null) {
                $futures[]          = $future;
            }
        }

        try {
            Future\await($futures, $this->mainCancellation?->getCancellation());
        } catch (CancelledException $exception) {
        }
    }

    private function workersWatcher(): void
    {
        try {
            $futures                = [];

            do {

                if (false === $this->scalingTrigger?->isCancelled()) {
                    $this->scalingTrigger->cancel();
                }

                $this->scalingTrigger = new DeferredCancellation;

                // Clear completed futures
                foreach ($futures as $key => $future) {
                    if ($future->isComplete()) {
                        unset($futures[$key]);
                    }
                }

                foreach ($this->workers as $worker) {
                    if ($worker->shouldBeStarted() && false === $worker->isRunningOrWillBeRunning()) {

                        $worker->starting();

                        $futures[]  = async($this->runWorkerWatcher(...), $worker);
                    }
                }

                if (false === $this->scalingFuture?->isComplete()) {
                    $this->scalingFuture->complete();
                }

                try {
                    Future\await($futures, $this->scalingTrigger->getCancellation());
                } catch (CancelledException|ScalingTrigger $exception) {
                    if (false === $exception->getPrevious() instanceof ScalingTrigger) {
                        throw $exception;
                    }
                }

                // Stop cycle when all workers are stopped
            } while (\count($futures) > 0 && true !== $this->workersCancellation?->isCancelled());

        } finally {
            // All workers are stopped here, so triggers cancellation if not already canceled
            if (false === $this->workersCancellation->isCancelled()) {
                $this->workersCancellation->cancel();
            }

            if (false === $this->scalingFuture?->isComplete()) {
                $this->scalingFuture->complete();
            }
        }
    }

    public function getMainCancellation(): ?Cancellation
    {
        return $this->mainCancellation?->getCancellation();
    }

    public function scaleWorkers(int $groupId, int $delta): int
    {
        if ($delta === 0) {
            return 0;
        }

        if ($this->scalingTrigger === null) {
            throw new WorkerPoolException('The scaling trigger is not initialized');
        }

        $group                      = $this->groupsScheme[$groupId] ?? null;

        if ($group === null) {
            throw new \Error("The worker group with ID '{$groupId}' is not found");
        }

        $isDecrease                 = $delta < 0;
        $delta                      = \abs($delta);
        $handled                    = 0;

        $workers                    = $isDecrease ? \array_reverse($this->workers) : $this->workers;

        foreach ($workers as $workerDescriptor) {
            if ($workerDescriptor->group->getWorkerGroupId() !== $groupId) {
                continue;
            }

            // Skip stopped workers
            if ($workerDescriptor->isStoppedForever()) {
                continue;
            }

            if ($handled >= $delta) {
                break;
            }

            if ($isDecrease && $workerDescriptor->shouldBeStarted()) {
                $workerDescriptor->willBeStopped();

                if ($workerDescriptor->isRunning()) {
                    $workerDescriptor->getWorkerProcess()->softShutdown();
                }

                $handled++;
            } elseif (false === $isDecrease && false === $workerDescriptor->shouldBeStarted()) {
                $workerDescriptor->willBeStarted();
                $handled++;
            }
        }

        if (false === $this->scalingFuture?->isComplete()) {
            $this->scalingFuture->complete();
        }

        $this->scalingFuture        = new DeferredFuture;

        $this->logger?->debug('Scaling workers request', ['group_id' => $groupId, 'delta' => $isDecrease? -$delta : $delta, 'handled' => $handled]);
        $this->scalingTrigger->cancel(new ScalingTrigger);

        return $handled;
    }

    public function restartWorker(int $workerId): bool
    {
        foreach ($this->workers as $workerDescriptor) {
            if ($workerDescriptor->id === $workerId) {
                if ($workerDescriptor->isRunning()) {
                    $workerDescriptor->getWorkerProcess()->softShutdown();
                    return true;
                }
            }
        }

        return false;
    }

    public function getWorkerEventEmitter(): WorkerEventEmitterInterface
    {
        return $this->eventEmitter;
    }

    private function awaitUnixEvents(): void
    {
        if ($this->mainCancellation === null || $this->workersCancellation === null) {
            return;
        }

        $cancellation           = new CompositeCancellation(
            $this->mainCancellation->getCancellation(),
            $this->workersCancellation->getCancellation()
        );

        try {
            $signal             = trapSignal([\SIGINT, \SIGTERM, \SIGUSR1, \SIGUSR2], true, $cancellation);
        } catch (CancelledException) {
            return;
        }

        if ($signal === \SIGINT || $signal === \SIGTERM) {
            $this->logger?->info('Server will stop due to signal SIGINT or SIGTERM');
            $this->stop();
        } elseif ($signal === \SIGUSR1) {
            $this->logger?->info('Server should hard-reload due to signal SIGUSR1');
            $this->restart();
        } elseif ($signal === \SIGUSR2) {
            $this->logger?->info('Server should soft-reload due to signal SIGUSR2');
            $this->restart(true);
        }
    }

    private function awaitWindowsEvents(): void
    {
        if ($this->mainCancellation === null || $this->workersCancellation === null) {
            return;
        }

        $suspension             = EventLoop::getSuspension();
        $cancellation           = new CompositeCancellation($this->mainCancellation->getCancellation(), $this->workersCancellation->getCancellation());

        $id                     = $cancellation->subscribe(static fn (CancelledException $exception) => $suspension->throw($exception));

        $handler                = null;

        $handler                = static function () use ($suspension, &$handler, $id): void {

            if ($handler === null) {
                return;
            }

            \sapi_windows_set_ctrl_handler($handler, false);
            $handler            = null;

            echo 'The server will attempt to stop gracefully with CTRL-C...'.PHP_EOL;

            $suspension->resume();
        };

        //
        // WARNING: sapi_windows_set_ctrl_handler() breaks the event loop suspension.
        // No use this on the production server.
        //
        \sapi_windows_set_ctrl_handler($handler);

        try {
            $suspension->suspend();
        } catch (CancelledException) {
            // Ignore
        } finally {
            /** @psalm-suppress PossiblyNullArgument $id will not be null if $cancellation is not null. */
            $cancellation->unsubscribe($id);

            if ($handler !== null) {
                \sapi_windows_set_ctrl_handler($handler, false);
                $handler            = null;
            }

            $this->stop();
        }
    }

    protected function applyGroupScheme(): void
    {
        foreach ($this->groupsScheme as $group) {
            $this->fillWorkersGroup($group);
        }
    }

    private function runWorkerWatcher(WorkerDescriptor $workerDescriptor): void
    {
        while ($workerDescriptor->shouldBeStarted() && true !== $this->workersCancellation?->isCancelled()) {
            $this->startWorker($workerDescriptor);
            $this->workerWatcher($workerDescriptor);
        }
    }

    private function startWorker(WorkerDescriptor $workerDescriptor): void
    {
        $runnerStrategy             = $workerDescriptor->group->getRunnerStrategy();

        if ($runnerStrategy === null) {
            throw new \Error('The runner strategy is not defined');
        }

        try {
            $context                = $this->contextFactory->start(
                $runnerStrategy->getScript(),
                new TimeoutCancellation($this->workerStartTimeout + 6000, 'The worker start timeout ('.$this->workerStartTimeout.') has been exceeded')
            );
        } catch (\Throwable $exception) {
            $this->logger?->critical('Starting the worker #'.$workerDescriptor->id
                                     .' group "'.$workerDescriptor->group->getGroupName()
                                     .'" failed: ' . $exception->getMessage(), ['exception' => $exception]);

            throw new FatalWorkerException('Starting the worker failed', 0, $exception);
        }

        try {

            $workerProcess          = new WorkerProcessContext(
                $workerDescriptor->id,
                $context,
                $this->workersCancellation->getCancellation(),
                $this->eventEmitter,
                $workerDescriptor->getStartDeferred(),
                $this->workerStopTimeout
            );

            if ($this->logger !== null) {
                $workerProcess->setLogger($this->logger);
            }

            $workerDescriptor->setWorkerProcess($workerProcess);

            $runnerStrategy->initiateWorkerContext($context, $workerDescriptor->id, $workerDescriptor->group, $this->poolContext);

            $this->eventEmitter->emitWorkerEvent(
                new WorkerProcessStarted($workerDescriptor->id, $workerDescriptor->group, $context),
                $workerDescriptor->id
            );

            $workerProcess->info(\sprintf('Started %s worker #%d', $workerDescriptor->group->getWorkerType()->value, $workerDescriptor->id));

        } catch (\Throwable $exception) {

            if (false === $context->isClosed()) {
                $context->close();
            }

            $this->eventEmitter->emitWorkerEvent(
                new WorkerProcessTerminating(
                    $workerDescriptor->id,
                    $workerDescriptor->group,
                    $context,
                    $exception
                ),
                $workerDescriptor->id
            );

            throw new FatalWorkerException(
                "Starting the worker '{$workerDescriptor->id}' failed. Sending the pool context failed",
                previous: $exception
            );
        }
    }

    /**
     * Watcher for the worker process and restarts it if necessary.
     *
     *
     * @throws TaskFailureThrowable
     * @throws \Throwable
     */
    private function workerWatcher(WorkerDescriptor $workerDescriptor): void
    {
        if (false === $this->running) {

            $workerDescriptor->started();

            if ($workerDescriptor->getWorkerProcess() !== null) {
                $this->eventEmitter->emitWorkerEvent(
                    new WorkerProcessTerminating(
                        $workerDescriptor->id,
                        $workerDescriptor->group,
                        $workerDescriptor->getWorkerProcess()->getContext()
                    ),
                    $workerDescriptor->id
                );
            }

            return;
        }

        $id                         = $workerDescriptor->id;
        $workerProcess              = $workerDescriptor->getWorkerProcess();
        $processContext             = $workerProcess->getContext();

        try {

            $exitResult             = $this->workerEventLoop($workerDescriptor);

            $this->freeWorkerDescriptor($workerDescriptor);

            $this->eventEmitter->emitWorkerEvent(
                new WorkerProcessTerminating($workerDescriptor->id, $workerDescriptor->group, $processContext),
                $workerDescriptor->id
            );

            if ($exitResult instanceof TerminateWorkerException) {
                $workerDescriptor->markAsStoppedForever();
                $workerProcess->error("Worker {$id} will be stopped forever: {$exitResult->getMessage()}");

                return;
            }

            if ($exitResult instanceof WorkerShouldBeStopped) {
                return;
            }

            $restarting             = $workerDescriptor->group->getRestartStrategy()?->shouldRestart($exitResult) ?? -1;

            // Restart the worker if the server is still running and the worker should be restarted.
            // We always terminate the worker if the server is not running
            // or $exitResult is an instance of TerminateWorkerException.
            if ($this->running && $restarting >= 0) {

                if ($restarting > 0) {
                    $workerProcess->info("Worker {$id} will be restarted in {$restarting} seconds");
                    delay($restarting);
                }

                return;

            } elseif ($restarting < 0) {

                $workerDescriptor->markAsStoppedForever();

                $workerProcess->warning(
                    "Worker {$id} will not be restarted: " .
                    $workerDescriptor->group->getRestartStrategy()?->getFailReason() ?? ''
                );
            }

        } catch (\Throwable $exception) {
            $this->stop();
            throw $exception;
        }
    }

    /**
     * Run the worker event loop and return the exit result.
     *
     *
     * @throws TaskFailureThrowable
     * @throws \Throwable
     */
    private function workerEventLoop(WorkerDescriptor $workerDescriptor): mixed
    {
        $id                         = $workerDescriptor->id;
        $workerProcess              = $workerDescriptor->getWorkerProcess();
        $exitResult                 = null;

        try {

            $workerProcess->runWorkerLoop();
            $workerProcess->info("Worker #{$id} terminated cleanly");

        } catch (CancelledException $exception) {

            $exitResult         = $exception;
            $workerProcess->notice("Worker #{$id} should be forcefully terminated");

        } catch (ChannelException $exception) {

            /**
             * The IPC socket has broken the connection,
             * and communication with the child process has been disrupted.
             * We interpret this as an abnormal termination of the worker.
             */
            $exitResult = $exception;

            $workerProcess->error(
                "Worker #{$id} died unexpectedly: {$exception->getMessage()}" .
                ($this->running ? ', restarting...' : '')
            );

            $remoteException = $exception->getPrevious();

            if ($remoteException instanceof TaskFailureThrowable || $remoteException instanceof ContextPanicError) {

                if ($remoteException->getOriginalClassName() === FatalWorkerException::class) {
                    // The Worker died due to a fatal error, so we should stop the server.
                    $workerDescriptor->markAsStoppedForever();
                    $this->logger?->error(
                        'Server shutdown due to fatal worker error: ' . $remoteException->getMessage()
                    );

                    throw $remoteException;
                }

                if ($remoteException->getOriginalClassName() === TerminateWorkerException::class) {
                    // The Worker has terminated itself cleanly.
                    $exitResult = new TerminateWorkerException;
                    $workerProcess->info("Worker #{$id} terminated yourself cleanly without restart");
                }
            }

        } catch (RemoteException $exception) {

            //
            // This exception was received from the worker process.
            //

            $exitResult             = $exception;

            if ($exception instanceof TerminateWorkerException) {
                $workerProcess->info("Worker #{$id} terminated cleanly without restart");
            } elseif ($exception instanceof FatalWorkerException) {
                $this->logger?->error(
                    'Server shutdown due to fatal worker error: ' . $exception->getMessage()
                );

                throw $exception;
            }

        } catch (WorkerShouldBeStopped $exception) {

            // The worker has terminated itself cleanly.
            $exitResult         = $exception;
            $workerProcess->info("Worker #{$id} terminated cleanly without restart");

        } catch (\Throwable $exception) {

            $workerDescriptor->workerState?->increaseAndUpdateShutdownErrors();
            $workerProcess->error("Worker #{$id} failed: " . $exception->getMessage(), ['exception' => $exception]);
            throw $exception;

        }

        return $exitResult;
    }

    private function freeWorkerDescriptor(WorkerDescriptor $workerDescriptor): void
    {
        if ($workerDescriptor->workerState === null) {
            return;
        }

        try {
            $workerDescriptor->workerState->markUsShutdown();

        } catch (\Throwable $exception) {
            $this->logger?->error('Failed to read or update the worker state: '.$exception->getMessage(), ['exception' => $exception]);
        }
    }

    private function fillWorkersGroup(WorkerGroupInterface $group): void
    {
        if ($group->getWorkerGroupId() === 0) {
            throw new \Error('The group id must be greater than zero');
        }

        if ($group->getMinWorkers() < 0) {
            throw new \Error('The minimum number of workers must be greater than zero');
        }

        if ($group->getMaxWorkers() < $group->getMinWorkers()) {
            throw new \Error('The maximum number of workers must be greater than or equal to the minimum number of workers');
        }

        $baseWorkerId               = $this->getLastWorkerId() + 1;

        // All workers in the group will have the same strategies
        $this->defaultWorkerStrategies($group);
        $this->initWorkerStrategies($group);

        $minWorkers                 = $group->getMinWorkers() - 1;

        foreach (\range($baseWorkerId, $baseWorkerId + $group->getMaxWorkers() - 1) as $id) {
            $this->addWorker(new WorkerDescriptor(
                $id,
                $group,
                $id <= ($baseWorkerId + $minWorkers)
            ));
        }
    }

    private function getLastWorkerId(): int
    {
        $maxId                      = 0;

        foreach ($this->workers as $worker) {
            if ($worker->id > $maxId) {
                $maxId              = $worker->id;
            }
        }

        return $maxId;
    }

    private function addWorker(WorkerDescriptor $worker): void
    {
        $this->workers[]            = $worker;
    }

    public function getWorkers(): array
    {
        $workers                    = [];

        foreach ($this->workers as $workerDescriptor) {
            $workers[]              = $workerDescriptor->id;
        }

        return $workers;
    }

    public function findWorkerContext(int $workerId): Context|null
    {
        foreach ($this->workers as $workerDescriptor) {
            if ($workerDescriptor->id === $workerId) {
                return $workerDescriptor->getWorkerProcess()?->getContext();
            }
        }

        return null;
    }

    public function isWorkerRunning(int $workerId): bool
    {
        foreach ($this->workers as $workerDescriptor) {
            if ($workerDescriptor->id === $workerId) {
                return $workerDescriptor->isRunning();
            }
        }

        return false;
    }

    public function findWorkerCancellation(int $workerId): Cancellation|null
    {
        foreach ($this->workers as $workerDescriptor) {
            if ($workerDescriptor->id === $workerId) {
                return $workerDescriptor->getWorkerProcess()->getCancellation();
            }
        }

        return null;
    }

    public function stop(): void
    {
        if (false === $this->mainCancellation?->isCancelled()) {
            $this->mainCancellation->cancel(new StopException('The worker pool was stopped'));
        }

        $this->stopWorkers();

        if ($this->applicationCollector !== null) {
            EventLoop::cancel($this->applicationCollectorId);
            $this->applicationCollector->stopApplication();
        }
    }

    private function stopWorkers(?\Throwable $throwable = null): void
    {
        if (false === $this->workersCancellation?->isCancelled()) {
            $this->workersCancellation->cancel($throwable ?? new WorkerShouldBeStopped);
        }
    }

    public function restart(bool $isSoft = false): void
    {
        if ($isSoft) {

            $this->applicationCollector?->restartApplication();

            foreach ($this->workers as $workerDescriptor) {
                if ($workerDescriptor->isRunning()) {
                    $workerDescriptor->getWorkerProcess()->softShutdown();
                }
            }

            $this->logger?->info('Server should be softly restarted');
            return;
        }

        $this->shouldRestart        = true;
        $this->stopWorkers();
        $this->logger?->info('Server should be restarted');
    }

    public function countWorkers(int $groupId, bool $onlyRunning = false, bool $notRunning = false): int
    {
        $count                      = 0;

        foreach ($this->workers as $worker) {
            if ($worker->group->getWorkerGroupId() !== $groupId) {
                continue;
            }

            if ($onlyRunning && $worker->isRunning()) {
                $count++;
            } elseif ($notRunning && $worker->isNotRunning()) {
                $count++;
            } elseif (false === $onlyRunning && false === $notRunning) {
                $count++;
            }
        }

        return $count;
    }

    private function defaultWorkerStrategies(WorkerGroupInterface $group): void
    {
        if ($group->getRunnerStrategy() === null) {
            $group->defineRunnerStrategy(new DefaultRunner);
        }

        if ($group->getPickupStrategy() === null) {
            $group->definePickupStrategy(new PickupLeastJobs);
        }

        if ($group->getScalingStrategy() === null) {
            $group->defineScalingStrategy(new ScalingByRequest);
        }

        if ($group->getRestartStrategy() === null) {
            $group->defineRestartStrategy(new RestartAlways);
        }

        if ($group->getJobExecutor() === null && $group->getWorkerType() === WorkerTypeEnum::JOB) {
            $group->defineJobExecutor(new JobExecutorScheduler);
        }

        if ($group->getJobClient() === null && $group->getJobGroups() !== []) {
            $group->defineJobClient(new JobClientDefault);
        }

        if ($group->getSocketStrategy() === null && $group->getWorkerType() === WorkerTypeEnum::REACTOR) {
            $group->defineSocketStrategy(IS_WINDOWS ? new SocketWindowsStrategy : new SocketUnixStrategy);
        }
    }

    private function initWorkerStrategies(WorkerGroupInterface $group): void
    {
        foreach ($group->getWorkerStrategies() as $strategy) {
            if ($strategy instanceof WorkerStrategyInterface) {
                $strategy->setWorkerPool($this)->setWorkerGroup($group);
            }
        }
    }

    private function updateApplicationState(): void
    {
        $workersPid                 = [];

        foreach ($this->workers as $workerDescriptor) {
            if ($workerDescriptor->isRunning()) {
                $workersPid[]       = $workerDescriptor->getWorkerProcess()?->getPid() ?? 0;
            } else {
                $workersPid[]       = 0;
            }
        }

        $this->applicationCollector?->updateApplicationState($workersPid);
    }

    public function getApplicationPid(): int
    {
        if ($this->pidFileHandler !== null) {
            return (int) \getmypid();
        }

        $pidFile                    = $this->getPidFile();
        $pidFileHandle              = null;

        try {
            $pidFileHandle          = Safe::execute(fn () => \fopen($pidFile, 'c'));
            $pidFileLocked          = Safe::execute(fn () => \flock($pidFileHandle, LOCK_EX | LOCK_NB));

            if (false === $pidFileLocked) {
                return (int) \file_get_contents($pidFile);
            }

        } catch (\Throwable) {
        } finally {
            if ($pidFileHandle) {
                \fclose($pidFileHandle);
            }
        }

        return 0;
    }

    public function getPidFile(): string
    {
        if ($this->pidFile === false) {
            return '';
        }

        if (\is_string($this->pidFile) && $this->pidFile !== '') {
            return $this->pidFile;
        }

        return \getcwd().'/server.pid';
    }

    public function applyGlobalErrorHandler(): void
    {
        $logger                     = \WeakReference::create($this->logger);
        $self                       = \WeakReference::create($this);

        EventLoop::setErrorHandler(static function (\Throwable $exception) use ($logger, $self): void {

            $logger                 = $logger->get();
            $self                   = $self->get();

            $logger?->error('Uncaught exception: ' . $exception->getMessage(), ['exception' => $exception]);

            // Try to stop gracefully
            $self?->stop();
        });
    }

    private function catchPidFile(): void
    {
        $pidFile                    = $this->getPidFile();

        if ($pidFile === '') {
            return;
        }

        $this->pidFileHandler       = null;
        $pidFileLocked              = false;

        try {
            // Try to lock the pid file without waiting
            $this->pidFileHandler = Safe::execute(fn () => \fopen($pidFile, 'c'));
            $pidFileLocked        = Safe::execute(fn () => \flock($this->pidFileHandler, LOCK_EX | LOCK_NB));

            if (!$pidFileLocked) {
                echo "Failed to lock the pid file: another instance is running... [EXIT]\n";
                return;
            }

            \ftruncate($this->pidFileHandler, 0);
            \fwrite($this->pidFileHandler, (string) \getmypid());

        } catch (\Throwable $throwable) {
            echo "Failed to lock the pid file: ".$throwable->getMessage()."\n";
            return;
        } finally {
            if ($this->pidFileHandler !== null && true !== $pidFileLocked) {
                \fclose($this->pidFileHandler);
                $this->pidFileHandler = null;
            }

            if (false === $pidFileLocked) {
                if ($this->tryHandleCliCommands()) {
                    exit(0);
                }

                exit(1);
            }
        }
    }

    private function freePidFile(): void
    {
        if (\is_resource($this->pidFileHandler)) {
            \fclose($this->pidFileHandler);

            try {
                Safe::execute(fn () => \unlink($this->getPidFile()));
            } catch (\Throwable $throwable) {
                echo 'Failed to remove the pid file: '.$throwable->getMessage().PHP_EOL;
            }
        }
    }

    private function tryHandleCliCommands(): bool
    {
        // we try to handle CLI commands: stop, restart

        $args                       = $_SERVER['argv'] ?? [];

        if (\count($args) < 2) {
            return false;
        }

        $command                    = $args[1];

        if (IS_WINDOWS) {
            echo 'The stop and restart commands are not supported on Windows'.PHP_EOL;
            return false;
        }

        if (false === \function_exists('posix_kill')) {
            echo 'The stop and restart commands are not supported because posix_kill not exists'.PHP_EOL;
            return false;
        }

        $this->initWorkersStorage();

        try {
            $this->workersStorage->getApplicationState()->read();
            $pid                    = $this->workersStorage->getApplicationState()->getPid();
        } catch (\Throwable) {
            $pid                    = 0;
        }

        if ($pid === 0) {
            echo 'Failed to get the application PID'.PHP_EOL;
            return false;
        }

        if ($command === 'stop') {
            echo 'The application will be stopped'.PHP_EOL;
            posix_kill($pid, \SIGTERM);
            return true;
        }

        if ($command === 'restart') {
            echo 'The application will be restarted'.PHP_EOL;
            posix_kill($pid, \SIGUSR2);
        }

        return false;
    }

    public function __destruct()
    {
        $this->freePidFile();
    }
}
