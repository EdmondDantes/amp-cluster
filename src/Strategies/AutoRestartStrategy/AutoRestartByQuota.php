<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies\AutoRestartStrategy;

use IfCastle\AmpPool\Strategies\WorkerStrategyAbstract;
use Revolt\EventLoop;

/**
 * Restart worker by quota.
 *
 * The worker will be restarted when it reaches the specified quota.
 * The quota can be defined by the number of requests, time, jobs, or system memory.
 */
final class AutoRestartByQuota extends WorkerStrategyAbstract
{
    private string $checkerId                   = '';
    private array $workersLastState             = [];

    public function __construct(
        public readonly int $maxRequests        = 0,
        public readonly int $maxTime            = 0,
        public readonly int $maxJobs            = 0,
        public readonly int $maxSystemMemory    = 0,
        public readonly int $frequencyOfCheck   = 60
    ) {
    }

    public function onStarted(): void
    {
        $workerPool                 = $this->getWorkerPool();

        if ($workerPool === null) {
            return;
        }

        $this->checkerId            = EventLoop::repeat($this->frequencyOfCheck, $this->checkWorkersByQuota(...));
    }

    public function onStopped(): void
    {
        $workerPool                 = $this->getWorkerPool();

        if ($workerPool === null) {
            return;
        }

        EventLoop::disable($this->checkerId);
    }

    private function checkWorkersByQuota(): void
    {
        $workerPool                 = $this->getWorkerPool();
        $group                      = $this->getWorkerGroup();

        if ($workerPool === null || $group === null) {
            EventLoop::disable($this->checkerId);
            return;
        }

        $groupWorkerIds               = [];
        $currentStates                = [];

        foreach ($workerPool->getWorkersStorage()->foreachWorkers() as $workerState) {

            if ($workerState->getGroupId() !== $group->getWorkerGroupId()) {
                continue;
            }

            $groupWorkerIds[]         = $workerState->getWorkerId();
            $currentStates[]          = $workerState;

            if (false === \array_key_exists($workerState->getWorkerId(), $this->workersLastState)) {
                $this->workersLastState[$workerState->getWorkerId()] = $workerState;
            }

            $lastState                = $this->workersLastState[$workerState->getWorkerId()];

            if ($lastState->getStartedAt() !== $workerState->getStartedAt()) {
                $this->workersLastState[$workerState->getWorkerId()] = $workerState;
                $lastState            = $workerState;
            }

            if ($this->maxRequests > 0 && ($workerState->getConnectionsProcessed() - $lastState->getConnectionsProcessed()) >= $this->maxRequests) {
                $workerPool->getLogger()?->info('Restart worker of group "'.$group->getGroupName().'" by quota maxRequests: ' . $this->maxRequests);
                $workerPool->restartWorker($workerState->getWorkerId());
                $this->workersLastState[$workerState->getWorkerId()] = $workerState;
                continue;
            }

            if ($this->maxTime > 0 && ($workerState->getStartedAt() - $lastState->getStartedAt()) >= $this->maxTime) {
                $workerPool->getLogger()?->info('Restart worker of group "'.$group->getGroupName().'" by quota maxTime: ' . $this->maxTime);
                $workerPool->restartWorker($workerState->getWorkerId());
                $this->workersLastState[$workerState->getWorkerId()] = $workerState;
                continue;
            }

            if ($this->maxJobs > 0 && ($workerState->getJobProcessed() - $lastState->getJobProcessed()) >= $this->maxJobs) {
                $workerPool->getLogger()?->info('Restart worker of group "'.$group->getGroupName().'" by quota maxJobs: ' . $this->maxJobs);
                $workerPool->restartWorker($workerState->getWorkerId());
                $this->workersLastState[$workerState->getWorkerId()] = $workerState;
            }
        }

        if ($this->maxSystemMemory <= 0) {
            return;
        }

        foreach ($workerPool->getWorkersStorage()->getMemoryUsage()->getStats() as $index => $memoryUsage) {

            $workerId               = $index + 1;
            $workerState            = $currentStates[$index] ?? null;

            if ($workerState === null) {
                continue;
            }

            if ($workerState->getStartedAt() === $this->workersLastState[$workerId]->getStartedAt()) {
                continue;
            }

            if (\in_array($workerId, $groupWorkerIds, true) && $memoryUsage >= $this->maxSystemMemory) {
                $workerPool->getLogger()?->info('Restart worker of group "'.$group->getGroupName().'" by quota maxSystemMemory: ' . $this->maxSystemMemory);
                $workerPool->restartWorker($workerId);
                $this->workersLastState[$workerState->getWorkerId()] = $workerState;
            }
        }
    }
}
