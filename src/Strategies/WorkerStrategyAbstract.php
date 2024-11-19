<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Strategies;

use IfCastle\AmpPool\Worker\WorkerInterface;
use IfCastle\AmpPool\WorkerGroupInterface;
use IfCastle\AmpPool\WorkerPoolInterface;
use IfCastle\AmpPool\WorkersStorage\WorkersStorageInterface;

abstract class WorkerStrategyAbstract implements WorkerStrategyInterface
{
    private \WeakReference|null $workerPool = null;
    private \WeakReference|null $worker = null;
    private \WeakReference|null $workerGroup = null;
    private bool $isSelfWorker = false;

    public function getWorkerPool(): WorkerPoolInterface|null
    {
        return $this->workerPool?->get();
    }

    public function getWorker(): WorkerInterface|null
    {
        return $this->worker?->get();
    }

    public function getSelfWorker(): WorkerInterface|null
    {
        if ($this->isSelfWorker()) {
            return $this->getWorker();
        }

        return null;
    }

    public function getWorkerGroup(): WorkerGroupInterface|null
    {
        return $this->workerGroup?->get();
    }

    public function setWorkerPool(WorkerPoolInterface $workerPool): self
    {
        $this->workerPool           = \WeakReference::create($workerPool);

        return $this;
    }

    public function setWorker(WorkerInterface $worker, bool $isSelfWorker): self
    {
        $this->worker               = \WeakReference::create($worker);
        $this->isSelfWorker         = $isSelfWorker;

        return $this;
    }

    public function setWorkerGroup(WorkerGroupInterface $workerGroup): self
    {
        $this->workerGroup          = \WeakReference::create($workerGroup);

        return $this;
    }

    /**
     * Returns true if the current worker is the same as the worker that the strategy is attached to.
     *
     */
    protected function isSelfWorker(): bool
    {
        return $this->isSelfWorker;
    }

    protected function isNotSelfWorker(): bool
    {
        return !$this->isSelfWorker;
    }

    protected function getGroupsScheme(): array
    {
        if ($this->worker?->get() !== null) {
            return $this->worker->get()->getGroupsScheme();
        }

        if ($this->workerPool?->get() !== null) {
            return $this->workerPool->get()->getGroupsScheme();
        }

        return [];
    }

    protected function getWorkersStorage(): ?WorkersStorageInterface
    {
        if ($this->workerPool?->get() !== null) {
            return $this->workerPool->get()->getWorkersStorage();
        }

        if ($this->worker?->get() !== null) {
            return $this->worker->get()->getWorkersStorage();
        }

        return null;
    }

    protected function getCurrentWorkerId(): ?int
    {
        if ($this->worker?->get() !== null) {
            return $this->worker->get()->getWorkerId();
        }

        return null;
    }

    protected function isWorker(): bool
    {
        return $this->worker !== null && $this->workerPool === null;
    }

    protected function isWatcher(): bool
    {
        return $this->worker === null && $this->workerPool !== null;
    }

    public function onStarted(): void
    {
    }

    public function onStopped(): void
    {
    }

    public function __serialize(): array
    {
        return [];
    }

    public function __unserialize(array $data): void
    {
    }
}
