<?php
declare(strict_types=1);

namespace IfCastle\AmpPool\Worker\Internal;

use Amp\DeferredFuture;
use Amp\Future;
use IfCastle\AmpPool\Internal\WorkerProcessContext;
use IfCastle\AmpPool\WorkerGroupInterface;
use IfCastle\AmpPool\WorkersStorage\WorkerStateInterface;

/**
 * @internal
 */
final class WorkerDescriptor
{
    private ?DeferredFuture       $startFuture      = null;
    private ?WorkerProcessContext $workerProcess    = null;
    private bool                  $isStoppedForever = false;
    public ?WorkerStateInterface  $workerState      = null;

    public function __construct(
        public readonly int $id,
        public readonly WorkerGroupInterface $group,
        private bool $shouldBeStarted = false,
    ) {
    }

    public function starting(): void
    {
        if ($this->startFuture === null || $this->startFuture->isComplete()) {
            $this->startFuture      = new DeferredFuture;
        }

        $this->workerState?->updateShouldBeStarted(true);
    }

    public function started(): void
    {
        if ($this->startFuture?->isComplete() === false) {
            $this->startFuture->complete();
        }
    }

    public function getStartDeferred(): DeferredFuture|null
    {
        return $this->startFuture;
    }

    public function getStartFuture(): Future|null
    {
        return $this->startFuture?->getFuture();
    }

    public function getWorkerProcess(): ?WorkerProcessContext
    {
        return $this->workerProcess;
    }

    public function setWorkerProcess(WorkerProcessContext $workerProcess): void
    {
        $this->workerProcess        = $workerProcess;
    }

    public function willBeStarted(): void
    {
        $this->shouldBeStarted      = true;

        $this->started();
        $this->startFuture          = null;
    }

    public function willBeStopped(): void
    {
        $this->shouldBeStarted      = false;
        $this->workerState?->updateShouldBeStarted(false);
    }

    public function markAsStopped(): void
    {
        $this->workerProcess        = null;
    }

    public function isRunningOrWillBeRunning(): bool
    {
        return $this->isRunning() || ($this->startFuture !== null && false === $this->startFuture->isComplete());
    }

    public function isRunning(): bool
    {
        return $this->workerProcess !== null && false === $this->workerProcess->wasTerminated();
    }

    public function isNotRunning(): bool
    {
        return $this->workerProcess === null || $this->workerProcess->wasTerminated();
    }

    public function isStoppedForever(): bool
    {
        return $this->isStoppedForever;
    }

    public function shouldBeStarted(): bool
    {
        return true === $this->shouldBeStarted && false === $this->isStoppedForever;
    }

    public function markAsStoppedForever(): void
    {
        $this->isStoppedForever     = true;
        $this->workerState?->updateShouldBeStarted(false);
    }
}
